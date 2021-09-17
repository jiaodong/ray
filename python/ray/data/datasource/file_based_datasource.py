import logging
import os
from typing import Callable, Optional, List, Tuple, Union, Any, TYPE_CHECKING
import urllib.parse

if TYPE_CHECKING:
    import pyarrow

from ray.types import ObjectRef
from ray.data.block import Block, BlockAccessor
from ray.data.impl.arrow_block import (ArrowRow, DelegatingArrowBlockBuilder)
from ray.data.impl.block_list import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask, WriteResult
from ray.util.annotations import DeveloperAPI
from ray.data.impl.util import _check_pyarrow_version
from ray.data.impl.remote_fn import cached_remote_fn

logger = logging.getLogger(__name__)


@DeveloperAPI
class FileBasedDatasource(Datasource[Union[ArrowRow, Any]]):
    """File-based datasource, for reading and writing files.

    This class should not be used directly, and should instead be subclassed
    and tailored to particular file formats. Classes deriving from this class
    must implement _read_file().

    Current subclasses:
        JSONDatasource, CSVDatasource, NumpyDatasource, BinaryDatasource
    """

    def prepare_read(
            self,
            parallelism: int,
            paths: Union[str, List[str]],
            filesystem: Optional["pyarrow.fs.FileSystem"] = None,
            schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
            _block_udf: Optional[Callable[[Block], Block]] = None,
            **reader_args) -> List[ReadTask]:
        """Creates and returns read tasks for a file-based datasource.
        """
        _check_pyarrow_version()
        import pyarrow as pa
        import numpy as np

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        paths, file_infos = _expand_paths(paths, filesystem)
        file_sizes = [file_info.size for file_info in file_infos]

        read_file = self._read_file

        filesystem = _wrap_s3_serialization_workaround(filesystem)

        def read_files(
                read_paths: List[str],
                fs: Union["pyarrow.fs.FileSystem", _S3FileSystemWrapper]):
            logger.debug(f"Reading {len(read_paths)} files.")
            if isinstance(fs, _S3FileSystemWrapper):
                fs = fs.unwrap()
            builder = DelegatingArrowBlockBuilder()
            for read_path in read_paths:
                with fs.open_input_stream(read_path) as f:
                    data = read_file(f, read_path, **reader_args)
                    if isinstance(data, pa.Table) or isinstance(
                            data, np.ndarray):
                        builder.add_block(data)
                    else:
                        builder.add(data)
            block = builder.build()
            if _block_udf is not None:
                block = _block_udf(block)
            return block

        read_tasks = []
        for read_paths, file_sizes in zip(
                np.array_split(paths, parallelism),
                np.array_split(file_sizes, parallelism)):
            if len(read_paths) <= 0:
                continue

            if self._rows_per_file() is None:
                num_rows = None
            else:
                num_rows = len(read_paths) * self._rows_per_file()
            read_task = ReadTask(
                lambda read_paths=read_paths: read_files(
                    read_paths, filesystem),
                BlockMetadata(
                    num_rows=num_rows,
                    size_bytes=sum(file_sizes),
                    schema=schema,
                    input_files=read_paths)
            )
            read_tasks.append(read_task)

        return read_tasks

    def _rows_per_file(self):
        """Returns the number of rows per file, or None if unknown.
        """
        return None

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        """Reads a single file, passing all kwargs to the reader.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError(
            "Subclasses of FileBasedDatasource must implement _read_files().")

    def do_write(self,
                 blocks: List[ObjectRef[Block]],
                 metadata: List[BlockMetadata],
                 path: str,
                 dataset_uuid: str,
                 filesystem: Optional["pyarrow.fs.FileSystem"] = None,
                 _block_udf: Optional[Callable[[Block], Block]] = None,
                 **write_args) -> List[ObjectRef[WriteResult]]:
        """Creates and returns write tasks for a file-based datasource."""
        path, filesystem = _resolve_paths_and_filesystem(path, filesystem)
        path = path[0]
        filesystem.create_dir(path, recursive=True)
        filesystem = _wrap_s3_serialization_workaround(filesystem)

        _write_block_to_file = self._write_block

        def write_block(write_path: str, block: Block):
            logger.debug(f"Writing {write_path} file.")
            fs = filesystem
            if isinstance(fs, _S3FileSystemWrapper):
                fs = fs.unwrap()
            if _block_udf is not None:
                block = _block_udf(block)
            with fs.open_output_stream(write_path) as f:
                _write_block_to_file(f, BlockAccessor.for_block(block))

        write_block = cached_remote_fn(write_block)

        file_format = self._file_format()
        write_tasks = []
        for block_idx, block in enumerate(blocks):
            write_path = os.path.join(
                path, f"{dataset_uuid}_{block_idx:06}.{file_format}")
            write_task = write_block.remote(write_path, block)
            write_tasks.append(write_task)

        return write_tasks

    def _write_block(self, f: "pyarrow.NativeFile", block: BlockAccessor,
                     **writer_args):
        """Writes a block to a single file, passing all kwargs to the writer.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError(
            "Subclasses of FileBasedDatasource must implement _write_files().")

    def _file_format(self):
        """Returns the file format string, to be used as the file extension
        when writing files.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError(
            "Subclasses of FileBasedDatasource must implement _file_format().")


# TODO(Clark): Add unit test coverage of _resolve_paths_and_filesystem and
# _expand_paths.


def _resolve_paths_and_filesystem(
        paths: Union[str, List[str]],
        filesystem: "pyarrow.fs.FileSystem" = None,
) -> Tuple[List[str], "pyarrow.fs.FileSystem"]:
    """
    Resolves and normalizes all provided paths, infers a filesystem from the
    paths and ensures that all paths use the same filesystem.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against all
            filesystems inferred from the provided paths to ensure
            compatibility.
    """
    import pyarrow as pa
    from pyarrow.fs import (FileSystem, PyFileSystem, FSSpecHandler,
                            _resolve_filesystem_and_path)
    import fsspec

    if isinstance(paths, str):
        paths = [paths]
    elif (not isinstance(paths, list)
          or any(not isinstance(p, str) for p in paths)):
        raise ValueError(
            "paths must be a path string or a list of path strings.")
    elif len(paths) == 0:
        raise ValueError("Must provide at least one path.")

    if filesystem and not isinstance(filesystem, FileSystem):
        if not isinstance(filesystem, fsspec.spec.AbstractFileSystem):
            raise TypeError(f"The filesystem passed must either conform to "
                            f"pyarrow.fs.FileSystem, or "
                            f"fsspec.spec.AbstractFileSystem. The provided "
                            f"filesystem was: {filesystem}")
        filesystem = PyFileSystem(FSSpecHandler(filesystem))

    resolved_paths = []
    for path in paths:
        try:
            resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
                path, filesystem)
        except pa.lib.ArrowInvalid as e:
            if "Cannot parse URI" in str(e):
                resolved_filesystem, resolved_path = (
                    _resolve_filesystem_and_path(
                        _encode_url(path), filesystem))
                resolved_path = _decode_url(resolved_path)
            else:
                raise
        if filesystem is None:
            filesystem = resolved_filesystem
        else:
            resolved_path = _unwrap_protocol(resolved_path)
        resolved_path = filesystem.normalize_path(resolved_path)
        resolved_paths.append(resolved_path)

    return resolved_paths, filesystem


def _expand_paths(paths: Union[str, List[str]],
                  filesystem: "pyarrow.fs.FileSystem"):
    """
    Expands all provided paths into concrete file paths by walking directories.
    Also returns a sidecar of file infos.

    This should be used on the output of _resolve_paths_and_filesystem.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories. These paths
            should be properly resolved, e.g. the paths returned from
            _resolve_paths_and_filesystem.
        filesystem: The filesystem implementation that should be used for
            reading these files.
    """
    from pyarrow.fs import FileType

    expanded_paths = []
    file_infos = []
    for path in paths:
        file_info = filesystem.get_file_info(path)
        if file_info.type == FileType.Directory:
            paths, file_infos_ = _expand_directory(path, filesystem)
            expanded_paths.extend(paths)
            file_infos.extend(file_infos_)
        elif file_info.type == FileType.File:
            expanded_paths.append(path)
            file_infos.append(file_info)
        else:
            raise FileNotFoundError(path)
    return expanded_paths, file_infos


def _expand_directory(path: str,
                      filesystem: "pyarrow.fs.FileSystem",
                      exclude_prefixes: List[str] = [".", "_"]) -> List[str]:
    """
    Expand the provided directory path to a list of file paths.

    Args:
        path: The directory path to expand.
        filesystem: The filesystem implementation that should be used for
            reading these files.
        exclude_prefixes: The file relative path prefixes that should be
            excluded from the returned file set. Default excluded prefixes are
            "." and "_".

    Returns:
        A list of file paths contained in the provided directory.
    """
    from pyarrow.fs import FileSelector
    selector = FileSelector(path, recursive=True)
    files = filesystem.get_file_info(selector)
    base_path = selector.base_dir
    filtered_paths = []
    for file_ in files:
        if not file_.is_file:
            continue
        file_path = file_.path
        if not file_path.startswith(base_path):
            continue
        relative = file_path[len(base_path):]
        if any(relative.startswith(prefix) for prefix in [".", "_"]):
            continue
        filtered_paths.append((file_path, file_))
    # We sort the paths to guarantee a stable order.
    return zip(*sorted(filtered_paths, key=lambda x: x[0]))


def _is_url(path) -> bool:
    return urllib.parse.urlparse(path).scheme != ""


def _encode_url(path):
    return urllib.parse.quote(path, safe="/:")


def _decode_url(path):
    return urllib.parse.unquote(path)


def _unwrap_protocol(path):
    """
    Slice off any protocol prefixes on path.
    """
    parsed = urllib.parse.urlparse(path)
    return parsed.netloc + parsed.path


def _wrap_s3_serialization_workaround(filesystem: "pyarrow.fs.FileSystem"):
    # This is needed because pa.fs.S3FileSystem assumes pa.fs is already
    # imported before deserialization. See #17085.
    import pyarrow as pa
    if isinstance(filesystem, pa.fs.S3FileSystem):
        return _S3FileSystemWrapper(filesystem)
    return filesystem


class _S3FileSystemWrapper:
    def __init__(self, fs: "pyarrow.fs.S3FileSystem"):
        self._fs = fs

    def unwrap(self):
        return self._fs

    @classmethod
    def _reconstruct(cls, fs_reconstruct, fs_args):
        # Implicitly trigger S3 subsystem initialization by importing
        # pyarrow.fs.
        import pyarrow.fs  # noqa: F401

        return cls(fs_reconstruct(*fs_args))

    def __reduce__(self):
        return _S3FileSystemWrapper._reconstruct, self._fs.__reduce__()
