import ray
from ray.experimental.dag.dag_node import DAGNode
from ray.experimental.dag.input_node import InputNode
from ray.experimental.dag.format_utils import get_dag_node_str
from ray.experimental.dag.constants import (
    PARENT_CLASS_NODE_KEY,
    PREV_CLASS_METHOD_CALL_KEY,
    DAGNODE_TYPE_KEY,
)

from typing import Any, Dict, List, Optional, Tuple


class ClassNode(DAGNode):
    """Represents an actor creation in a Ray task DAG."""

    def __init__(
        self,
        cls,
        cls_args,
        cls_kwargs,
        cls_options,
        other_args_to_resolve=None,
    ):
        self._body = cls
        self._last_call: Optional["ClassMethodNode"] = None
        super().__init__(
            cls_args,
            cls_kwargs,
            cls_options,
            other_args_to_resolve=other_args_to_resolve,
        )

        if self._contains_input_node():
            raise ValueError(
                "InputNode handles user dynamic input the the DAG, and "
                "cannot be used as args, kwargs, or other_args_to_resolve "
                "in ClassNode constructor because it is not available at "
                "class construction or binding time."
            )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return ClassNode(
            self._body,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args):
        """Executor of ClassNode by ray.remote()"""
        return (
            ray.remote(self._body)
            .options(**self._bound_options)
            .remote(*self._bound_args, **self._bound_kwargs)
        )

    def _contains_input_node(self) -> bool:
        """Check if InputNode is used in children DAGNodes with current node
        as the root.
        """
        children_dag_nodes = self._get_all_child_nodes()
        for child in children_dag_nodes:
            if isinstance(child, InputNode):
                return True
        return False

    def __getattr__(self, method_name: str):
        # Raise an error if the method is invalid.
        getattr(self._body, method_name)
        call_node = _UnboundClassMethodNode(self, method_name)
        return call_node

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._body))

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        json_dict = super().to_json_base(encoder_cls)
        json_dict[DAGNODE_TYPE_KEY] = ClassNode.__name__
        body = self._body.__ray_actor_class__
        json_dict["import_path"] = f"{body.__module__}.{body.__qualname__}"
        return json_dict

    @classmethod
    def from_json(cls, input_json, module, object_hook=None):
        args_dict = super().from_json_base(input_json, object_hook=object_hook)
        return cls(
            module.__ray_metadata__.modified_class,
            args_dict["args"],
            args_dict["kwargs"],
            args_dict["options"],
            other_args_to_resolve=args_dict["other_args_to_resolve"],
        )


class _UnboundClassMethodNode(object):
    def __init__(self, actor: ClassNode, method_name: str):
        self._actor = actor
        self._method_name = method_name
        self._options = {}

    def _bind(self, *args, **kwargs):
        other_args_to_resolve = {
            PARENT_CLASS_NODE_KEY: self._actor,
            PREV_CLASS_METHOD_CALL_KEY: self._actor._last_call,
        }

        node = ClassMethodNode(
            self._method_name,
            args,
            kwargs,
            self._options,
            other_args_to_resolve=other_args_to_resolve,
        )
        self._actor._last_call = node
        return node

    def options(self, **options):
        self._options = options
        return self


class ClassMethodNode(DAGNode):
    """Represents an actor method invocation in a Ray function DAG."""

    def __init__(
        self,
        method_name: str,
        method_args: Tuple[Any],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
        other_args_to_resolve: Dict[str, Any],
    ):

        self._bound_args = method_args or []
        self._bound_kwargs = method_kwargs or {}
        self._bound_options = method_options or {}
        self._method_name: str = method_name
        # Parse other_args_to_resolve and assign to variables
        self._parent_class_node: ClassNode = other_args_to_resolve.get(
            PARENT_CLASS_NODE_KEY
        )
        # Used to track lineage of ClassMethodCall to preserve deterministic
        # submission and execution order.
        self._prev_class_method_call: Optional[
            ClassMethodNode
        ] = other_args_to_resolve.get(PREV_CLASS_METHOD_CALL_KEY, None)
        # The actor creation task dependency is encoded as the first argument,
        # and the ordering dependency as the second, which ensures they are
        # executed prior to this node.
        super().__init__(
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve=other_args_to_resolve,
        )

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return ClassMethodNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args):
        """Executor of ClassMethodNode by ray.remote()"""
        method_body = getattr(self._parent_class_node, self._method_name)
        # Execute with bound args.
        return method_body.options(**self._bound_options).remote(
            *self._bound_args,
            **self._bound_kwargs,
        )

    def __str__(self) -> str:
        return get_dag_node_str(self, f"{self._method_name}()")

    def to_json(self, encoder_cls) -> Dict[str, Any]:
        json_dict = super().to_json_base(encoder_cls)
        json_dict[DAGNODE_TYPE_KEY] = ClassMethodNode.__name__
        body = self._parent_class_node._body.__ray_actor_class__
        json_dict["import_path"] = f"{body.__module__}.{body.__qualname__}"
        json_dict.update({"method_name": self._method_name})
        return json_dict

    @classmethod
    def from_json(cls, input_json, object_hook=None):
        args_dict = super().from_json_base(input_json, object_hook=object_hook)
        return cls(
            input_json["method_name"],
            args_dict["args"],
            args_dict["kwargs"],
            args_dict["options"],
            other_args_to_resolve=args_dict["other_args_to_resolve"],
        )
