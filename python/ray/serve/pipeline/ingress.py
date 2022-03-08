import starlette
from importlib import import_module

from ray import serve
from ray.experimental.dag import DAGNode
from ray.serve.utils import parse_import_path, _get_logger

logger = _get_logger()


@serve.deployment
class Ingress:
    """User facing HTTP component of a serve pipeline. Generated by default."""

    def __init__(
        self,
        serve_dag_node_json: str,
        preprocessor_import_path: str,
    ):
        import json
        from ray.serve.pipeline.json_serde import dagnode_from_json

        self.dag: DAGNode = json.loads(
            serve_dag_node_json, object_hook=dagnode_from_json
        )
        module_name, attr_name = parse_import_path(preprocessor_import_path)
        self.preprocessor = getattr(import_module(module_name), attr_name)

    async def __call__(self, request: starlette.requests.Request):
        # TODO (jiaodong, simonmo): Integrate with ModelWrapper
        user_input_python = await self.preprocessor(request)
        return await self.dag.execute(user_input_python)
