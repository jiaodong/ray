import json
from ray.experimental.dag.class_node import ClassNode  # noqa: F401
from ray.experimental.dag.function_node import FunctionNode  # noqa: F401
from ray.experimental.dag import DAGNode  # noqa: F401
from ray.util.annotations import PublicAPI
from ray.serve.pipeline.deployment_node import DeploymentNode  # noqa: F401
from ray.serve.pipeline.deployment_function_node import (  # noqa: F401
    DeploymentFunctionNode,
)
from ray.serve.pipeline.deployment_method_node import DeploymentMethodNode  # noqa: F401


@PublicAPI(stability="alpha")
class RayServeDAGHandle:
    """Resolved from a DeploymentNode at runtime.

    This can be used to call the DAG from a driver deployment to efficiently
    orchestrate a deployment graph.
    """

    def __init__(self, dag_node_json: str) -> None:
        from ray.serve.pipeline.json_serde import dagnode_from_json

        self.dagnode_from_json = dagnode_from_json
        self.dag_node_json = dag_node_json

        # NOTE(simon): Making this lazy to avoid deserialization in controller for now
        # This would otherwise hang because it's trying to get handles from within
        # the controller.
        self.dag_node = None

    @classmethod
    def _deserialize(cls, *args):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(*args)

    def __reduce__(self):
        return RayServeDAGHandle._deserialize, (self.dag_node_json,)

    def remote(self, *args, **kwargs):
        if self.dag_node is None:
            self.dag_node = json.loads(
                self.dag_node_json, object_hook=self.dagnode_from_json
            )
        return self.dag_node.execute(*args, **kwargs)
