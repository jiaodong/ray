from typing import Any, Dict, List

from ray.experimental.dag import DAGNode
from ray.experimental.dag.format_utils import get_dag_node_str

from ray.experimental.dag.constants import DAGNODE_TYPE_KEY
from ray.serve.handle import RayServeLazySyncHandle

class MinimalDeploymentNode(DAGNode):
    """Represents a serve DAGNode in a DAG that contains minimal info to
    facilitate JSON serialization meanwhile has same power of DAG execution.
    """

    def __init__(self, args, kwargs, deployment_name, deployment_method_name=None):
        super().__init__(args, kwargs, {}, other_args_to_resolve={})
        self.deployment_name = deployment_name
        self.deployment_method_name = deployment_method_name
        self.deployment_handle = RayServeLazySyncHandle(deployment_name)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return MinimalDeploymentNode(
            new_args,
            new_kwargs,
            self.deployment_name,
            deployment_method_name=self.deployment_method_name,
        )

    def _execute_impl(self, *args, **kwargs):
        """Executor of MinimalDeploymentNode by ray.remote()"""
        # Execute with bound args.
        if self.deployment_method_name:
            # Method call on deployment class
            handle = getattr(
                self.deployment_handle, self.deployment_method_name
            )
        else:
            handle = self.deployment_handle

        return handle.remote(
            *self._bound_args,
            **self._bound_kwargs,
        )

    def __str__(self) -> str:
        return get_dag_node_str(self, self.deployment_name)

    def to_json(self) -> Dict[str, Any]:
        return {
            DAGNODE_TYPE_KEY: MinimalDeploymentNode.__name__,
            "args": self.get_args(),
            "kwargs": self.get_kwargs(),
            "deployment_name": self.deployment_name,
            "deployment_method_name": self.deployment_method_name,
            "uuid": self.get_stable_uuid(),
        }

    @classmethod
    def from_json(cls, input_json):
        assert input_json[DAGNODE_TYPE_KEY] == MinimalDeploymentNode.__name__
        node = cls(
            input_json["args"],
            input_json["kwargs"],
            input_json["deployment_name"],
            deployment_method_name=input_json["deployment_method_name"],
        )
        node._stable_uuid = input_json["uuid"]
        return node
