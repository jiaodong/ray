

from typing import Any, Dict, Optional, List, Union

from ray.experimental.dag import DAGNode
from ray.serve.handle import RayServeSyncHandle, RayServeHandle
from ray.experimental.dag.constants import DAGNODE_TYPE_KEY
from ray.experimental.dag.format_utils import get_dag_node_str

class DeploymentExecutorNode(DAGNode):
    """The lightweight executor DAGNode of DeploymentNode that optimizes for
    efficiency.

        - We need Ray DAGNode's traversal and replacement mechanism to deal
            with deeply nested nodes as args in the DAG
        - Meanwhile, __init__, _copy_impl and _execute_impl are on the critical
            pass of execution for every request.

    Therefore for serve we introduce a minimal weight node as the final product
    of DAG transformation, and will be used in actual execution as well as
    deployment.
    """

    def __init__(
        self,
        deployment_handle: Union[RayServeSyncHandle, RayServeHandle],
        other_args_to_resolve: Optional[Dict[str, Any]] = None,
    ):
        super().__init__([], {}, {}, other_args_to_resolve=other_args_to_resolve)
        self._deployment_handle = deployment_handle

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentExecutorNode(
            self._deployment_handle,
            other_args_to_resolve=new_other_args_to_resolve,
        )

    def _execute_impl(self, *args, **kwargs):
        """Executor of DeploymentNode getting called each time on dag.execute.

        The execute implementation is recursive, that is, the method nodes will receive
        whatever this method returns. We return a handle here so method node can
        directly call upon.
        """
        return self._deployment_handle

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._deployment_handle))

    def to_json(self) -> Dict[str, Any]:
        return {
            DAGNODE_TYPE_KEY: DeploymentExecutorNode.__name__,
            "deployment_handle": self._deployment_handle,
            "other_args_to_resolve": self.get_other_args_to_resolve(),
            "uuid": self.get_stable_uuid(),
        }

    @classmethod
    def from_json(cls, input_json):
        assert input_json[DAGNODE_TYPE_KEY] == DeploymentExecutorNode.__name__
        return cls(
            input_json["deployment_handle"],
            other_args_to_resolve=input_json["other_args_to_resolve"],
        )