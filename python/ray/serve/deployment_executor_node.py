from typing import Any, Dict, Optional, List, Union

import ray
from ray.experimental.dag import DAGNode
from ray.serve.deployment_method_executor_node import DeploymentMethodExecutorNode
from ray.serve.handle import RayServeSyncHandle, RayServeHandle
from ray.serve.deployment_graph import RayServeDAGHandle
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
        deployment_handle,
        dag_args,  # Not deployment init args
        dag_kwargs,  # Not deployment init kwargs
    ):
        print(
            f">>>> init DeploymentExecutorNode with args: {dag_args}, kwargs: {dag_kwargs}"
        )
        self._deployment_handle = deployment_handle
        super().__init__(dag_args, dag_kwargs, {}, {})

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return DeploymentExecutorNode(
            self._deployment_handle,
            new_args,
            new_kwargs,
        )

    def _execute_impl(self, *args, **kwargs):
        """Does not call into anything or produce a new value, as the time
        this function gets called, all child nodes are already resolved to
        ObjectRefs.
        """
        print(f"????? Executor Node - called with args: {args}, kwargs: {kwargs}")
        print(
            f"????? Executor Node - _bound_args: {self._bound_args}, _bound_kwargs: {self._bound_kwargs}"
        )
        # For DAGDriver -- Return object ref of the dag handle
        # For regular deployment -- return handle
        # if self._deployment_handle.deployment_name == "DAGDriver":
        #     return self._bound_args[0]
        # else:
        return self._deployment_handle

    def __str__(self) -> str:
        return get_dag_node_str(self, str(self._deployment_handle))

    def to_json(self) -> Dict[str, Any]:
        return {
            DAGNODE_TYPE_KEY: DeploymentExecutorNode.__name__,
            "deployment_handle": self._deployment_handle,
            "args": self.get_args(),
            "kwargs": self.get_kwargs(),
        }

    @classmethod
    def from_json(cls, input_json):
        assert input_json[DAGNODE_TYPE_KEY] == DeploymentExecutorNode.__name__
        return cls(
            input_json["deployment_handle"], input_json["args"], input_json["kwargs"]
        )
