from typing import Any, Dict, List

from ray.experimental.dag import DAGNode


class InputNode(DAGNode):
    """Ray dag node used in DAG building API to mark entrypoints of a DAG.

    Should only be function or class method. A DAG can have multiple
    entrypoints.

    Ex:
                A.forward
             /            \
        input               ensemble -> output
             \            /
                B.forward

    In this pipeline, each user input is broadcasted to both A.forward and
    B.forward as first stop of the DAG, and authored like

    a = A.forward.bind(ray.dag.InputNode())
    b = B.forward.bind(ray.dag.InputNode())
    dag = ensemble.bind(a, b)

    dag.execute(user_input) --> broadcast to a and b
    """

    def __init__(self):
        super().__init__([], {}, {}, {})
        # TODO: (jiaodong) Support better structured user input data

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return InputNode()

    def _execute_impl(self, *args):
        """Executor of InputNode by ray.remote()"""
        # TODO: (jiaodong) Extend this to take more complicated user inputs
        return args[0]
