import ray
from ray.experimental.dag.dag_node import DAGNode

from typing import Any, Dict, List, Optional


class ActorNode(DAGNode):
    """Represents an actor creation in a Ray task DAG."""

    # TODO(ekl) support actor options
    def __init__(self, actor_cls: type, cls_args, cls_kwargs, cls_options=None):
        self._actor_cls = actor_cls
        self._last_call: Optional["ActorMethodNode"] = None
        DAGNode.__init__(self, cls_args, cls_kwargs, options=cls_options)

    def _copy(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
    ):
        return ActorNode(self._actor_cls, new_args, new_kwargs, new_options)

    def _execute(self):
        if self._bound_options:
            return (
                ray.remote(self._actor_cls)
                .options(**self._bound_options)
                .remote(*self._bound_args, **self._bound_kwargs)
            )
        else:
            return ray.remote(self._actor_cls).remote(
                *self._bound_args, **self._bound_kwargs
            )

    def __getattr__(self, method_name: str):
        # Raise an error if the method is invalid.
        getattr(self._actor_cls, method_name)
        call_node = _UnboundActorMethodNode(self, method_name)
        return call_node

    def __str__(self):
        return "ActorNode(cls={}, args={}, kwargs={})".format(
            self._actor_cls, self._bound_args, self._bound_kwargs
        )


class _UnboundActorMethodNode(object):
    def __init__(self, actor: ActorNode, method_name: str):
        self._actor = actor
        self._method_name = method_name
        self._options = None

    def _bind(self, *args, **kwargs):
        node = ActorMethodNode(
            self._actor,
            self._actor._last_call,
            self._method_name,
            args,
            kwargs,
            method_options=self._options,
        )
        self._actor._last_call = node
        return node

    def options(self, **options):
        self._options = options
        return self


class ActorMethodNode(DAGNode):
    """Represents an actor method invocation in a Ray task DAG."""

    # TODO(ekl) support method options
    def __init__(
        self,
        actor: ActorNode,
        prev_call: Optional["ActorMethodNode"],
        method_name: str,
        method_args,
        method_kwargs,
        method_options: Optional[Dict[str, Any]] = None,
    ):
        self._method_name: str = method_name
        # The actor creation task dependency is encoded as the first argument,
        # and the ordering dependency as the second, which ensures they are
        # executed prior to this node.
        DAGNode.__init__(
            self,
            (actor, prev_call) + method_args,
            method_kwargs,
            options=method_options,
        )

    def _copy(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
    ):
        return ActorMethodNode(
            new_args[0],
            new_args[1],
            self._method_name,
            new_args[2:],
            new_kwargs,
            method_options=new_options,
        )

    def _execute(self):
        actor_handle = self._bound_args[0]
        if self._bound_options:
            return (
                getattr(actor_handle, self._method_name)
                .options(**self._bound_options)
                .remote(
                    *self._bound_args[2:],
                    **self._bound_kwargs,
                )
            )
        else:
            return getattr(actor_handle, self._method_name).remote(
                *self._bound_args[2:], **self._bound_kwargs
            )

    def __str__(self):
        return (
            "ActorMethodNode(actor={}, prev_call={}, method={}, "
            "args={}, kwargs={}, options={})"
        ).format(
            self._bound_args[0],
            self._bound_args[1],
            self._method_name,
            self._bound_args[2:],
            self._bound_kwargs,
            self._bound_options,
        )
