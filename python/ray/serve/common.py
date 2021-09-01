import ray

from dataclasses import dataclass, field
from typing import List, Optional
from uuid import UUID

from ray.actor import ActorClass
from ray.serve.config import BackendConfig, ReplicaConfig

BackendTag = str
EndpointTag = str
NodeId = str
GoalId = UUID
Duration = float


@dataclass
class EndpointInfo:
    python_methods: Optional[List[str]] = field(default_factory=list)
    route: Optional[str] = None


class BackendInfo:
    def __init__(self,
                 backend_config: BackendConfig,
                 replica_config: ReplicaConfig,
                 start_time_ms: int,
                 actor_def: Optional[ActorClass] = None,
                 version: Optional[str] = None,
                 deployer_job_id: "Optional[ray._raylet.JobID]" = None,
                 end_time_ms: Optional[int] = None):
        self.backend_config = backend_config
        self.replica_config = replica_config
        # The time when .deploy() was first called for this deployment.
        self.start_time_ms = start_time_ms
        self.actor_def = actor_def
        self.version = version
        self.deployer_job_id = deployer_job_id
        # The time when this deployment was deleted.
        self.end_time_ms = end_time_ms


@dataclass
class ReplicaTag:
    # Contextual input sources
    deployment_tag: str
    replica_suffix: str
    # Generated replica name with fixed naming rule
    name: str = ""
    delimiter: str = "#"

    def __init__(self, deployment_tag: str, replica_suffix: str):
        self.deployment_tag = deployment_tag
        self.replica_suffix = replica_suffix
        self.name = f"{deployment_tag}{self.delimiter}{replica_suffix}"

    @classmethod
    def from_str(self, replica_tag):
        parsed = replica_tag.split(self.delimiter)
        assert len(parsed) == 2, (
            f"Given replica name {replica_tag} didn't match pattern, please "
            f"ensure it has exactly two fields with delimiter {self.delimiter}"
        )
        self.deployment_tag = parsed[0]
        self.replica_suffix = parsed[1]
        self.name = replica_tag

    def __str__(self):
        return self.name
