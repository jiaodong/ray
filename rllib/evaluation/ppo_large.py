# Import the RL algorithm (Trainer) we would like to use.
import ray
from ray.rllib.agents.ppo import PPOTrainer
from ray.util.collective.types import Backend

# ray.init(address="auto")

# ray.init(address="auto", log_to_driver=True)
# Configure the algorithm.
config = {
    # Environment (RLlib understands openAI gym registered strings).
    "env": "CartPole-v0",
    # Use 4 environment workers (aka "rollout workers") that parallelly
    # collect samples from their own environment clone(s).
    "num_workers": 3,
    # Change this to "framework: torch", if you are using PyTorch.
    # Also, use "framework: tf2" for tf2.x eager execution.
    "framework": "torch",
    "train_batch_size": 10,
    "sgd_minibatch_size": 10,
    "num_sgd_iter": 1,
    "min_sample_timesteps_per_reporting": 1,
    "min_train_timesteps_per_reporting": 1,
    "min_time_s_per_reporting": 0,
    # Tweak the default model provided automatically by RLlib,
    # given the environment's observation- and action spaces.
    "model": {
        "fcnet_hiddens": [4096, 4096, 2048],
        "fcnet_activation": "relu",
    },
    # Set up a separate evaluation worker set for the
    # `trainer.evaluate()` call after training (see below).
    # "evaluation_num_workers": 2,
    # Only for evaluation runs, render the env.
    "evaluation_config": {
        "render_env": True,
    },
    "num_gpus": 1,
    "num_gpus_per_worker": 1,
}

# from ray import tune

# tune.run(PPOTrainer, config=config)
# Create our RLlib Trainer.
# trainer = PPOTrainer(config=config)
trainer_actor = ray.remote(PPOTrainer).options(num_gpus=1, max_concurrency=10).remote(config=config)
# print(f">>>>>>> {trainer_actor}")

# Run it for n training iterations. A training iteration includes
# parallel sample collection by the environment workers as well as
# loss calculation on the collected batch and a model update.
# print(ray.get(trainer_actor.train.remote()))
remote_workers = ray.get(trainer_actor.get_remote_workers.remote())
all_workers = [trainer_actor] + remote_workers
print(f">>>> Creating collective group for {all_workers}")
init_results = ray.get(
    [
        worker.init_group.remote(len(all_workers), i, Backend.NCCL, "device_mesh")
        for i, worker in enumerate(all_workers)
    ]
)
print(init_results)
# init_buffers = ray.get(
#     [
#         worker.init_buffers.remote() for _, worker in enumerate(all_workers)
#     ]
# )
# cp.cuda.Device(0).synchronize()
# cp.cuda.Stream.null.synchronize()
# print(f">>>>> Broadcasting for the first time in main loop...")
# results = ray.get(
#     [
#         trainer_actor.broadcast.remote(group_name="device_mesh", src_rank=0),
#         remote_workers[0].broadcast.remote(group_name="device_mesh", src_rank=0),
#         remote_workers[1].broadcast.remote(group_name="device_mesh", src_rank=0),
#         remote_workers[2].broadcast.remote(group_name="device_mesh", src_rank=0)
#     ]
# )
# print(f">>>>> results: {results}")

for _ in range(3):
    print(ray.get(trainer_actor.train.remote()))

# Evaluate the trained Trainer (and render each timestep to the shell's
# output).
# trainer.evaluate()