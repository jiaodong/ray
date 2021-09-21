# Regular ray application that user wrote and runs on local cluster.
# intermediate status are dumped to GCS
import argparse
import time
import os

import ray
import ray.experimental.internal_kv as ray_kv


@ray.remote
class StepActor:
    def __init__(self, interval_s=1, total_steps=3, fail_at_step=None):
        self.interval_s = interval_s
        self.stopped = False
        self.current_step = 1
        self.total_steps = total_steps
        self.fail_at_step = fail_at_step

    def run(self):
        while self.current_step <= self.total_steps:
            if (self.fail_at_step is not None and
                self.current_step == self.fail_at_step):
                raise RuntimeError(
                    f"Intentionally throwing at step {self.current_step}")

            if not self.stopped:
                print(f"Sleeping {self.interval_s} secs to executing "
                      f"step {self.current_step}")
                time.sleep(self.interval_s)
                self.current_step += 1
            else:
                print("Stop called or reached final step.")
                break

        self.stopped = True
        return "DONE"

    def get_step(self):
        return self.current_step

    def stop(self):
        self.stopped = True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--interval-s",
        required=False,
        type=int,
        default=1,
        help="time between each step")
    parser.add_argument(
        "--total-steps",
        required=False,
        type=int,
        default=3,
        help="total number of steps taken")
    parser.add_argument(
        "--fail-at-step",
        required=False,
        type=int,
        default=None,
        help="throw exception at given step, if given")
    args, _ = parser.parse_known_args()

    ray.init()
    step_actor = StepActor.remote(
        interval_s=args.interval_s, total_steps=args.total_steps, fail_at_step=args.fail_at_step)
    ref = step_actor.run.remote()
    print(ray.get([ref]))
