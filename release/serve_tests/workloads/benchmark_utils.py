import time

import ray


@ray.remote
class DeploymentHandleClient:
    def __init__(self, async_func):
        self.async_func = async_func

    def ready(self):
        return "ok"

    async def run(self, *args, **kwargs):
        return await self.async_func(*args, **kwargs)


async def measure_latency_ms(async_fn, args, expected_output, num_requests=10):
    # warmup for 1sec
    start = time.time()
    while time.time() - start < 1:
        await async_fn(args)

    latency_stats = []
    for _ in range(num_requests):
        start = time.time()
        await async_fn(args) == expected_output
        end = time.time()
        latency_stats.append((end - start) * 1000)

    return latency_stats


async def measure_throughput_tps(async_fn, args, expected_output, duration_secs=10):
    # warmup for 1sec
    start = time.time()
    while time.time() - start < 1:
        await async_fn(args)

    tps_stats = []
    for _ in range(duration_secs):
        start = time.time()
        request_completed = 0
        while time.time() - start < 1:
            await async_fn(args) == expected_output
            request_completed += 1
        tps_stats.append(request_completed)

    return tps_stats
