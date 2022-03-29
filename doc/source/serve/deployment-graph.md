---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.6
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

(serve-deployment-graph)=

# Deployment Graph

```{note} 
Note: This feature is in Alpha, so APIs are subject to change.
```

## Motivation

Production machine learning serving pipelines are getting longer and wider. They often consist of many models collectively making a final prediction. This is common in use cases like image / video content classification and tagging, fraud detection pipeline with multiple policies, multi-stage ranking and recommendation, etc.

Meanwhile, the size of a model is also growing beyond the memory limit of a single machine due to the exponentially growing number of parameters. GPT-3 and sparse feature embeddings in large recommendation models are two prime examples. The need of serving large models with disaggregated and distributed inference is rapidly growing.

We want to leverage the programmable and general purpose distributed computing ability of Ray and double down on its unique strengths (scheduling, communication and shared memory) to facilitate authoring, orchestrating, scaling and deployment of complex serving graphs so a user can program & test multiple models or multiple shards of a single large model dynamically and deploy to production at scale, and able to scale and reconfigure individually.

## Key Features
- Provide the ability to author a DAG of Serve nodes to form a complex inference graph.
- Graph authoring experience should be fully Python-programmable with support for dynamic selection, control flows, user business logic, etc.
- Graph can be instantiated and locally executed using tasks and actors API
- Graph can be deployed as a group where individual nodes can be reconfigured and scaled indepenently.

__[Full Ray Enhancement Proposal, REP-001: Serve Pipeline](https://github.com/ray-project/enhancements/blob/main/reps/2022-03-08-serve_pipeline.md)__

+++

## Concepts

### Deployment
Scalable, upgradeable group of actors managed by Ray Serve. __[See docs for detail](https://docs.ray.io/en/master/serve/core-apis.html#core-api-deployments)__

### DeploymentNode
Smallest unit in a graph, typically a serve annotated class or function, backed by a Deployment.

### Bind
A graph building API applicable to decorated class or function.  `decorated_class_or_func.bind(*args, **kwargs)` generates an IR node that can be used to build graph, and bound arguments will be applied at execution time, including dynamic user input.

### Deployment Graph
Collection of deployment nodes bound together that forms a DAG that represents an inference graph for complicated tasks, can be deployed and call as a unit. Ex: ensemble, chaining, dynamic selection. 


+++

## Key APIs Explained

The class and function definition as well as decorator didn't diverage from existing serve API. So in the following section we only need to dive into a few new key APIs used: `bind()`, `InputNode()` and `serve.run()`

+++

### **`bind(*args, **kwargs)`**

Once called on supported ray decorated function or class (@serve.deployment fully supported, @ray.remote soon), generates an IR of type DAGNode that acts as the building block of graph building.

+++

#### On function

```bind()``` on function produces a DAGNode that can be exeucted with user input.

+++

```{code-cell} python3
:tags: [remove-cell]
import ray
from ray import serve
from ray.serve.pipeline.generate import DeploymentNameGenerator

if ray.is_initialized():
    serve.shutdown()
    DeploymentNameGenerator.reset()
    ray.shutdown()

ray.init(num_cpus=4)
serve.start()

### Setting up clean ray cluster with serve ###
```
        

```{code-cell} ipython3
@serve.deployment
def preprocessor_with_arg(val):
    print(val)

# Produces a DeploymentFunctionNode with no bound args.
func_node = preprocessor_with_arg.bind()

dag_handle = serve.run(func_node)
# Once executed it will execute with arg value of 2.
print(ray.get(dag_handle.remote(2)))

"""
(my_func pid=17060) 2
"""
```

#### On class constructor 

**`Class.bind(*args, **kwargs)`** constructs and returns a DAGNode that acts as the instantiated instance of Class, where `*args` and `**kwargs` are used as init args.

#### On class method

Once a class is bound with its init args, its class methods can be directly accessed, called or bound with other args.

+++

```{code-cell} python3
:tags: [remove-cell]
import ray
from ray import serve
from ray.serve.pipeline.generate import DeploymentNameGenerator

if ray.is_initialized():
    serve.shutdown()
    DeploymentNameGenerator.reset()
    ray.shutdown()

ray.init(num_cpus=4)
serve.start()

### Setting up clean ray cluster with serve ###
```

```{code-cell} ipython3
@serve.deployment
class Model:
    def __init__(self, val):
        self.val = val
    def get(self):
        return self.val

# Produces a deployment class node instance initialized with bound args value. 
class_node = Model.bind(5)
dag_handle = serve.run(class_node)
# Access get() class method on bound Model
print(ray.get(dag_handle.get.remote()))
"""
get() returns

5
"""
```

### DAGNode as arguments in other node's bind()

DAGNode can also be passed into other DAGNode in dag binding. In the full example below, ```Combiner``` calls into two instantiations of ```Model``` class, which can be bound and passed into ```Combiner```'s constructor as if we're passing in two regular python class instances.

```python
m1 = Model.bind(1)
m2 = Model.bind(2)
combiner = Combiner.bind(m1, m2)
```

Similarly, we can also pass and bind upstream DAGNode results that will be resolved upon runtime to downstream DAGNodes, in our example, a `DeploymentMethodNode` that access class method of ```Combiner``` class takes two preprocessing DAGNodes' output as well as part of user input.

```python
preprocessed_1 = preprocessor.bind(dag_input[0])
preprocessed_2 = avg_preprocessor.bind(dag_input[1])
...
dag = combiner.run.bind(preprocessed_1, preprocessed_2, dag_input[2])
```

+++

### **```InputNode()```** : User input of the graph

```InputNode``` is a special singleton in DAG building that's only relevant to it's runtime call behavior. Even though all decorated classes or functions can be reused in arbitrary way to facilitate DAG building where the root DAGNode forms the graph with its children, in each deployment graph there should be one and only one InputNode used.

```InputNode``` value is fulfilled and replaced by user input at runtime, therefore it takes no argument when being constructed.

It's possible to access partial user input by index or key, if some DAGNode in the graph doesn't need the complete user input to run. In the full example below, `combiner.run` only needs the element at index 2 to determine it's runtime behavior.

```python
dag = combiner.run.bind(preprocessed_1, preprocessed_2, dag_input[2])
```

+++

### **```serve.run()```** : running the deployment graph

The deployment graph can be deployed with ```serve.run()```. ```serve.run()```
takes in a target DeploymentNode, and it deploys the node's deployments, as
well as all its child nodes' deployments. To deploy your graph, pass in the
driver DeploymentNode into ```serve.run()```:

```python
with InputNode() as dag_input:
    # ... DAG building
    serve_dag = ...
dag_handle = serve.run(serve_dag)
```

```serve.run``` returns the passed-in node's deployment's handle. You can use
this handle to issue requests to the deployment:

```python
# Warm up
ray.get(dag_handle.predict.remote(["0", [0, 0], "sum"]))
```

During development, you can also use the Serve CLI to run your deployment
graph. The CLI was included with Serve when you did ``pip install "ray[serve]"``.
The command ```serve run [node import path]``` will deploy the node and its
childrens' deployments. For example, we can remove the ```serve.run()``` calls
inside the Python script and save our example pipeline to a file called
example.py. Then we can run the driver DeploymentNode using its import path,
```example.serve_dag```:

```bash
$ serve run example.serve_dag
```

+++

```{tip}
The CLI expects the import path to either be a Python module on your system
or a relative import from the command line's current working directory. You can
change the directory that the CLI searches using the ```--app-dir``` flag.
The command will block on the terminal window and periodically print all the
deployments' health statuses. You can open a separate terminal window and
issue HTTP requests to your deployments
```

+++

```bash
$ python
>>> import requests
>>> requests.post("http://127.0.0.1:8000/my-dag", json=["1", [0, 2], "max"]).text
```
The CLI's ```serve run``` tool has useful flags to configure which Ray cluster
to run on, which runtime_env to use, and more. Use ```serve run --help``` to get
more info on these options.

+++

## Full End to End Example

Let's put the concepts together to a full runnable DAG example including the following attributes:

- All nodes in the deployment graph naturally forms a DAG structure.
- A node could use or call into other nodes in the deployment graph.
- Deployment graph has mix of class and function as nodes.
- Same input or output can be used in multiple nodes in the DAG.
- A node might access partial user input.
- Control flow is used where dynamic dispatch happens with respect to input value.
- Same class can be constructed, or function bound with different args that generates multiple distinct nodes in DAG.
- A node can be called either sync or async.

+++

![deployment graph](https://github.com/ray-project/images/blob/master/docs/serve/deployment_graph.png?raw=true)

+++

```{code-cell} python3
:tags: [remove-cell]
import ray
from ray import serve
from ray.serve.pipeline.generate import DeploymentNameGenerator

if ray.is_initialized():
    serve.shutdown()
    DeploymentNameGenerator.reset()
    ray.shutdown()

ray.init(num_cpus=4)
serve.start()

### Setting up clean ray cluster with serve ###
```

```{code-cell} ipython3
import time
import asyncio
import requests
import starlette

from ray.experimental.dag.input_node import InputNode

@serve.deployment
async def preprocessor(input_data: str):
    """Simple feature processing that converts str to int"""
    time.sleep(0.1) # Manual delay for blocking computation
    return int(input_data)

@serve.deployment
async def avg_preprocessor(input_data):
    """Simple feature processing that returns average of input list as float."""
    time.sleep(0.15) # Manual delay for blocking computation
    return sum(input_data) / len(input_data)

@serve.deployment
class Model:
    def __init__(self, weight: int):
        self.weight = weight

    async def forward(self, input: int):
        time.sleep(0.3) # Manual delay for blocking computation 
        return f"({self.weight} * {input})"


@serve.deployment
class Combiner:
    def __init__(self, m1: Model, m2: Model):
        self.m1 = m1
        self.m2 = m2

    async def run(self, req_part_1, req_part_2, operation):
        # Merge model input from two preprocessors  
        req = f"({req_part_1} + {req_part_2})"
        
        # Submit to both m1 and m2 with same req data in parallel
        r1_ref = self.m1.forward.remote(req)
        r2_ref = self.m2.forward.remote(req)
        
        # Async gathering of model forward results for same request data
        rst = await asyncio.gather(*[r1_ref, r2_ref])
        
        # Control flow that determines runtime behavior based on user input
        if operation == "sum":
            return f"sum({rst})"
        else:
            return f"max({rst})"
        
@serve.deployment(num_replicas=2)
class DAGDriver:
    def __init__(self, dag_handle):
        self.dag_handle = dag_handle

    async def predict(self, inp):
        """Perform inference directly without HTTP."""
        return await self.dag_handle.remote(inp)

    async def __call__(self, request: starlette.requests.Request):
        """HTTP endpoint of the DAG."""
        input_data = await request.json()
        return await self.predict(input_data)

# DAG building
with InputNode() as dag_input:
    preprocessed_1 = preprocessor.bind(dag_input[0])  # Partial access of user input by index
    preprocessed_2 = avg_preprocessor.bind(dag_input[1]) # Partial access of user input by index
    m1 = Model.bind(1)
    m2 = Model.bind(2)
    combiner = Combiner.bind(m1, m2)
    dag = combiner.run.bind(
        preprocessed_1, preprocessed_2, dag_input[2]  # Partial access of user input by index
    ) 
    
    # Each serve dag has a driver deployment as ingress that can be user provided.
    serve_dag = DAGDriver.options(route_prefix="/my-dag").bind(dag)


dag_handle = serve.run(serve_dag)

# Warm up
ray.get(dag_handle.predict.remote(["0", [0, 0], "sum"]))

# Python handle 
cur = time.time()
print(ray.get(dag_handle.predict.remote(["5", [1, 2], "sum"])))
print(f"Time spent: {round(time.time() - cur, 2)} secs.")
# Http endpoint
cur = time.time()
print(requests.post("http://127.0.0.1:8000/my-dag", json=["5", [1, 2], "sum"]).text)
print(f"Time spent: {round(time.time() - cur, 2)} secs.")

# Python handle 
cur = time.time()
print(ray.get(dag_handle.predict.remote(["1", [0, 2], "max"])))
print(f"Time spent: {round(time.time() - cur, 2)} secs.")

# Http endpoint
cur = time.time()
print(requests.post("http://127.0.0.1:8000/my-dag", json=["1", [0, 2], "max"]).text)
print(f"Time spent: {round(time.time() - cur, 2)} secs.")
```

## Outputs

```
sum(['(1 * (5 + 1.5))', '(2 * (5 + 1.5))'])
Time spent: 0.49 secs.
sum(['(1 * (5 + 1.5))', '(2 * (5 + 1.5))'])
Time spent: 0.49 secs.


max(['(1 * (1 + 1.0))', '(2 * (1 + 1.0))'])
Time spent: 0.48 secs.
max(['(1 * (1 + 1.0))', '(2 * (1 + 1.0))'])
Time spent: 0.48 secs.
```


Critical path for each request in the DAG is 

preprocessing: ```max(preprocessor, avg_preprocessor) = 0.15 secs```
<br>
model forward: ```max(m1.forward, m2.forward) = 0.3 secs```
<br>
<br>
Total of `0.45` secs.