import inspect
from kfp import dsl, kubernetes
from typing import List, Dict, Optional, Any


class KFPTask(object):
    def __init__(
        self,
        name: str,
        image: str,
        command: List[str],
        args: List[str],
        inputs: Optional[Dict[str, type]] = None,
        outputs: Optional[Dict[str, type]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        k8s_resources: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.image = image
        self.command = command
        self.args = args
        self.inputs = inputs or {}
        self.outputs = outputs or {}
        self.env_vars = env_vars or {}
        self.k8s_resources = k8s_resources or {}

    def create_task(self, **input_values) -> dsl.PipelineTask:
        parameters = []
        for input_name, input_type in self.inputs.items():
            parameters.append(
                inspect.Parameter(
                    input_name,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=input_type,
                )
            )

        for output_name, output_type in self.outputs.items():
            parameters.append(
                inspect.Parameter(
                    output_name,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=dsl.OutputPath(output_type),
                )
            )

        sig = inspect.Signature(parameters)

        # This inner function is what gets decorated
        def component_func(*args, **kwargs):
            return dsl.ContainerSpec(
                image=self.image,
                command=self.command,
                args=self.args,
            )

        component_func.__name__ = self.name
        # set __signature__ *before* decorating
        component_func.__signature__ = sig

        decorated = dsl.container_component(component_func)
        task = decorated(**input_values)

        field_path_env_vars = {
            "METAFLOW_KUBERNETES_NAMESPACE": "metadata.namespace",
            "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
            "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
            "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
            "METAFLOW_KUBERNETES_SERVICE_ACCOUNT_NAME": "spec.serviceAccountName",
            "METAFLOW_KUBERNETES_NODE_IP": "status.hostIP",
        }

        for env_name, field_path in field_path_env_vars.items():
            task = kubernetes.use_field_path_as_env(
                task,
                env_name,
                field_path,
            )

        # disable caching...
        task.set_caching_options(enable_caching=False)

        # set env vars...
        for k, v in self.env_vars.items():
            task.set_env_variable(k, v)

        # fill in k8s specific stuff...
        labels = self.k8s_resources.get("labels", None)
        if labels:
            for k, v in labels.items():
                kubernetes.add_pod_label(task, k, v)

        annotations = self.k8s_resources.get("annotations", None)
        if annotations:
            for k, v in annotations.items():
                kubernetes.add_pod_annotation(task, k, v)

        pod_resources = self.k8s_resources.get("pod_resources", None)
        if pod_resources:
            requests = pod_resources.get("requests", {}).copy()
            limits = pod_resources.get("limits", {}).copy()

            # no methods to set "ephemeral-storage" in requests and limits
            requests.pop("ephemeral-storage", None)
            limits.pop("ephemeral-storage", None)

            if "cpu" in requests:
                task.set_cpu_request(requests["cpu"])
            if "cpu" in limits:
                task.set_cpu_limit(limits["cpu"])
            if "memory" in requests:
                task.set_memory_request(requests["memory"])
            if "memory" in limits:
                task.set_memory_limit(limits["memory"])

            # After removing cpu, memory, ephemeral-storage,
            # any remaining key should be a GPU resource
            gpu_resources = {
                k: v
                for k, v in limits.items()
                if k not in ["cpu", "memory", "ephemeral-storage"]
            }

            if gpu_resources:
                # Should only be one GPU type per task
                if len(gpu_resources) > 1:
                    raise ValueError(
                        f"Multiple GPU types specified: {list(gpu_resources.keys())}. "
                        "Only one GPU type per task is supported."
                    )
                gpu_type, gpu_count = list(gpu_resources.items())[0]
                task.set_accelerator_type(gpu_type)
                task.set_accelerator_limit(int(gpu_count))

        return task


class KFPFlow(object):
    def __init__(
        self,
        name: str,
        graph,
        parameters: Dict[str, dict],
        kfp_tasks: Dict[str, KFPTask],
    ):
        self.name = name
        self.graph = graph
        self.parameters = parameters
        self.kfp_tasks = kfp_tasks

    def get_pipeline_func(self) -> None:
        pipeline_params = []
        for param_name, param_info in self.parameters.items():
            pipeline_params.append(
                inspect.Parameter(
                    param_name,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    default=param_info["value"],
                    annotation=param_info["type"],
                )
            )
        pipeline_sig = inspect.Signature(pipeline_params)

        # 2. Define the pipeline function
        def pipeline_func(*args, **kwargs):
            # KFP populates kwargs from the signature
            pipeline_kwargs = pipeline_sig.bind(*args, **kwargs).arguments

            seen = {}

            for node_name in self.graph.sorted_nodes:
                node = self.graph[node_name]
                kfp_task = self.kfp_tasks[node_name]
                input_values = {}

                if node.name == "start":
                    # Pass the pipeline parameters to the start step
                    for param_name in self.parameters.keys():
                        if param_name in pipeline_kwargs:
                            input_values[param_name] = pipeline_kwargs[param_name]
                else:
                    # Wire parent task_id outputs to this step's inputs
                    for parent_name in node.in_funcs:
                        input_name = f"{parent_name}_task_id"
                        if parent_name in seen:
                            input_values[input_name] = seen[parent_name].outputs[
                                "task_id_out"
                            ]

                task = kfp_task.create_task(**input_values)

                if node.name != "start":
                    for parent_name in node.in_funcs:
                        if parent_name in seen:
                            task.after(seen[parent_name])

                seen[node_name] = task

        # 3. Set the dynamic signature on the function
        pipeline_func.__signature__ = pipeline_sig

        # 4. Decorate and return the pipeline function
        return dsl.pipeline(name=self.name)(pipeline_func)
