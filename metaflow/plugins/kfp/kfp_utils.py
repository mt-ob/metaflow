import inspect
from kfp import dsl, kubernetes
from .exception import NotSupportedException
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

        def component_func(*args, **kwargs):
            return dsl.ContainerSpec(
                image=self.image,
                command=self.command,
                args=self.args,
            )

        component_func.__name__ = self.name
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

        task.set_caching_options(enable_caching=False)

        for k, v in self.env_vars.items():
            task.set_env_variable(k, v)

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

            gpu_resources = {
                k: v
                for k, v in limits.items()
                if k not in ["cpu", "memory", "ephemeral-storage"]
            }

            if gpu_resources:
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

        def pipeline_func(*args, **kwargs):
            pipeline_kwargs = pipeline_sig.bind(*args, **kwargs).arguments
            self._build_pipeline(pipeline_kwargs)

        pipeline_func.__signature__ = pipeline_sig
        return dsl.pipeline(name=self.name)(pipeline_func)

    def _build_pipeline(self, pipeline_kwargs):
        if "start" in self.graph:
            seen: Dict[str, dsl.PipelineTask] = {}
            loop_item_index = None
            self._traverse_node(
                node=self.graph["start"],
                pipeline_kwargs=pipeline_kwargs,
                seen=seen,
                loop_item_index=loop_item_index,
            )

    def create_and_connect(
        self,
        node,
        pipeline_kwargs,
        seen,
        loop_item_index=None,
        extra_inputs: Optional[Dict] = None,
    ):
        kfp_task_def = self.kfp_tasks[node.name]
        input_values = self._prepare_node_inputs(
            node, pipeline_kwargs, seen, loop_item_index, extra_inputs
        )
        task = kfp_task_def.create_task(**input_values)
        self._add_dependencies(task, node, seen)
        seen[node.name] = task

        return task

    def _traverse_node(
        self,
        node,
        pipeline_kwargs: Dict[str, Any],
        seen: Dict[str, dsl.PipelineTask],
        loop_item_index: Optional[Any] = None,
    ) -> Optional[dsl.PipelineTask]:
        if node.name in seen:
            return seen[node.name]

        for parent_name in node.in_funcs:
            self._traverse_node(
                node=self.graph[parent_name],
                pipeline_kwargs=pipeline_kwargs,
                seen=seen,
                loop_item_index=loop_item_index,
            )

        if node.type == "join" and node.name in seen and seen[node.name] is None:
            task = None
        else:
            task = self.create_and_connect(node, pipeline_kwargs, seen, loop_item_index)

        if node.type == "end":
            return task

        elif node.type == "split":
            self._handle_split(node, pipeline_kwargs, seen, loop_item_index)

        elif node.type == "foreach":
            if node.is_inside_foreach:
                raise NotSupportedException(
                    "nested foreach are not supported as of now."
                )
            self._handle_foreach(node, task, pipeline_kwargs, seen, loop_item_index)

        elif node.type in ["start", "linear", "join"] and len(node.out_funcs) == 1:
            self._traverse_node(
                node=self.graph[node.out_funcs[0]],
                pipeline_kwargs=pipeline_kwargs,
                seen=seen,
                loop_item_index=loop_item_index,
            )

        return task

    def _prepare_node_inputs(
        self,
        node,
        pipeline_kwargs,
        seen,
        loop_item_index=None,
        extra_inputs: Optional[Dict] = None,
    ):
        input_values = {}
        if node.name == "start":
            for param_name in self.parameters.keys():
                if param_name in pipeline_kwargs:
                    # Pass flow parameters as inputs to the 'start' task
                    input_values[param_name] = pipeline_kwargs[param_name]
        elif (
            node.type == "join" and self.graph[node.split_parents[-1]].type == "foreach"
        ):
            pass
        else:
            # Pass parent task outputs (like task_id_out) as inputs
            for parent_name in node.in_funcs:
                if parent_name in seen and seen[parent_name] is not None:
                    parent_task = seen[parent_name]
                    input_values[f"{parent_name}_task_id"] = parent_task.outputs.get(
                        "task_id_out"
                    )

        if loop_item_index is not None:
            input_values["split_index"] = loop_item_index

        if extra_inputs is not None:
            for k, v in extra_inputs.items():
                input_values[k] = v

        return input_values

    def _add_dependencies(
        self, task: dsl.PipelineTask, node: Any, seen: Dict[str, dsl.PipelineTask]
    ):
        for parent_name in node.in_funcs:
            if parent_name in seen and seen[parent_name] is not None:
                parent = seen[parent_name]
                task.after(parent)

    def _handle_split(
        self,
        node,
        pipeline_kwargs,
        seen: Dict[str, dsl.PipelineTask],
        loop_item_index: Optional[Any] = None,
    ):
        if node.matching_join and node.matching_join not in seen:
            seen[node.matching_join] = None

        for next_node_name in node.out_funcs:
            self._traverse_node(
                node=self.graph[next_node_name],
                pipeline_kwargs=pipeline_kwargs,
                seen=seen,
                loop_item_index=loop_item_index,
            )

        if (
            node.matching_join
            and node.matching_join in seen
            and seen[node.matching_join] is None
        ):
            join_node = self.graph[node.matching_join]
            _ = self.create_and_connect(
                join_node, pipeline_kwargs, seen, loop_item_index
            )

            if len(join_node.out_funcs) == 1:
                self._traverse_node(
                    node=self.graph[join_node.out_funcs[0]],
                    pipeline_kwargs=pipeline_kwargs,
                    seen=seen,
                    loop_item_index=loop_item_index,
                )

    def _handle_foreach(
        self,
        node,
        task,
        pipeline_kwargs,
        seen: Dict[str, dsl.PipelineTask],
        outer_loop_index: Optional[Any] = None,
    ):
        splits_out = task.outputs.get("splits_out")
        loop_start_node_name = node.out_funcs[0]

        if node.matching_join and node.matching_join not in seen:
            seen[node.matching_join] = None

        with dsl.ParallelFor(
            ## passing name leads to an error:
            ### failed to resolve inputs: resolving input parameter with spec
            ### task_output_parameter:{producer_task:"for-loop-1"
            ### output_parameter_key:"pipelinechannel--a-component-task_id_out"}:
            ### producer task, for-loop-1_544, not in tasks
            # name=loop_start_node_name,
            items=splits_out,
            parallelism=None,
        ) as inner_loop_index:

            child_node = self.graph[loop_start_node_name]
            child_task = self._traverse_node(
                node=child_node,
                pipeline_kwargs=pipeline_kwargs,
                seen=seen,
                loop_item_index=inner_loop_index,
            )

        collected_task_ids = dsl.Collected(child_task.outputs.get("task_id_out"))

        if (
            node.matching_join
            and node.matching_join in seen
            and seen[node.matching_join] is None
        ):
            join_node = self.graph[node.matching_join]

            _ = self.create_and_connect(
                join_node,
                pipeline_kwargs,
                seen,
                outer_loop_index,
                {f"{loop_start_node_name}_task_ids": collected_task_ids},
            )

            if len(join_node.out_funcs) == 1:
                self._traverse_node(
                    node=self.graph[join_node.out_funcs[0]],
                    pipeline_kwargs=pipeline_kwargs,
                    seen=seen,
                    loop_item_index=outer_loop_index,
                )
