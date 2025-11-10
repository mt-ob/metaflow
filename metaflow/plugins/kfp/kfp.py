import os
import sys
import json
import shlex
import tempfile
from io import BytesIO
from kfp import compiler
from datetime import timedelta

from .kfp_utils import KFPTask, KFPFlow
from .exception import KubeflowPipelineException

import metaflow.util as util
from metaflow import current
from metaflow.decorators import flow_decorators
from metaflow.exception import MetaflowException
from metaflow.parameters import deploy_time_eval
from metaflow.metaflow_config_funcs import config_values
from metaflow.plugins.kubernetes.kube_utils import qos_requests_and_limits
from metaflow.mflog import BASH_SAVE_LOGS, bash_capture_logs, export_mflog_env_vars
from metaflow.metaflow_config import (
    AWS_SECRETS_MANAGER_DEFAULT_REGION,
    GCP_SECRET_MANAGER_PREFIX,
    AZURE_STORAGE_BLOB_SERVICE_ENDPOINT,
    CARD_AZUREROOT,
    CARD_GSROOT,
    CARD_S3ROOT,
    DATASTORE_SYSROOT_AZURE,
    DATASTORE_SYSROOT_GS,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    DEFAULT_SECRETS_BACKEND_TYPE,
    KUBERNETES_SECRETS,
    KUBERNETES_SERVICE_ACCOUNT,
    S3_ENDPOINT_URL,
    SERVICE_HEADERS,
    SERVICE_INTERNAL_URL,
    AZURE_KEY_VAULT_PREFIX,
)


class KubeflowPipelines(object):
    TOKEN_STORAGE_ROOT = "mf.kfp"

    def __init__(
        self,
        kfp_client,
        name,
        graph,
        flow,
        code_package_metadata,
        code_package_sha,
        code_package_url,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        production_token,
        tags=None,
        namespace=None,
        username=None,
        max_workers=None,
        description=None,
    ):
        self.kfp_client = kfp_client
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package_metadata = code_package_metadata
        self.code_package_sha = code_package_sha
        self.code_package_url = code_package_url
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.production_token = production_token
        self.tags = tags
        self.namespace = namespace
        self.username = username
        self.max_workers = max_workers
        self.description = description

        _, self.graph_structure = self.graph.output_steps()
        self.parameters = self._process_parameters()

    @classmethod
    def get_existing_deployment(cls, name, flow_datastore):
        _backend = flow_datastore._storage_impl
        token_exists = _backend.is_file([cls.get_token_path(name)])
        if not token_exists[0]:
            return None
        with _backend.load_bytes([cls.get_token_path(name)]) as get_results:
            for _, path, _ in get_results:
                if path is not None:
                    with open(path, "r") as f:
                        data = json.loads(f.read())
                    return (data["owner"], data["production_token"])

    @classmethod
    def get_token_path(cls, name):
        return os.path.join(cls.TOKEN_STORAGE_ROOT, name)

    @classmethod
    def save_deployment_token(cls, owner, name, token, flow_datastore):
        _backend = flow_datastore._storage_impl
        _backend.save_bytes(
            [
                (
                    cls.get_token_path(name),
                    BytesIO(
                        bytes(
                            json.dumps({"production_token": token, "owner": owner}),
                            "utf-8",
                        )
                    ),
                )
            ],
            overwrite=False,
        )

    def _process_parameters(self):
        parameters = {}
        has_schedule = self.flow._flow_decorators.get("schedule") is not None

        seen = set()
        for var, param in self.flow._get_parameters():
            norm = param.name.lower()
            if norm in seen:
                raise MetaflowException(
                    "Parameter *%s* is specified twice. "
                    "Note that parameter names are "
                    "case-insensitive." % param.name
                )
            seen.add(norm)
            if param.IS_CONFIG_PARAMETER:
                continue

            py_type = param.kwargs.get("type", str)
            is_required = param.kwargs.get("required", False)

            # Throw an exception if a schedule is set for a flow with required
            # parameters with no defaults. We currently don't have any notion
            # of data triggers in Argo Workflows.
            if "default" not in param.kwargs and is_required and has_schedule:
                raise MetaflowException(
                    "The parameter *%s* does not have a default and is required. "
                    "Scheduling such parameters via Argo CronWorkflows is not "
                    "currently supported." % param.name
                )
            default_value = deploy_time_eval(param.kwargs.get("default"))

            parameters[param.name] = dict(
                python_var_name=var,
                name=param.name,
                value=default_value,
                type=py_type,
                description=param.kwargs.get("help"),
                is_required=is_required,
            )
        return parameters

    def _get_retries(self, node):
        max_user_code_retries = 0
        max_error_retries = 0
        minutes_between_retries = "2"

        for deco in node.decorators:
            if deco.name == "retry":
                minutes_between_retries = deco.attributes.get(
                    "minutes_between_retries", minutes_between_retries
                )
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries, user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)

        user_code_retries = max_user_code_retries
        total_retries = max_user_code_retries + max_error_retries
        retry_delay = timedelta(minutes=float(minutes_between_retries))

        return user_code_retries, total_retries, retry_delay

    def _get_environment_variables(self, node):
        env_deco = [deco for deco in node.decorators if deco.name == "environment"]
        env = {}
        if env_deco:
            env = env_deco[0].attributes["vars"].copy()

        env["METAFLOW_FLOW_NAME"] = self.flow.name
        env["METAFLOW_STEP_NAME"] = node.name
        env["METAFLOW_OWNER"] = self.username

        metadata_env = self.metadata.get_runtime_environment("kubeflow-pipelines")
        env.update(metadata_env)

        metaflow_version = self.environment.get_environment_info()
        metaflow_version["flow_name"] = self.graph.name
        metaflow_version["production_token"] = self.production_token
        env["METAFLOW_VERSION"] = json.dumps(metaflow_version)

        env.update(
            {
                k: v
                for k, v in config_values()
                if k.startswith("METAFLOW_CONDA_") or k.startswith("METAFLOW_DEBUG_")
            }
        )

        additional_mf_variables = {
            "AWS_ACCESS_KEY_ID": "rootuser",
            "AWS_SECRET_ACCESS_KEY": "rootpass123",
            "AWS_ENDPOINT_URL_S3": "http://host.docker.internal:9000",
            "METAFLOW_CODE_METADATA": self.code_package_metadata,
            "METAFLOW_CODE_SHA": self.code_package_sha,
            "METAFLOW_CODE_URL": self.code_package_url,
            "METAFLOW_CODE_DS": self.flow_datastore.TYPE,
            "METAFLOW_USER": "kubeflow-pipelines",
            "METAFLOW_SERVICE_URL": "http://host.docker.internal:8080",
            # "METAFLOW_SERVICE_URL": SERVICE_INTERNAL_URL,
            "METAFLOW_SERVICE_HEADERS": json.dumps(SERVICE_HEADERS),
            "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
            "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
            "METAFLOW_DEFAULT_DATASTORE": self.flow_datastore.TYPE,
            "METAFLOW_DEFAULT_METADATA": "service",
            "METAFLOW_RUNTIME_ENVIRONMENT": "kubernetes",
            "METAFLOW_CARD_S3ROOT": CARD_S3ROOT,
            "METAFLOW_PRODUCTION_TOKEN": self.production_token,
            "METAFLOW_DATASTORE_SYSROOT_GS": DATASTORE_SYSROOT_GS,
            "METAFLOW_CARD_GSROOT": CARD_GSROOT,
            "METAFLOW_S3_ENDPOINT_URL": S3_ENDPOINT_URL,
            "METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT": AZURE_STORAGE_BLOB_SERVICE_ENDPOINT,
            "METAFLOW_DATASTORE_SYSROOT_AZURE": DATASTORE_SYSROOT_AZURE,
            "METAFLOW_CARD_AZUREROOT": CARD_AZUREROOT,
            "METAFLOW_RUN_ID": "kfp-{{workflow.name}}",
            "METAFLOW_KUBERNETES_WORKLOAD": str(1),
        }

        if DEFAULT_SECRETS_BACKEND_TYPE:
            env["METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE"] = DEFAULT_SECRETS_BACKEND_TYPE
        if AWS_SECRETS_MANAGER_DEFAULT_REGION:
            env["METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION"] = (
                AWS_SECRETS_MANAGER_DEFAULT_REGION
            )
        if GCP_SECRET_MANAGER_PREFIX:
            env["METAFLOW_GCP_SECRET_MANAGER_PREFIX"] = GCP_SECRET_MANAGER_PREFIX
        if AZURE_KEY_VAULT_PREFIX:
            env["METAFLOW_AZURE_KEY_VAULT_PREFIX"] = AZURE_KEY_VAULT_PREFIX

        env.update(additional_mf_variables)

        return {k: v for k, v in env.items() if v is not None}

    def _get_kubernetes_resources(self, node):
        k8s_deco = [deco for deco in node.decorators if deco.name == "kubernetes"][0]
        resources = k8s_deco.attributes

        labels = {
            "app": "metaflow",
            "app.kubernetes.io/name": "metaflow-task",
            "app.kubernetes.io/part-of": "metaflow",
            "app.kubernetes.io/created-by": util.get_username(),
        }

        service_account = (
            KUBERNETES_SERVICE_ACCOUNT
            if resources["service_account"] is None
            else resources["service_account"]
        )

        k8s_namespace = (
            resources["namespace"] if resources["namespace"] is not None else "default"
        )

        qos_requests, qos_limits = qos_requests_and_limits(
            resources["qos"],
            resources["cpu"],
            resources["memory"],
            resources["disk"],
        )

        pod_resources = dict(
            requests=qos_requests,
            limits={
                **qos_limits,
                **{
                    "%s.com/gpu".lower()
                    % resources["gpu_vendor"]: str(resources["gpu"])
                    for k in [0]
                    # Don't set GPU limits if gpu isn't specified.
                    if resources["gpu"] is not None
                },
            },
        )

        annotations = {
            "metaflow/production_token": self.production_token,
            "metaflow/owner": self.username,
            "metaflow/user": self.username,
            "metaflow/flow_name": self.flow.name,
        }
        if current.get("project_name"):
            annotations.update(
                {
                    "metaflow/project_name": current.project_name,
                    "metaflow/branch_name": current.branch_name,
                    "metaflow/project_flow_name": current.project_flow_name,
                }
            )

        secrets = []
        if resources["secrets"]:
            if isinstance(resources["secrets"], str):
                secrets = resources["secrets"].split(",")
            elif isinstance(resources["secrets"], list):
                secrets = resources["secrets"]
        if len(KUBERNETES_SECRETS) > 0:
            secrets += KUBERNETES_SECRETS.split(",")

        return {
            "image": resources["image"],
            "labels": labels,
            "annotations": annotations,
            "service_account": service_account,
            "namespace": k8s_namespace,
            "secrets": list(set(secrets)),
            "pod_resources": pod_resources,
        }

    def create_kfp_task(self, node):
        inputs = {}
        outputs = {}

        if node.name == "start":
            for param_name, param_info in self.parameters.items():
                inputs[param_name] = param_info["type"]

        if node.name != "end":
            outputs["task_id_out"] = str

        for parent_name in node.in_funcs:
            inputs[f"{parent_name}_task_id"] = str

        resources = self._get_kubernetes_resources(node)
        env_vars = self._get_environment_variables(node)

        kfp_task_obj = KFPTask(
            name="%s_component" % node.name,
            image=resources["image"],
            command=["bash", "-c"],
            args=[],
            inputs=inputs,
            outputs=outputs,
            env_vars=env_vars,
            k8s_resources=resources,
        )

        command_str = self._step_cli(node, kfp_task_obj)
        args = [command_str]

        if node.name == "start":
            # Order: outputs, then params
            args.extend([f"{{{{$.outputs.parameters['task_id_out'].output_file}}}}"])
            args.extend(
                [
                    f"{{{{$.inputs.parameters['{p_name}']}}}}"
                    for p_name in self.parameters
                ]
            )
        else:
            # Order: parent_task_ids, then outputs (if any)
            args.extend(
                [
                    f"{{{{$.inputs.parameters['{p_name}_task_id']}}}}"
                    for p_name in node.in_funcs
                ]
            )
            if node.name != "end":
                args.extend(
                    [f"{{{{$.outputs.parameters['task_id_out'].output_file}}}}"]
                )

        kfp_task_obj.args = args
        return kfp_task_obj

    def _step_cli(self, node, kfp_task):
        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)
        entrypoint = [executable, script_name]

        run_id = "kfp-{{workflow.name}}"
        task_idx = ""
        input_paths = ""
        root_input = None

        task_id_base_parts = [
            node.name,
            "{{workflow.creationTimestamp}}",
            root_input or input_paths,
            task_idx,
        ]

        if node.name == "start":
            for i in range(len(self.parameters)):
                task_id_base_parts.append(f"${i+1}")
        else:
            for i in range(len(node.in_funcs)):
                task_id_base_parts.append(f"${i}")

        # Task string to be hashed into an ID
        task_str = "-".join(task_id_base_parts)
        _task_id_base = (
            "$(echo -n %s | md5sum | cut -d ' ' -f 1 | tail -c 9)" % task_str
        )
        task_str = "t-%s" % _task_id_base

        task_id_expr = "export METAFLOW_TASK_ID=%s" % task_str
        task_id = "$METAFLOW_TASK_ID"

        user_code_retries, total_retries, _ = self._get_retries(node)

        retry_count = (
            (
                "{{retries}}"
                if not node.parallel_step
                else "{{inputs.parameters.retryCount}}"
            )
            if total_retries
            else 0
        )

        # Configure log capture.
        mflog_expr = export_mflog_env_vars(
            datastore_type=self.flow_datastore.TYPE,
            stdout_path="$PWD/.logs/mflog_stdout",
            stderr_path="$PWD/.logs/mflog_stderr",
            flow_name=self.flow.name,
            run_id=run_id,
            step_name=node.name,
            task_id=task_id,
            retry_count=retry_count,
        )

        init_cmds = " && ".join(
            [
                # For supporting sandboxes, ensure that a custom script is executed
                # before anything else is executed. The script is passed in as an
                # env var.
                '${METAFLOW_INIT_SCRIPT:+eval \\"${METAFLOW_INIT_SCRIPT}\\"}',
                "mkdir -p $PWD/.logs",
                task_id_expr,
                mflog_expr,
            ]
            + self.environment.get_package_commands(
                self.code_package_url,
                self.flow_datastore.TYPE,
                self.code_package_metadata,
            )
        )

        step_cmds = self.environment.bootstrap_commands(
            node.name, self.flow_datastore.TYPE
        )

        top_opts_dict = {
            "with": [
                decorator.make_decorator_spec()
                for decorator in node.decorators
                if not decorator.statically_defined and decorator.inserted_by is None
            ]
        }

        # FlowDecorators can define their own top-level options. They are
        # responsible for adding their own top-level options and values through
        # the get_top_level_options() hook. See similar logic in runtime.py.
        for deco in flow_decorators(self.flow):
            top_opts_dict.update(deco.get_top_level_options())

        top_opts = list(util.dict_to_cli_options(top_opts_dict))

        top_level = top_opts + [
            "--quiet",
            "--metadata=%s" % self.metadata.TYPE,
            "--environment=%s" % self.environment.TYPE,
            "--datastore=%s" % self.flow_datastore.TYPE,
            "--datastore-root=%s" % self.flow_datastore.datastore_root,
            "--event-logger=%s" % self.event_logger.TYPE,
            "--monitor=%s" % self.monitor.TYPE,
            "--no-pylint",
            "--with=kfp_internal",
        ]

        if node.name == "start":
            # Execute `init` before any step of the workflow executes
            task_id_params = "%s-params" % task_id
            param_cli_args = []
            for i, p in enumerate(self.parameters.values()):
                # $0 is output_file, $1 is first param, etc.
                param_cli_args.append('--%s "$%d"' % (p["name"], i + 1))

            init = (
                entrypoint
                + top_level
                + [
                    "init",
                    "--run-id %s" % run_id,
                    "--task-id %s" % task_id_params,
                ]
                + param_cli_args
            )

            if self.tags:
                init.extend("--tag %s" % tag for tag in self.tags)
            # if the start step gets retried, we must be careful
            # not to regenerate multiple parameters tasks. Hence,
            # we check first if _parameters exists already.
            exists = entrypoint + [
                "dump",
                "--max-value-size=0",
                "%s/_parameters/%s" % (run_id, task_id_params),
            ]

            step_cmds.extend(
                [
                    "if ! %s >/dev/null 2>/dev/null; then %s; fi"
                    % (" ".join(exists), " ".join(init))
                ]
            )
            input_paths = "%s/_parameters/%s" % (run_id, task_id_params)
        else:
            input_paths_parts = []
            for i, parent_name in enumerate(node.in_funcs):
                input_paths_parts.append(f"{run_id}/{parent_name}/${i}")

            input_paths = ",".join(input_paths_parts)

        # NOTE: input-paths might be extremely lengthy so we dump
        # these to diskinstead of passing them directly to the cmd
        step_cmds.append("echo -n %s >> /tmp/mf-input-paths" % input_paths)

        step = [
            "step",
            node.name,
            "--run-id %s" % run_id,
            "--task-id %s" % task_id,
            "--retry-count %s" % retry_count,
            "--max-user-code-retries %d" % user_code_retries,
            "--input-paths-filename /tmp/mf-input-paths",
        ]

        if self.tags:
            step.extend("--tag %s" % tag for tag in self.tags)
        if self.namespace is not None:
            step.append("--namespace=%s" % self.namespace)

        step_cmds.extend([" ".join(entrypoint + top_level + step)])

        final_cmds = ["c=$?", BASH_SAVE_LOGS]
        if "task_id_out" in kfp_task.outputs:
            if node.name == "start":
                output_file_var = "$0"
            else:
                output_file_var = f"${len(node.in_funcs)}"
            final_cmds.append(f"echo -n {task_id} > {output_file_var}")
        final_cmds.append("exit $c")

        cmd_str = "%s; %s" % (
            " && ".join([init_cmds, bash_capture_logs(" && ".join(step_cmds))]),
            "; ".join(final_cmds),
        )

        cmds = shlex.split('bash -c "%s"' % cmd_str)
        return cmds[2]

    def compile_and_upload(self):
        if self.flow._flow_decorators.get("trigger") or self.flow._flow_decorators.get(
            "trigger_on_finish"
        ):
            raise KubeflowPipelineException(
                "Deploying flows with @trigger or @trigger_on_finish decorator(s) "
                "to Kubeflow Pipelines is not supported currently."
            )

        if self.flow._flow_decorators.get("exit_hook"):
            raise KubeflowPipelineException(
                "Deploying flows with the @exit_hook decorator "
                "to Kubeflow Pipelines is not currently supported."
            )

        component_tasks = {}
        for node in self.graph:
            component_tasks[node.name] = self.create_kfp_task(node)

        pipeline_func = KFPFlow(
            self.name,
            self.graph,
            self.parameters,
            component_tasks,
        ).get_pipeline_func()

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=True
        ) as tmp_file:
            compiler.Compiler().compile(pipeline_func, tmp_file.name)
            self.kfp_client.upload_pipeline(
                pipeline_package_path=tmp_file.name,
                pipeline_name=self.name,
                namespace=None,
            )
