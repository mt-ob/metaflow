import os
import json
from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum


class KFPInternalDecorator(StepDecorator):
    name = "kfp_internal"

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        self.task_id = task_id
        self.run_id = run_id
        self.step_name = step_name
        self.graph = graph

        meta = {}
        meta["kfp-template-owner"] = os.environ.get("METAFLOW_OWNER", "unknown")

        entries = [
            MetaDatum(
                field=k, value=v, type=k, tags=["attempt_id:{0}".format(retry_count)]
            )
            for k, v in meta.items()
        ]

        metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self,
        step_name,
        flow,
        graph,
        is_task_ok,
        retry_count,
        max_user_code_retries,
    ):
        if not is_task_ok:
            return

        node = graph[step_name]

        # Write task_id output (all steps except end and parallel_foreach)
        if node.name != "end" and not node.parallel_foreach:
            task_id_path = os.environ.get("KFP_OUTPUT_task_id_out")
            if task_id_path:
                with open(task_id_path, "w") as f:
                    f.write(self.task_id)

        # Handle foreach outputs
        if node.type == "foreach":
            splits = list(range(flow._foreach_num_splits))

            splits_path = os.environ.get("KFP_OUTPUT_splits_out")
            if splits_path:
                with open(splits_path, "w") as f:
                    json.dump(splits, f)

        # Handle switch step
        if node.type == "split-switch":
            _out_funcs, _ = flow._transition
            chosen_step = _out_funcs[0]

            switch_path = os.environ.get("KFP_OUTPUT_switch_step_out")
            if switch_path:
                with open(switch_path, "w") as f:
                    f.write(chosen_step)
