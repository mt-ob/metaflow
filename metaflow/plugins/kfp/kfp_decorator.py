import os
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
        meta = {}
        # insert more if necessary
        meta["kfp-template-owner"] = os.environ["METAFLOW_OWNER"]

        entries = [
            MetaDatum(
                field=k, value=v, type=k, tags=["attempt_id:{0}".format(retry_count)]
            )
            for k, v in meta.items()
        ]

        metadata.register_metadata(run_id, step_name, task_id, entries)
