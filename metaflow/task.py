from __future__ import print_function
from io import BytesIO
import math
import sys
import os
import time
import traceback


from types import MethodType, FunctionType

from metaflow.sidecar import Message, MessageTypes
from metaflow.datastore.exceptions import DataException

from .metaflow_config import MAX_ATTEMPTS
from .metadata_provider import MetaDatum
from .mflog import TASK_LOG_SOURCE
from .datastore import Inputs, TaskDataStoreSet
from .exception import (
    MetaflowInternalError,
    MetaflowDataMissing,
    MetaflowExceptionWrapper,
)
from .unbounded_foreach import UBF_CONTROL
from .util import all_equal, get_username, resolve_identity, unicode_type
from .clone_util import clone_task_helper
from .metaflow_current import current
from metaflow.user_configs.config_parameters import ConfigValue
from metaflow.system import _system_logger, _system_monitor
from metaflow.tracing import get_trace_id
from metaflow.tuple_util import ForeachFrame

# Maximum number of characters of the foreach path that we store in the metadata.
MAX_FOREACH_PATH_LENGTH = 256


class MetaflowTask(object):
    """
    MetaflowTask prepares a Flow instance for execution of a single step.
    """

    def __init__(
        self,
        flow,
        flow_datastore,
        metadata,
        environment,
        console_logger,
        event_logger,
        monitor,
        ubf_context,
    ):
        self.flow = flow
        self.flow_datastore = flow_datastore
        self.metadata = metadata
        self.environment = environment
        self.console_logger = console_logger
        self.event_logger = event_logger
        self.monitor = monitor
        self.ubf_context = ubf_context

    def _exec_step_function(self, step_function, orig_step_func, input_obj=None):
        wrappers_stack = []
        wrapped_func = None
        do_next = False
        raised_exception = None
        # If we have wrappers w1, w2 and w3, we need to execute
        #  - w3_pre
        #  - w2_pre
        #  - w1_pre
        #  - step_function
        #  - w1_post
        #  - w2_post
        #  - w3_post
        # in that order. We do this by maintaining a stack of generators.
        # Note that if any of the pre functions returns a function, we execute that
        # instead of the rest of the inside part. This is useful if you want to create
        # no-op function for example.
        for w in reversed(orig_step_func.wrappers):
            wrapped_func = w.pre_step(orig_step_func.name, self.flow, input_obj)
            wrappers_stack.append(w)
            if w.skip_step:
                # We have nothing to run
                do_next = w.skip_step
                break
            if wrapped_func:
                break  # We have nothing left to do since we now execute the
                # wrapped function
            # Else, we continue down the list of wrappers
        try:
            if not do_next:
                if input_obj is None:
                    if wrapped_func:
                        do_next = wrapped_func(self.flow)
                        if not do_next:
                            do_next = True
                    else:
                        step_function()
                else:
                    if wrapped_func:
                        do_next = wrapped_func(self.flow, input_obj)
                        if not do_next:
                            do_next = True
                    else:
                        step_function(input_obj)
        except Exception as ex:
            raised_exception = ex

        if do_next or raised_exception:
            # If we are skipping the step, or executed a wrapped function,
            # we need to set the transition variables
            # properly. We call the next function as needed
            # We also do this in case we want to gobble the exception.
            graph_node = self.flow._graph[orig_step_func.name]
            out_funcs = [getattr(self.flow, f) for f in graph_node.out_funcs]
            if out_funcs:
                if isinstance(do_next, bool) or raised_exception:
                    # We need to extract things from the self.next. This is not possible
                    # in the case where there was a num_parallel.
                    if graph_node.parallel_foreach:
                        raise RuntimeError(
                            "Skipping a parallel foreach step without providing "
                            "the arguments to the self.next call is not supported. "
                        )
                    if graph_node.foreach_param:
                        self.flow.next(*out_funcs, foreach=graph_node.foreach_param)
                    else:
                        self.flow.next(*out_funcs)
                elif isinstance(do_next, dict):
                    # Here it is a dictionary so we just call the next method with
                    # those arguments
                    self.flow.next(*out_funcs, **do_next)
                else:
                    raise RuntimeError(
                        "Invalid value passed to self.next; expected "
                        " bool of a dictionary; got: %s" % do_next
                    )
        # We back out of the stack of generators
        for w in reversed(wrappers_stack):
            raised_exception = w.post_step(
                orig_step_func.name, self.flow, raised_exception
            )
        if raised_exception:
            # We have an exception that we need to propagate
            raise raised_exception

    def _init_parameters(self, parameter_ds, passdown=True):
        cls = self.flow.__class__

        def _set_cls_var(_, __):
            raise AttributeError(
                "Flow level attributes and Parameters are not modifiable"
            )

        def set_as_parameter(name, value):
            if callable(value):
                setattr(cls, name, property(fget=value, fset=_set_cls_var))
            else:
                setattr(
                    cls,
                    name,
                    property(fget=lambda _, val=value: val, fset=_set_cls_var),
                )

        # overwrite Parameters in the flow object
        all_vars = []
        for var, param in self.flow._get_parameters():
            # make the parameter a read-only property
            # note x=x binds the current value of x to the closure
            def property_setter(
                _,
                param=param,
                var=var,
                parameter_ds=parameter_ds,
            ):
                v = param.load_parameter(parameter_ds[var])
                # Reset the parameter to just return the value now that we have loaded it
                set_as_parameter(var, v)
                return v

            set_as_parameter(var, property_setter)
            all_vars.append(var)

        param_only_vars = list(all_vars)
        # make class-level values read-only to be more consistent across steps in a flow
        # they are also only persisted once, so we similarly pass them down if
        # required
        for var in dir(cls):
            if var[0] == "_" or var in cls._NON_PARAMETERS or var in all_vars:
                continue
            val = getattr(cls, var)
            # Exclude methods, properties and other classes
            if isinstance(val, (MethodType, FunctionType, property, type)):
                continue
            set_as_parameter(var, val)
            all_vars.append(var)

        # We also passdown _graph_info through the entire graph
        set_as_parameter(
            "_graph_info",
            lambda _, parameter_ds=parameter_ds: parameter_ds["_graph_info"],
        )
        all_vars.append("_graph_info")

        if passdown:
            self.flow._datastore.passdown_partial(parameter_ds, all_vars)
        return param_only_vars

    def _init_data(self, run_id, join_type, input_paths):
        # We prefer to use the parallelized version to initialize datastores
        # (via TaskDataStoreSet) only with more than 4 datastores, because
        # the baseline overhead of using the set is ~1.5s and each datastore
        # init takes ~200-300ms when run sequentially.
        if len(input_paths) > 4:
            prefetch_data_artifacts = None
            if join_type and join_type == "foreach":
                # Prefetch 'foreach' related artifacts to improve time taken by
                # _init_foreach.
                prefetch_data_artifacts = [
                    "_foreach_stack",
                    "_foreach_num_splits",
                    "_foreach_var",
                ]
            # Note: Specify `pathspecs` while creating the datastore set to
            # guarantee strong consistency and guard against missing input.
            datastore_set = TaskDataStoreSet(
                self.flow_datastore,
                run_id,
                pathspecs=input_paths,
                prefetch_data_artifacts=prefetch_data_artifacts,
            )
            ds_list = [ds for ds in datastore_set]
            if len(ds_list) != len(input_paths):
                raise MetaflowDataMissing(
                    "Some input datastores are missing. "
                    "Expected: %d Actual: %d" % (len(input_paths), len(ds_list))
                )
        else:
            # initialize directly in the single input case.
            ds_list = []
            for input_path in input_paths:
                run_id, step_name, task_id = input_path.split("/")
                ds_list.append(
                    self.flow_datastore.get_task_datastore(run_id, step_name, task_id)
                )
        if not ds_list:
            # this guards against errors in input paths
            raise MetaflowDataMissing(
                "Input paths *%s* resolved to zero inputs" % ",".join(input_paths)
            )
        return ds_list

    def _init_foreach(self, step_name, join_type, inputs, split_index):
        # these variables are only set by the split step in the output
        # data. They don't need to be accessible in the flow.
        self.flow._foreach_var = None
        self.flow._foreach_num_splits = None

        # There are three cases that can alter the foreach state:
        # 1) start - initialize an empty foreach stack
        # 2) join - pop the topmost frame from the stack
        # 3) step following a split - push a new frame in the stack

        # We have a non-modifying case (case 4)) where we propagate the
        # foreach-stack information to all tasks in the foreach. This is
        # then used later to write the foreach-stack metadata for that task

        # case 1) - reset the stack
        if step_name == "start":
            self.flow._foreach_stack = []

        # case 2) - this is a join step
        elif join_type:
            # assert the lineage of incoming branches
            def lineage():
                for i in inputs:
                    if join_type == "foreach":
                        top = i["_foreach_stack"][-1]
                        bottom = i["_foreach_stack"][:-1]
                        # the topmost indices and values in the stack are
                        # all different naturally, so ignore them in the
                        # assertion
                        yield bottom + [top._replace(index=0, value=0)]
                    else:
                        yield i["_foreach_stack"]

            if not all_equal(lineage()):
                raise MetaflowInternalError(
                    "Step *%s* tried to join branches "
                    "whose lineages don't match." % step_name
                )

            # assert that none of the inputs are splits - we don't
            # allow empty `foreach`s (joins immediately following splits)
            if any(not i.is_none("_foreach_var") for i in inputs):
                raise MetaflowInternalError(
                    "Step *%s* tries to join a foreach "
                    "split with no intermediate steps." % step_name
                )

            inp = inputs[0]
            if join_type == "foreach":
                # Make sure that the join got all splits as its inputs.
                # Datastore.resolve() leaves out all undone tasks, so if
                # something strange happened upstream, the inputs list
                # may not contain all inputs which should raise an exception
                stack = inp["_foreach_stack"]
                if stack[-1].num_splits and len(inputs) != stack[-1].num_splits:
                    raise MetaflowDataMissing(
                        "Foreach join *%s* expected %d "
                        "splits but only %d inputs were "
                        "found" % (step_name, stack[-1].num_splits, len(inputs))
                    )
                # foreach-join pops the topmost frame from the stack
                self.flow._foreach_stack = stack[:-1]
            else:
                # a non-foreach join doesn't change the stack
                self.flow._foreach_stack = inp["_foreach_stack"]

        # case 3) - our parent was a split. Initialize a new foreach frame.
        elif not inputs[0].is_none("_foreach_var"):
            if len(inputs) != 1:
                raise MetaflowInternalError(
                    "Step *%s* got multiple inputs "
                    "although it follows a split step." % step_name
                )

            if self.ubf_context != UBF_CONTROL and split_index is None:
                raise MetaflowInternalError(
                    "Step *%s* follows a split step "
                    "but no split_index is "
                    "specified." % step_name
                )

            split_value = (
                inputs[0]["_foreach_values"][split_index]
                if not inputs[0].is_none("_foreach_values")
                else None
            )
            # push a new index after a split to the stack
            frame = ForeachFrame(
                step_name,
                inputs[0]["_foreach_var"],
                inputs[0]["_foreach_num_splits"],
                split_index,
                split_value,
            )

            stack = inputs[0]["_foreach_stack"]
            stack.append(frame)
            self.flow._foreach_stack = stack
        # case 4) - propagate in the foreach nest
        elif "_foreach_stack" in inputs[0]:
            self.flow._foreach_stack = inputs[0]["_foreach_stack"]

    def _clone_flow(self, datastore):
        x = self.flow.__class__(use_cli=False)
        x._set_datastore(datastore)
        return x

    def clone_only(
        self,
        step_name,
        run_id,
        task_id,
        clone_origin_task,
        retry_count,
    ):
        if not clone_origin_task:
            raise MetaflowInternalError(
                "task.clone_only needs a valid clone_origin_task value."
            )
        origin_run_id, _, origin_task_id = clone_origin_task.split("/")
        # Update system logger and monitor context
        # We also pass this context as part of the task payload to support implementations that
        # can't access the context directly
        task_payload = {
            "run_id": run_id,
            "step_name": step_name,
            "task_id": task_id,
            "retry_count": retry_count,
            "project_name": current.get("project_name"),
            "branch_name": current.get("branch_name"),
            "is_user_branch": current.get("is_user_branch"),
            "is_production": current.get("is_production"),
            "project_flow_name": current.get("project_flow_name"),
            "origin_run_id": origin_run_id,
            "origin_task_id": origin_task_id,
        }

        msg = "Cloning task from {}/{}/{}/{} to {}/{}/{}/{}".format(
            self.flow.name,
            origin_run_id,
            step_name,
            origin_task_id,
            self.flow.name,
            run_id,
            step_name,
            task_id,
        )
        with _system_monitor.count("metaflow.task.clone"):
            _system_logger.log_event(
                level="info",
                module="metaflow.task",
                name="clone",
                payload={**task_payload, "msg": msg},
            )
        # If we actually have to do the clone ourselves, proceed...
        clone_task_helper(
            self.flow.name,
            origin_run_id,
            run_id,
            step_name,
            origin_task_id,
            task_id,
            self.flow_datastore,
            self.metadata,
            attempt_id=retry_count,
        )

    def _finalize_control_task(self):
        # Update `_transition` which is expected by the NativeRuntime.
        step_name = self.flow._current_step
        next_steps = self.flow._graph[step_name].out_funcs
        self.flow._transition = (next_steps, None)
        if self.flow._task_ok:
            # Throw an error if `_control_mapper_tasks` isn't populated.
            mapper_tasks = self.flow._control_mapper_tasks
            if not mapper_tasks:
                msg = (
                    "Step *{step}* has a control task which didn't "
                    "specify the artifact *_control_mapper_tasks* for "
                    "the subsequent *{join}* step."
                )
                raise MetaflowInternalError(
                    msg.format(step=step_name, join=next_steps[0])
                )
            elif not (
                isinstance(mapper_tasks, list)
                and isinstance(mapper_tasks[0], unicode_type)
            ):
                msg = (
                    "Step *{step}* has a control task which didn't "
                    "specify the artifact *_control_mapper_tasks* as a "
                    "list of strings but instead specified it as {typ} "
                    "with elements of {elem_typ}."
                )
                raise MetaflowInternalError(
                    msg.format(
                        step=step_name,
                        typ=type(mapper_tasks),
                        elem_typ=type(mapper_tasks[0]),
                    )
                )

    def run_step(
        self,
        step_name,
        run_id,
        task_id,
        origin_run_id,
        input_paths,
        split_index,
        retry_count,
        max_user_code_retries,
    ):
        if run_id and task_id:
            self.metadata.register_run_id(run_id)
            self.metadata.register_task_id(run_id, step_name, task_id, retry_count)
        else:
            raise MetaflowInternalError(
                "task.run_step needs a valid run_id and task_id"
            )

        if retry_count >= MAX_ATTEMPTS:
            # any results with an attempt ID >= MAX_ATTEMPTS will be ignored
            # by datastore, so running a task with such a retry_could would
            # be pointless and dangerous
            raise MetaflowInternalError(
                "Too many task attempts (%d)! MAX_ATTEMPTS exceeded." % retry_count
            )

        metadata_tags = ["attempt_id:{0}".format(retry_count)]

        metadata = [
            MetaDatum(
                field="attempt",
                value=str(retry_count),
                type="attempt",
                tags=metadata_tags,
            ),
            MetaDatum(
                field="origin-run-id",
                value=str(origin_run_id),
                type="origin-run-id",
                tags=metadata_tags,
            ),
            MetaDatum(
                field="ds-type",
                value=self.flow_datastore.TYPE,
                type="ds-type",
                tags=metadata_tags,
            ),
            MetaDatum(
                field="ds-root",
                value=self.flow_datastore.datastore_root,
                type="ds-root",
                tags=metadata_tags,
            ),
        ]
        trace_id = get_trace_id()
        if trace_id:
            metadata.append(
                MetaDatum(
                    field="otel-trace-id",
                    value=trace_id,
                    type="trace-id",
                    tags=metadata_tags,
                )
            )

        step_func = getattr(self.flow, step_name)
        decorators = step_func.decorators

        node = self.flow._graph[step_name]
        join_type = None
        if node.type == "join":
            join_type = self.flow._graph[node.split_parents[-1]].type

        # 1. initialize output datastore
        output = self.flow_datastore.get_task_datastore(
            run_id, step_name, task_id, attempt=retry_count, mode="w"
        )

        output.init_task()

        if input_paths:
            # 2. initialize input datastores
            inputs = self._init_data(run_id, join_type, input_paths)

            # 3. initialize foreach state
            self._init_foreach(step_name, join_type, inputs, split_index)

            # Add foreach stack to metadata of the task

            foreach_stack = (
                self.flow._foreach_stack
                if hasattr(self.flow, "_foreach_stack") and self.flow._foreach_stack
                else []
            )

            foreach_stack_formatted = []
            current_foreach_path_length = 0
            for frame in foreach_stack:
                if not (frame.var and frame.value):
                    break

                foreach_step = "%s=%s" % (frame.var, frame.value)
                if (
                    current_foreach_path_length + len(foreach_step)
                    > MAX_FOREACH_PATH_LENGTH
                ):
                    break
                current_foreach_path_length += len(foreach_step)
                foreach_stack_formatted.append(foreach_step)

            if foreach_stack_formatted:
                metadata.append(
                    MetaDatum(
                        field="foreach-stack",
                        value=foreach_stack_formatted,
                        type="foreach-stack",
                        tags=metadata_tags,
                    )
                )

            # Add runtime dag information to the metadata of the task
            foreach_execution_path = ",".join(
                [
                    "{}:{}".format(foreach_frame.step, foreach_frame.index)
                    for foreach_frame in foreach_stack
                ]
            )
            if foreach_execution_path:
                metadata.extend(
                    [
                        MetaDatum(
                            field="foreach-execution-path",
                            value=foreach_execution_path,
                            type="foreach-execution-path",
                            tags=metadata_tags,
                        ),
                    ]
                )

        self.metadata.register_metadata(
            run_id,
            step_name,
            task_id,
            metadata,
        )

        # 4. initialize the current singleton
        current._set_env(
            flow=self.flow,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            retry_count=retry_count,
            origin_run_id=origin_run_id,
            namespace=resolve_identity(),
            username=get_username(),
            metadata_str=self.metadata.metadata_str(),
            is_running=True,
            tags=self.metadata.sticky_tags,
        )

        # 5. run task
        output.save_metadata(
            {
                "task_begin": {
                    "code_package_metadata": os.environ.get(
                        "METAFLOW_CODE_METADATA", ""
                    ),
                    "code_package_sha": os.environ.get("METAFLOW_CODE_SHA"),
                    "code_package_ds": os.environ.get("METAFLOW_CODE_DS"),
                    "code_package_url": os.environ.get("METAFLOW_CODE_URL"),
                    "retry_count": retry_count,
                }
            }
        )

        # 6. Update system logger and monitor context
        # We also pass this context as part of the task payload to support implementations that
        # can't access the context directly

        task_payload = {
            "run_id": run_id,
            "step_name": step_name,
            "task_id": task_id,
            "retry_count": retry_count,
            "project_name": current.get("project_name"),
            "branch_name": current.get("branch_name"),
            "is_user_branch": current.get("is_user_branch"),
            "is_production": current.get("is_production"),
            "project_flow_name": current.get("project_flow_name"),
            "trace_id": trace_id or None,
        }
        start = time.time()
        self.metadata.start_task_heartbeat(self.flow.name, run_id, step_name, task_id)
        with self.monitor.measure("metaflow.task.duration"):
            try:
                with self.monitor.count("metaflow.task.start"):
                    _system_logger.log_event(
                        level="info",
                        module="metaflow.task",
                        name="start",
                        payload={**task_payload, "msg": "Task started"},
                    )

                self.flow._current_step = step_name
                self.flow._success = False
                self.flow._task_ok = None
                self.flow._exception = None

                # Note: All internal flow attributes (ie: non-user artifacts)
                # should either be set prior to running the user code or listed in
                # FlowSpec._EPHEMERAL to allow for proper merging/importing of
                # user artifacts in the user's step code.

                if join_type:
                    # Join step:

                    # Ensure that we have the right number of inputs. The
                    # foreach case is checked above.
                    if join_type != "foreach" and len(inputs) != len(node.in_funcs):
                        raise MetaflowDataMissing(
                            "Join *%s* expected %d "
                            "inputs but only %d inputs "
                            "were found" % (step_name, len(node.in_funcs), len(inputs))
                        )

                    # Multiple input contexts are passed in as an argument
                    # to the step function.
                    input_obj = Inputs(self._clone_flow(inp) for inp in inputs)
                    self.flow._set_datastore(output)
                    # initialize parameters (if they exist)
                    # We take Parameter values from the first input,
                    # which is always safe since parameters are read-only
                    current._update_env(
                        {
                            "parameter_names": self._init_parameters(
                                inputs[0], passdown=True
                            ),
                            "graph_info": self.flow._graph_info,
                        }
                    )
                else:
                    # Linear step:
                    # We are running with a single input context.
                    # The context is embedded in the flow.
                    if len(inputs) > 1:
                        # This should be captured by static checking but
                        # let's assert this again
                        raise MetaflowInternalError(
                            "Step *%s* is not a join "
                            "step but it gets multiple "
                            "inputs." % step_name
                        )
                    self.flow._set_datastore(inputs[0])
                    if input_paths:
                        # initialize parameters (if they exist)
                        # We take Parameter values from the first input,
                        # which is always safe since parameters are read-only
                        current._update_env(
                            {
                                "parameter_names": self._init_parameters(
                                    inputs[0], passdown=False
                                ),
                                "graph_info": self.flow._graph_info,
                            }
                        )
                for deco in decorators:
                    deco.task_pre_step(
                        step_name,
                        output,
                        self.metadata,
                        run_id,
                        task_id,
                        self.flow,
                        self.flow._graph,
                        retry_count,
                        max_user_code_retries,
                        self.ubf_context,
                        inputs,
                    )

                orig_step_func = step_func
                for deco in decorators:
                    # decorators can actually decorate the step function,
                    # or they can replace it altogether. This functionality
                    # is used e.g. by catch_decorator which switches to a
                    # fallback code if the user code has failed too many
                    # times.
                    step_func = deco.task_decorate(
                        step_func,
                        self.flow,
                        self.flow._graph,
                        retry_count,
                        max_user_code_retries,
                        self.ubf_context,
                    )

                if join_type:
                    self._exec_step_function(step_func, orig_step_func, input_obj)
                else:
                    self._exec_step_function(step_func, orig_step_func)

                for deco in decorators:
                    deco.task_post_step(
                        step_name,
                        self.flow,
                        self.flow._graph,
                        retry_count,
                        max_user_code_retries,
                    )

                self.flow._task_ok = True
                self.flow._success = True

            except Exception as ex:
                with self.monitor.count("metaflow.task.exception"):
                    _system_logger.log_event(
                        level="error",
                        module="metaflow.task",
                        name="exception",
                        payload={**task_payload, "msg": traceback.format_exc()},
                    )

                exception_handled = False
                for deco in decorators:
                    res = deco.task_exception(
                        ex,
                        step_name,
                        self.flow,
                        self.flow._graph,
                        retry_count,
                        max_user_code_retries,
                    )
                    exception_handled = bool(res) or exception_handled

                if exception_handled:
                    self.flow._task_ok = True
                else:
                    self.flow._task_ok = False
                    self.flow._exception = MetaflowExceptionWrapper(ex)
                    print("%s failed:" % self.flow, file=sys.stderr)
                    raise

            finally:
                if self.ubf_context == UBF_CONTROL:
                    self._finalize_control_task()

                # Emit metrics to logger/monitor sidecar implementations
                with self.monitor.count("metaflow.task.end"):
                    _system_logger.log_event(
                        level="info",
                        module="metaflow.task",
                        name="end",
                        payload={**task_payload, "msg": "Task ended"},
                    )
                try:
                    # persisting might fail due to unpicklable artifacts.
                    output.persist(self.flow)
                except Exception as ex:
                    self.flow._task_ok = False
                    raise ex
                finally:
                    # The attempt_ok metadata is used to determine task status so it is important
                    # we ensure that it is written even in case of preceding failures.
                    # f.ex. failing to serialize artifacts leads to a non-zero exit code for the process,
                    # even if user code finishes successfully. Flow execution will not continue due to the exit,
                    # so arguably we should mark the task as failed.
                    attempt_ok = str(bool(self.flow._task_ok))
                    self.metadata.register_metadata(
                        run_id,
                        step_name,
                        task_id,
                        [
                            MetaDatum(
                                field="attempt_ok",
                                value=attempt_ok,
                                type="internal_attempt_status",
                                tags=["attempt_id:{0}".format(retry_count)],
                            ),
                        ],
                    )

                output.save_metadata({"task_end": {}})

                # this writes a success marker indicating that the
                # "transaction" is done
                output.done()

                # final decorator hook: The task results are now
                # queryable through the client API / datastore
                for deco in decorators:
                    deco.task_finished(
                        step_name,
                        self.flow,
                        self.flow._graph,
                        self.flow._task_ok,
                        retry_count,
                        max_user_code_retries,
                    )

                # terminate side cars
                self.metadata.stop_heartbeat()

                # Task duration consists of the time taken to run the task as well as the time taken to
                # persist the task metadata and data to the datastore.
                duration = time.time() - start
                _system_logger.log_event(
                    level="info",
                    module="metaflow.task",
                    name="duration",
                    payload={**task_payload, "msg": str(duration)},
                )
