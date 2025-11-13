import re
import base64
from kfp import Client
from hashlib import sha1

from metaflow._vendor import click
from metaflow import current, decorators
from .kfp import KubeflowPipelines
from metaflow.package import MetaflowPackage
from metaflow.util import get_username, to_bytes, to_unicode
from metaflow.metaflow_config import FEAT_ALWAYS_UPLOAD_CODE_PACKAGE
from .exception import KubeflowPipelineException, NotSupportedException
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.plugins.aws.step_functions.production_token import (
    load_token,
    new_token,
    store_token,
)
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator


class IncorrectProductionToken(MetaflowException):
    headline = "Incorrect production token"


VALID_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,127}$")


def resolve_token(
    name, token_prefix, obj, authorize, given_token, generate_new_token, is_project
):
    # 1) retrieve the previous deployment, if one exists
    workflow = KubeflowPipelines.get_existing_deployment(name, obj.flow_datastore)
    if workflow is None:
        obj.echo(
            "It seems this is the first time you are deploying *%s* to "
            "Kubeflow Pipelines." % name
        )
        prev_token = None
    else:
        prev_user, prev_token = workflow

    # 2) authorize this deployment
    if prev_token is not None:
        if authorize is None:
            authorize = load_token(token_prefix)
        elif authorize.startswith("production:"):
            authorize = authorize[11:]

        # we allow the user who deployed the previous version to re-deploy,
        # even if they don't have the token
        if prev_user != get_username() and authorize != prev_token:
            obj.echo(
                "There is an existing version of *%s* on Kubeflow Pipelines which was "
                "deployed by the user *%s*." % (name, prev_user)
            )
            obj.echo(
                "To deploy a new version of this flow, you need to use the same "
                "production token that they used. "
            )
            obj.echo(
                "Please reach out to them to get the token. Once you have it, call "
                "this command:"
            )
            obj.echo("    kfp create --authorize MY_TOKEN", fg="green")
            obj.echo(
                'See "Organizing Results" at docs.metaflow.org for more information '
                "about production tokens."
            )
            raise IncorrectProductionToken(
                "Try again with the correct production token."
            )

    # 3) do we need a new token or should we use the existing token?
    if given_token:
        if is_project:
            # we rely on a known prefix for @project tokens, so we can't
            # allow the user to specify a custom token with an arbitrary prefix
            raise MetaflowException(
                "--new-token is not supported for @projects. Use --generate-new-token "
                "to create a new token."
            )
        if given_token.startswith("production:"):
            given_token = given_token[11:]
        token = given_token
        obj.echo("")
        obj.echo("Using the given token, *%s*." % token)
    elif prev_token is None or generate_new_token:
        token = new_token(token_prefix, prev_token)
        if token is None:
            if prev_token is None:
                raise MetaflowInternalError(
                    "We could not generate a new token. This is unexpected. "
                )
            else:
                raise MetaflowException(
                    "--generate-new-token option is not supported after using "
                    "--new-token. Use --new-token to make a new namespace."
                )
        obj.echo("")
        obj.echo("A new production token generated.")
        KubeflowPipelines.save_deployment_token(
            get_username(), name, token, obj.flow_datastore
        )
    else:
        token = prev_token

    obj.echo("")
    obj.echo("The namespace of this production flow is")
    obj.echo("    production:%s" % token, fg="green")
    obj.echo(
        "To analyze results of this production flow add this line in your notebooks:"
    )
    obj.echo('    namespace("production:%s")' % token, fg="green")
    obj.echo(
        "If you want to authorize other people to deploy new versions of this flow to "
        "Kubeflow Pipelines, they need to call"
    )
    obj.echo("    kfp create --authorize %s" % token, fg="green")
    obj.echo("when deploying this flow to Kubeflow Pipelines for the first time.")
    obj.echo(
        'See "Organizing Results" at https://docs.metaflow.org/ for more '
        "information about production tokens."
    )
    obj.echo("")
    store_token(token_prefix, token)

    return token


def resolve_pipeline_name(name):
    project = current.get("project_name")
    is_project = False

    if project:
        is_project = True
        if name:
            raise MetaflowException(
                "--name is not supported for @projects. " "Use --branch instead."
            )
        pipeline_name = current.project_flow_name
        if pipeline_name and not VALID_NAME.fullmatch(pipeline_name):
            raise MetaflowException(
                "Name '%s' contains invalid characters. Please construct a name using regex %s"
                % (pipeline_name, VALID_NAME.pattern)
            )
        project_branch = to_bytes(".".join((project, current.branch_name)))
        token_prefix = (
            "mfprj-%s"
            % to_unicode(base64.b32encode(sha1(project_branch).digest()))[:16]
        )
    else:
        if name and not VALID_NAME.fullmatch(name):
            raise MetaflowException(
                "Name '%s' contains invalid characters. Please construct a name using regex %s"
                % (name, VALID_NAME.pattern)
            )
        pipeline_name = name if name else current.flow_name
        token_prefix = pipeline_name
    return pipeline_name, token_prefix.lower(), is_project


# TODO: only check for linear flows, needs to be evolved later...
def _validate_linear_workflow(graph):
    def traverse_graph(node, state):
        if node.type not in ("start", "linear", "end"):
            raise NotSupportedException(
                "Step *%s* is a *%s* step. "
                "This type of graph is currently not supported with Kubeflow Pipelines."
                % (node.name, node.type)
            )

        if node.type == "end":
            return

        if len(node.out_funcs) != 1:
            raise NotSupportedException(
                "Step *%s* has %d successors. "
                "Linear flows must have exactly one successor per step."
                % (node.name, len(node.out_funcs))
            )

        traverse_graph(graph[node.out_funcs[0]], state)

    traverse_graph(graph["start"], {})


def _validate_workflow(flow, graph, flow_datastore, metadata):
    seen = set()
    for _, param in flow._get_parameters():
        # Throw an exception if the parameter is specified twice.
        norm = param.name.lower()
        if norm in seen:
            raise MetaflowException(
                "Parameter *%s* is specified twice. "
                "Note that parameter names are case-insensitive." % param.name
            )
        seen.add(norm)

        if "default" not in param.kwargs:
            raise MetaflowException(
                "Parameter *%s* does not have a default value. "
                "A default value is required for parameters when deploying flows on Kubeflow Pipelines."
                % param.name
            )

    for node in graph:
        if node.parallel_foreach:
            raise KubeflowPipelineException(
                "Deploying flows with @parallel decorator(s) "
                "to Kubeflow Pipelines is not supported currently."
            )
        if any([d.name == "batch" for d in node.decorators]):
            raise NotSupportedException(
                "Step *%s* is marked for execution on AWS Batch with Kubeflow Pipelines which isn't currently supported."
                % node.name
            )
        if any([d.name == "slurm" for d in node.decorators]):
            raise NotSupportedException(
                "Step *%s* is marked for execution on Slurm with Kubeflow Pipelines which isn't currently supported."
                % node.name
            )

    SUPPORTED_DATASTORES = ("azure", "s3", "gs")
    if flow_datastore.TYPE not in SUPPORTED_DATASTORES:
        raise KubeflowPipelineException(
            "Datastore type `%s` is not supported with `kfp create`. "
            "Please choose from datastore of type %s when calling `kfp create`"
            % (
                str(flow_datastore.TYPE),
                "or ".join(["`%s`" % x for x in SUPPORTED_DATASTORES]),
            )
        )

    # TODO: supports only linear transitions for now, needs to evolve further
    _validate_linear_workflow(graph)

    # TODO: handle the @schedule flow decorator?


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Kubeflow Pipelines.")
@click.option(
    "--name",
    default=None,
    type=str,
    help="Kubeflow Pipeline name. The flow name is used instead if this option is not "
    "specified",
)
@click.pass_obj
def kfp(obj, name=None):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    obj.pipeline_name, obj.token_prefix, obj.is_project = resolve_pipeline_name(name)


@kfp.command(help="Compile a new version of this flow to Kubeflow Pipeline.")
@click.option(
    "--authorize",
    default=None,
    help="Authorize using this production token. You need this "
    "when you are re-deploying an existing flow for the first "
    "time. The token is cached in METAFLOW_HOME, so you only "
    "need to specify this once.",
)
@click.option(
    "--generate-new-token",
    is_flag=True,
    help="Generate a new production token for this flow. "
    "This will move the production flow to a new namespace.",
)
@click.option(
    "--new-token",
    "given_token",
    default=None,
    help="Use the given production token for this flow. "
    "This will move the production flow to the given namespace.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Annotate all objects produced by Kubeflow Pipeline executions "
    "with the given tag. You can specify this option multiple "
    "times to attach multiple tags.",
)
@click.option(
    "--namespace",
    "user_namespace",
    default=None,
    help="Change the namespace from the default to the given namespace. "
    "See run --help for more information.",
)
@click.option(
    "--max-workers",
    default=100,
    show_default=True,
    help="Maximum number of parallel processes.",
)
@click.pass_obj
def create(
    obj,
    authorize=None,
    generate_new_token=False,
    given_token=None,
    tags=None,
    user_namespace=None,
    max_workers=None,
):
    kfp_client = Client(host="http://localhost:8081")

    # Validate if the workflow is correctly parsed.
    # _validate_workflow(
    #     obj.flow,
    #     obj.graph,
    #     obj.flow_datastore,
    #     obj.metadata,
    # )

    obj.echo("Compiling *%s* to Kubeflow Pipelines..." % obj.pipeline_name, bold=True)
    token = resolve_token(
        obj.pipeline_name,
        obj.token_prefix,
        obj,
        authorize,
        given_token,
        generate_new_token,
        obj.is_project,
    )

    flow = make_flow(
        obj,
        obj.pipeline_name,
        token,
        tags,
        user_namespace,
        max_workers,
        kfp_client,
    )

    flow.compile_and_upload()

    obj.echo(
        "Pipeline *{pipeline_name}* "
        "for flow *{name}* compiled to "
        "Kubeflow Pipelines successfully.\n".format(
            pipeline_name=obj.pipeline_name, name=current.flow_name
        ),
        bold=True,
    )


def make_flow(
    obj,
    pipeline_name,
    production_token,
    tags,
    namespace,
    max_workers,
    kfp_client,
):
    # Attach @kubernetes.
    decorators._attach_decorators(obj.flow, [KubernetesDecorator.name])
    decorators._init(obj.flow)

    decorators._init_step_decorators(
        obj.flow, obj.graph, obj.environment, obj.flow_datastore, obj.logger
    )
    obj.graph = obj.flow._graph
    # Save the code package in the flow datastore so that both user code and
    # metaflow package can be retrieved during workflow execution.
    obj.package = MetaflowPackage(
        obj.flow,
        obj.environment,
        obj.echo,
        suffixes=obj.package_suffixes,
        flow_datastore=obj.flow_datastore if FEAT_ALWAYS_UPLOAD_CODE_PACKAGE else None,
    )

    # This blocks until the package is created
    if FEAT_ALWAYS_UPLOAD_CODE_PACKAGE:
        package_url = obj.package.package_url()
        package_sha = obj.package.package_sha()
    else:
        package_url, package_sha = obj.flow_datastore.save_data(
            [obj.package.blob], len_hint=1
        )[0]

    return KubeflowPipelines(
        kfp_client,
        pipeline_name,
        obj.graph,
        obj.flow,
        obj.package.package_metadata,
        package_sha,
        package_url,
        obj.metadata,
        obj.flow_datastore,
        obj.environment,
        obj.event_logger,
        obj.monitor,
        production_token,
        tags=tags,
        namespace=namespace,
        username=get_username(),
        max_workers=max_workers,
        description=obj.flow.__doc__,
    )
