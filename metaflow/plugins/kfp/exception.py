from metaflow.exception import MetaflowException


class KubeflowPipelineException(MetaflowException):
    headline = "Kubeflow Pipeline Exception"

    def __init__(self, msg):
        super().__init__(msg)


class NotSupportedException(MetaflowException):
    headline = "Not yet supported with Kubeflow Pipelines"
