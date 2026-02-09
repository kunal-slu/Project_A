"""Minimal monitoring compatibility stubs."""


class PipelineMonitor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def emit(self, *_args, **_kwargs):
        return None

