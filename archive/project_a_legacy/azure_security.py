"""Minimal Azure security compatibility stubs."""


class AzureSecurityManager:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def get_security_summary(self):
        return {"overall_status": "compliant", "issues": []}


def setup_azure_security(config):
    return {"status": "configured", "config": bool(config)}

