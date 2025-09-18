"""
Azure Security & Compliance Module

This module provides comprehensive Azure security integration for:
- Azure AD RBAC and access control
- Managed Identities for secure authentication
- Key Vault integration for secret management
- VNet injection and Private Endpoints
- Compliance controls and audit logging
- Data encryption and security policies
"""

import logging
import os
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from enum import Enum

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.network import NetworkManagementClient
try:
    from azure.mgmt.databricks import AzureDatabricksManagementClient as DatabricksClient
except ImportError:
    try:
        from azure.mgmt.databricks import DatabricksClient
    except ImportError:
        DatabricksClient = None
from azure.mgmt.storage import StorageManagementClient

logger = logging.getLogger(__name__)


class SecurityLevel(Enum):
    """Security level enumeration."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class ComplianceStandard(Enum):
    """Compliance standard enumeration."""
    GDPR = "gdpr"
    HIPAA = "hipaa"
    SOX = "sox"
    PCI_DSS = "pci_dss"
    ISO_27001 = "iso_27001"


class AzureSecurityManager:
    """
    Manages Azure security, compliance, and access control.
    """

    def __init__(self, subscription_id: str, resource_group: str, workspace_name: str = None):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.workspace_name = workspace_name

        # Initialize Azure clients
        self.credential = self._get_credential()
        self.auth_client = AuthorizationManagementClient(self.credential, subscription_id)
        self.network_client = NetworkManagementClient(self.credential, subscription_id)
        self.databricks_client = DatabricksClient(self.credential, subscription_id) if DatabricksClient else None
        self.storage_client = StorageManagementClient(self.credential, subscription_id)

        # Key Vault client (will be initialized when needed)
        self._key_vault_client = None

    def _get_credential(self):
        """Get Azure credential using Managed Identity or DefaultAzureCredential."""
        try:
            # Try Managed Identity first (for Databricks)
            return ManagedIdentityCredential()
        except Exception:
            try:
                # Fall back to DefaultAzureCredential (for local development)
                return DefaultAzureCredential()
            except Exception as e:
                logger.warning(f"Could not initialize Azure credentials: {str(e)}")
                return None

    def _get_key_vault_client(self, key_vault_name: str):
        """Get Key Vault client for secret management."""
        if not self._key_vault_client:
            try:
                key_vault_url = f"https://{key_vault_name}.vault.azure.net/"
                self._key_vault_client = SecretClient(vault_url=key_vault_url, credential=self.credential)
            except Exception as e:
                logger.error(f"Failed to initialize Key Vault client: {str(e)}")
                return None
        return self._key_vault_client

    def create_managed_identity(self, identity_name: str, description: str = "") -> Dict[str, Any]:
        """
        Create a User-Assigned Managed Identity.

        Args:
            identity_name: Name of the managed identity
            description: Description of the identity

        Returns:
            dict: Identity creation result
        """
        try:
            from azure.mgmt.msi import ManagedServiceIdentityClient

            msi_client = ManagedServiceIdentityClient(self.credential, self.subscription_id)

            identity_params = {
                "location": "eastus",  # Should be configurable
                "tags": {
                    "purpose": "data-engineering",
                    "environment": "production",
                    "created_by": "azure-security-manager"
                }
            }

            if description:
                identity_params["tags"]["description"] = description

            poller = msi_client.user_assigned_identities.create_or_update(
                resource_group_name=self.resource_group,
                resource_name=identity_name,
                parameters=identity_params
            )

            identity = poller.result()

            logger.info(f"Created managed identity: {identity_name}")
            return {
                "id": identity.id,
                "name": identity.name,
                "principal_id": identity.principal_id,
                "client_id": identity.client_id,
                "tenant_id": identity.tenant_id,
                "location": identity.location
            }

        except Exception as e:
            logger.error(f"Failed to create managed identity {identity_name}: {str(e)}")
            return {"error": str(e)}

    def assign_rbac_role(self, principal_id: str, role_definition_id: str,
                        scope: str = None) -> bool:
        """
        Assign RBAC role to a principal.

        Args:
            principal_id: Principal ID (user, group, or managed identity)
            role_definition_id: Role definition ID
            scope: Scope for the role assignment (defaults to resource group)

        Returns:
            bool: True if successful
        """
        try:
            if not scope:
                scope = f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}"

            # Create role assignment
            role_assignment_name = f"role-{int(time.time())}"

            role_assignment = self.auth_client.role_assignments.create(
                scope=scope,
                role_assignment_name=role_assignment_name,
                parameters={
                    "role_definition_id": role_definition_id,
                    "principal_id": principal_id,
                    "principal_type": "ServicePrincipal"  # Adjust based on principal type
                }
            )

            logger.info(f"Assigned role {role_definition_id} to {principal_id} at scope {scope}")
            return True

        except Exception as e:
            logger.error(f"Failed to assign RBAC role: {str(e)}")
            return False

    def get_secret_from_key_vault(self, key_vault_name: str, secret_name: str) -> Optional[str]:
        """
        Retrieve a secret from Azure Key Vault.

        Args:
            key_vault_name: Name of the Key Vault
            secret_name: Name of the secret

        Returns:
            str: Secret value or None if failed
        """
        try:
            client = self._get_key_vault_client(key_vault_name)
            if not client:
                return None

            secret = client.get_secret(secret_name)
            logger.info(f"Retrieved secret {secret_name} from Key Vault {key_vault_name}")
            return secret.value

        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_name} from {key_vault_name}: {str(e)}")
            return None

    def set_secret_in_key_vault(self, key_vault_name: str, secret_name: str,
                               secret_value: str, content_type: str = "text/plain") -> bool:
        """
        Store a secret in Azure Key Vault.

        Args:
            key_vault_name: Name of the Key Vault
            secret_name: Name of the secret
            secret_value: Value of the secret
            content_type: Content type of the secret

        Returns:
            bool: True if successful
        """
        try:
            client = self._get_key_vault_client(key_vault_name)
            if not client:
                return False

            client.set_secret(secret_name, secret_value, content_type=content_type)
            logger.info(f"Stored secret {secret_name} in Key Vault {key_vault_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to store secret {secret_name} in {key_vault_name}: {str(e)}")
            return False

    def create_private_endpoint(self, resource_name: str, resource_type: str,
                               subnet_id: str, private_service_connection_name: str) -> Dict[str, Any]:
        """
        Create a Private Endpoint for secure resource access.

        Args:
            resource_name: Name of the resource
            resource_type: Type of the resource (e.g., Microsoft.Databricks/workspaces)
            subnet_id: ID of the subnet for the private endpoint
            private_service_connection_name: Name for the private service connection

        Returns:
            dict: Private endpoint creation result
        """
        try:
            # Create private endpoint
            private_endpoint_params = {
                "location": "eastus",  # Should be configurable
                "private_link_service_connections": [{
                    "name": private_service_connection_name,
                    "private_link_service_id": f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/{resource_type}/{resource_name}",
                    "group_ids": ["databricks_ui_api"],
                    "request_message": "Request for private endpoint access"
                }],
                "subnet": {
                    "id": subnet_id
                }
            }

            poller = self.network_client.private_endpoints.begin_create_or_update(
                resource_group_name=self.resource_group,
                private_endpoint_name=f"{resource_name}-pe",
                parameters=private_endpoint_params
            )

            private_endpoint = poller.result()

            logger.info(f"Created private endpoint for {resource_name}")
            return {
                "id": private_endpoint.id,
                "name": private_endpoint.name,
                "location": private_endpoint.location,
                "private_link_service_connections": private_endpoint.private_link_service_connections
            }

        except Exception as e:
            logger.error(f"Failed to create private endpoint for {resource_name}: {str(e)}")
            return {"error": str(e)}

    def enable_vnet_injection(self, workspace_name: str, vnet_id: str,
                             subnet_id: str, public_ip_enabled: bool = False) -> bool:
        """
        Enable VNet injection for Databricks workspace.

        Args:
            workspace_name: Name of the Databricks workspace
            vnet_id: ID of the VNet
            subnet_id: ID of the subnet
            public_ip_enabled: Whether to enable public IP

        Returns:
            bool: True if successful
        """
        try:
            # Get current workspace
            workspace = self.databricks_client.workspaces.get(
                resource_group_name=self.resource_group,
                workspace_name=workspace_name
            )

            # Update workspace with VNet injection
            workspace.parameters.vnet_id = vnet_id
            workspace.parameters.subnet_id = subnet_id
            workspace.parameters.public_ip_enabled = public_ip_enabled

            poller = self.databricks_client.workspaces.begin_create_or_update(
                resource_group_name=self.resource_group,
                workspace_name=workspace_name,
                parameters=workspace
            )

            updated_workspace = poller.result()

            logger.info(f"Enabled VNet injection for workspace {workspace_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to enable VNet injection for {workspace_name}: {str(e)}")
            return False

    def set_storage_encryption(self, storage_account_name: str,
                              encryption_algorithm: str = "AES256") -> bool:
        """
        Configure storage account encryption.

        Args:
            storage_account_name: Name of the storage account
            encryption_algorithm: Encryption algorithm to use

        Returns:
            bool: True if successful
        """
        try:
            # Get current storage account
            storage_account = self.storage_client.storage_accounts.get_properties(
                resource_group_name=self.resource_group,
                account_name=storage_account_name
            )

            # Update encryption settings
            storage_account.encryption.services.blob.enabled = True
            storage_account.encryption.services.file.enabled = True
            storage_account.encryption.services.table.enabled = True
            storage_account.encryption.services.queue.enabled = True

            poller = self.storage_client.storage_accounts.begin_create_or_update(
                resource_group_name=self.resource_group,
                account_name=storage_account_name,
                parameters=storage_account
            )

            updated_account = poller.result()

            logger.info(f"Configured encryption for storage account {storage_account_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to configure encryption for {storage_account_name}: {str(e)}")
            return False

    def create_compliance_policy(self, policy_name: str, compliance_standard: ComplianceStandard,
                                policy_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create a compliance policy for the specified standard.

        Args:
            policy_name: Name of the policy
            compliance_standard: Compliance standard to follow
            policy_rules: List of policy rules

        Returns:
            dict: Policy creation result
        """
        try:
            # Define compliance mappings
            compliance_mappings = {
                ComplianceStandard.GDPR: {
                    "data_retention": "max_retention_days",
                    "data_processing_basis": "legitimate_interest",
                    "data_subject_rights": ["access", "rectification", "erasure", "portability"],
                    "data_protection_officer": "required"
                },
                ComplianceStandard.HIPAA: {
                    "phi_protection": "required",
                    "access_controls": "role_based",
                    "audit_logging": "required",
                    "encryption": "at_rest_and_in_transit"
                },
                ComplianceStandard.SOX: {
                    "financial_data_integrity": "required",
                    "change_management": "required",
                    "access_reviews": "quarterly",
                    "audit_trail": "permanent"
                }
            }

            # Create policy configuration
            policy_config = {
                "name": policy_name,
                "compliance_standard": compliance_standard.value,
                "rules": policy_rules,
                "mappings": compliance_mappings.get(compliance_standard, {}),
                "created_at": datetime.now().isoformat(),
                "enabled": True,
                "review_frequency": "monthly",
                "next_review": (datetime.now() + timedelta(days=30)).isoformat()
            }

            # Store policy (in production, this would go to Azure Policy or similar)
            logger.info(f"Created compliance policy {policy_name} for {compliance_standard.value}")
            return policy_config

        except Exception as e:
            logger.error(f"Failed to create compliance policy {policy_name}: {str(e)}")
            return {"error": str(e)}

    def audit_data_access(self, user_id: str, resource_name: str, action: str,
                         data_classification: SecurityLevel, compliance_standard: ComplianceStandard = None) -> bool:
        """
        Audit data access for compliance purposes.

        Args:
            user_id: ID of the user accessing data
            resource_name: Name of the resource being accessed
            action: Action performed (SELECT, INSERT, UPDATE, DELETE)
            data_classification: Security level of the data
            compliance_standard: Compliance standard being enforced

        Returns:
            bool: True if successful
        """
        try:
            # Create audit record
            audit_record = {
                "timestamp": datetime.now().isoformat(),
                "user_id": user_id,
                "resource_name": resource_name,
                "action": action,
                "data_classification": data_classification.value,
                "compliance_standard": compliance_standard.value if compliance_standard else None,
                "workspace_id": os.environ.get("DATABRICKS_WORKSPACE_ID", "unknown"),
                "cluster_id": os.environ.get("DATABRICKS_CLUSTER_ID", "unknown"),
                "ip_address": os.environ.get("CLIENT_IP_ADDRESS", "unknown"),
                "session_id": os.environ.get("SESSION_ID", "unknown")
            }

            # Store audit record (in production, this would go to Azure Monitor or Log Analytics)
            logger.info(f"Audit: {user_id} performed {action} on {resource_name} ({data_classification.value})")

            # For demo purposes, save to local file
            audit_file = "data/audit/azure_access_logs.json"
            os.makedirs(os.path.dirname(audit_file), exist_ok=True)

            with open(audit_file, "a") as f:
                f.write(json.dumps(audit_record) + "\n")

            return True

        except Exception as e:
            logger.error(f"Failed to audit data access: {str(e)}")
            return False

    def get_security_summary(self) -> Dict[str, Any]:
        """
        Get summary of security and compliance status.

        Returns:
            dict: Security summary
        """
        try:
            summary = {
                "managed_identities": 0,
                "rbac_assignments": 0,
                "private_endpoints": 0,
                "vnet_injection_enabled": False,
                "storage_encryption_enabled": False,
                "compliance_policies": 0,
                "key_vaults_configured": 0,
                "last_security_audit": datetime.now().isoformat(),
                "security_score": 0
            }

            # Calculate security score based on implemented controls
            security_controls = [
                "managed_identities",
                "rbac_assignments",
                "private_endpoints",
                "vnet_injection_enabled",
                "storage_encryption_enabled",
                "compliance_policies",
                "key_vaults_configured"
            ]

            implemented_controls = sum(1 for control in security_controls if summary.get(control, 0) > 0)
            summary["security_score"] = int((implemented_controls / len(security_controls)) * 100)

            logger.info(f"Security summary: Score {summary['security_score']}/100")
            return summary

        except Exception as e:
            logger.error(f"Failed to get security summary: {str(e)}")
            return {"error": str(e)}


def setup_azure_security(config: Dict[str, Any]) -> AzureSecurityManager:
    """
    Setup Azure security and compliance for the project.

    Args:
        config: Configuration dictionary

    Returns:
        AzureSecurityManager: Configured Azure security manager
    """
    try:
        # Get Azure configuration
        azure_config = config.get("azure", {})
        subscription_id = azure_config.get("subscription_id")
        resource_group = azure_config.get("resource_group")
        workspace_name = azure_config.get("workspace_name")

        if not subscription_id or not resource_group:
            raise ValueError("Azure subscription_id and resource_group are required")

        # Initialize security manager
        security_manager = AzureSecurityManager(subscription_id, resource_group, workspace_name)

        # Setup compliance policies
        compliance_standards = azure_config.get("compliance_standards", [])
        for standard in compliance_standards:
            if isinstance(standard, str):
                try:
                    compliance_standard = ComplianceStandard(standard.lower())
                    security_manager.create_compliance_policy(
                        f"policy_{standard.lower()}",
                        compliance_standard,
                        []  # Empty rules for demo
                    )
                except ValueError:
                    logger.warning(f"Unknown compliance standard: {standard}")

        logger.info("Azure security setup completed")
        return security_manager

    except Exception as e:
        logger.error(f"Failed to setup Azure security: {str(e)}")
        raise


def validate_security_compliance(security_manager: AzureSecurityManager,
                               required_standards: List[ComplianceStandard]) -> Dict[str, Any]:
    """
    Validate security compliance against required standards.

    Args:
        security_manager: Azure security manager
        required_standards: List of required compliance standards

    Returns:
        dict: Compliance validation results
    """
    try:
        validation_results = {
            "overall_compliance": True,
            "standards": {},
            "missing_controls": [],
            "recommendations": [],
            "validated_at": datetime.now().isoformat()
        }

        for standard in required_standards:
            standard_name = standard.value
            validation_results["standards"][standard_name] = {
                "compliant": False,
                "controls_implemented": 0,
                "total_controls": 0,
                "missing_controls": []
            }

            # Define required controls for each standard
            required_controls = {
                ComplianceStandard.GDPR: [
                    "data_encryption", "access_controls", "audit_logging",
                    "data_retention_policies", "data_subject_rights"
                ],
                ComplianceStandard.HIPAA: [
                    "phi_protection", "access_controls", "audit_logging",
                    "encryption", "backup_recovery"
                ],
                ComplianceStandard.SOX: [
                    "financial_data_integrity", "change_management",
                    "access_reviews", "audit_trail"
                ]
            }

            controls = required_controls.get(standard, [])
            validation_results["standards"][standard_name]["total_controls"] = len(controls)

            # Check which controls are implemented (simplified check)
            implemented_controls = []
            for control in controls:
                # This is a simplified check - in production, you'd verify actual implementation
                if control in ["data_encryption", "access_controls", "audit_logging"]:
                    implemented_controls.append(control)

            validation_results["standards"][standard_name]["controls_implemented"] = len(implemented_controls)
            validation_results["standards"][standard_name]["compliant"] = (
                len(implemented_controls) == len(controls)
            )

            if not validation_results["standards"][standard_name]["compliant"]:
                validation_results["overall_compliance"] = False
                missing = [c for c in controls if c not in implemented_controls]
                validation_results["standards"][standard_name]["missing_controls"] = missing
                validation_results["missing_controls"].extend(missing)

        # Generate recommendations
        if validation_results["missing_controls"]:
            validation_results["recommendations"] = [
                f"Implement {control} controls" for control in set(validation_results["missing_controls"])
            ]

        logger.info(f"Security compliance validation completed: {validation_results['overall_compliance']}")
        return validation_results

    except Exception as e:
        logger.error(f"Failed to validate security compliance: {str(e)}")
        return {"error": str(e)}
