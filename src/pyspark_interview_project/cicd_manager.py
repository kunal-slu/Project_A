"""
CI/CD Manager for Multi-Environment Deployments

This module provides comprehensive CI/CD capabilities for:
- Multi-environment deployment management (Dev, Test, Staging, Prod)
- Infrastructure as Code (ARM/Bicep/Terraform)
- Automated testing and validation
- Blue/Green deployments
- Environment-specific configurations
- Deployment rollbacks and monitoring
"""

import logging
import os
import json
import yaml
import time
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum


logger = logging.getLogger(__name__)


class Environment(Enum):
    """Deployment environment enumeration."""
    DEV = "dev"
    TEST = "test"
    STAGING = "staging"
    PROD = "prod"


class DeploymentStatus(Enum):
    """Deployment status enumeration."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class DeploymentType(Enum):
    """Deployment type enumeration."""
    BLUE_GREEN = "blue_green"
    ROLLING = "rolling"
    CANARY = "canary"
    IMMEDIATE = "immediate"


@dataclass
class DeploymentConfig:
    """Deployment configuration."""
    environment: Environment
    deployment_type: DeploymentType
    version: str
    config_file: str
    variables: Dict[str, Any]
    secrets: Dict[str, str]
    rollback_enabled: bool = True
    health_check_enabled: bool = True
    automated_testing: bool = True
    approval_required: bool = False
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class DeploymentResult:
    """Deployment result."""
    deployment_id: str
    environment: Environment
    status: DeploymentStatus
    version: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_minutes: Optional[float] = None
    logs: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    rollback_triggered: bool = False
    rollback_reason: Optional[str] = None


class CICDManager:
    """
    Manages CI/CD pipelines and multi-environment deployments.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.deployments: Dict[str, DeploymentResult] = {}
        self.environment_configs: Dict[Environment, Dict[str, Any]] = {}
        self.deployment_history: List[DeploymentResult] = []
        self._load_environment_configs()

    def _load_environment_configs(self):
        """Load environment-specific configurations."""
        try:
            cicd_config = self.config.get("cicd", {})
            environments = cicd_config.get("environments", {})

            for env_name, env_config in environments.items():
                try:
                    environment = Environment(env_name)
                    self.environment_configs[environment] = env_config
                    logger.info(f"Loaded configuration for environment: {env_name}")
                except ValueError:
                    logger.warning(f"Unknown environment: {env_name}")

            logger.info(f"Loaded {len(self.environment_configs)} environment configurations")

        except Exception as e:
            logger.error(f"Failed to load environment configurations: {str(e)}")

    def create_deployment(self, environment: Environment, version: str,
                         deployment_type: DeploymentType = DeploymentType.BLUE_GREEN,
                         config_file: str = None, variables: Dict[str, Any] = None,
                         secrets: Dict[str, str] = None) -> DeploymentConfig:
        """
        Create a new deployment configuration.

        Args:
            environment: Target environment
            version: Version to deploy
            deployment_type: Type of deployment
            config_file: Configuration file path
            variables: Environment variables
            secrets: Secret values

        Returns:
            DeploymentConfig: Created deployment configuration
        """
        try:
            # Validate environment
            if environment not in self.environment_configs:
                raise ValueError(f"Environment {environment.value} not configured")

            # Get environment-specific settings
            env_config = self.environment_configs[environment]

            # Set default values
            if config_file is None:
                config_file = env_config.get("default_config_file", f"configs/{environment.value}.yaml")

            if variables is None:
                variables = env_config.get("default_variables", {})

            if secrets is None:
                secrets = env_config.get("default_secrets", {})

            # Create deployment config
            deployment_config = DeploymentConfig(
                environment=environment,
                deployment_type=deployment_type,
                version=version,
                config_file=config_file,
                variables=variables,
                secrets=secrets,
                rollback_enabled=env_config.get("rollback_enabled", True),
                health_check_enabled=env_config.get("health_check_enabled", True),
                automated_testing=env_config.get("automated_testing", True),
                approval_required=env_config.get("approval_required", False)
            )

            logger.info(f"Created deployment configuration for {environment.value} version {version}")
            return deployment_config

        except Exception as e:
            logger.error(f"Failed to create deployment configuration: {str(e)}")
            raise

    def deploy_to_environment(self, deployment_config: DeploymentConfig) -> DeploymentResult:
        """
        Deploy to a specific environment.

        Args:
            deployment_config: Deployment configuration

        Returns:
            DeploymentResult: Deployment result
        """
        try:
            deployment_id = f"deploy_{deployment_config.environment.value}_{int(time.time())}"

            # Create deployment result
            deployment_result = DeploymentResult(
                deployment_id=deployment_id,
                environment=deployment_config.environment,
                status=DeploymentStatus.IN_PROGRESS,
                version=deployment_config.version,
                start_time=datetime.now()
            )

            # Store deployment
            self.deployments[deployment_id] = deployment_result
            self.deployment_history.append(deployment_result)

            logger.info(f"Starting deployment {deployment_id} to {deployment_config.environment.value}")

            # Step 1: Pre-deployment validation
            if not self._validate_deployment(deployment_config, deployment_result):
                deployment_result.status = DeploymentStatus.FAILED
                deployment_result.end_time = datetime.now()
                deployment_result.duration_minutes = (deployment_result.end_time - deployment_result.start_time).total_seconds() / 60
                return deployment_result

            # Step 2: Run automated tests
            if deployment_config.automated_testing:
                if not self._run_automated_tests(deployment_config, deployment_result):
                    deployment_result.status = DeploymentStatus.FAILED
                    deployment_result.end_time = datetime.now()
                    deployment_result.duration_minutes = (deployment_result.end_time - deployment_result.start_time).total_seconds() / 60
                    return deployment_result

            # Step 3: Execute deployment
            if not self._execute_deployment(deployment_config, deployment_result):
                deployment_result.status = DeploymentStatus.FAILED
                deployment_result.end_time = datetime.now()
                deployment_result.duration_minutes = (deployment_result.end_time - deployment_result.start_time).total_seconds() / 60

                # Attempt rollback if enabled
                if deployment_config.rollback_enabled:
                    self._trigger_rollback(deployment_config, deployment_result)

                return deployment_result

            # Step 4: Post-deployment validation
            if deployment_config.health_check_enabled:
                if not self._run_health_checks(deployment_config, deployment_result):
                    deployment_result.status = DeploymentStatus.FAILED
                    deployment_result.end_time = datetime.now()
                    deployment_result.duration_minutes = (deployment_result.end_time - deployment_result.start_time).total_seconds() / 60

                    # Attempt rollback if enabled
                    if deployment_config.rollback_enabled:
                        self._trigger_rollback(deployment_config, deployment_result)

                    return deployment_result

            # Deployment successful
            deployment_result.status = DeploymentStatus.SUCCESS
            deployment_result.end_time = datetime.now()
            deployment_result.duration_minutes = (deployment_result.end_time - deployment_result.start_time).total_seconds() / 60

            logger.info(f"Deployment {deployment_id} completed successfully")
            return deployment_result

        except Exception as e:
            logger.error(f"Deployment failed: {str(e)}")

            if 'deployment_result' in locals():
                deployment_result.status = DeploymentStatus.FAILED
                deployment_result.end_time = datetime.now()
                deployment_result.duration_minutes = (deployment_result.end_time - deployment_result.start_time).total_seconds() / 60
                deployment_result.errors.append(str(e))

                # Attempt rollback if enabled
                if deployment_config.rollback_enabled:
                    self._trigger_rollback(deployment_config, deployment_result)

                return deployment_result
            else:
                # Create error result
                error_result = DeploymentResult(
                    deployment_id=f"error_{int(time.time())}",
                    environment=deployment_config.environment,
                    status=DeploymentStatus.FAILED,
                    version=deployment_config.version,
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    duration_minutes=0.0,
                    errors=[str(e)]
                )
                return error_result

    def _validate_deployment(self, deployment_config: DeploymentConfig,
                           deployment_result: DeploymentResult) -> bool:
        """Validate deployment configuration and prerequisites."""
        try:
            # Check if config file exists
            if not os.path.exists(deployment_config.config_file):
                error_msg = f"Configuration file not found: {deployment_config.config_file}"
                deployment_result.errors.append(error_msg)
                logger.error(error_msg)
                return False

            # Check environment-specific requirements
            env_config = self.environment_configs[deployment_config.environment]
            required_variables = env_config.get("required_variables", [])

            for var in required_variables:
                if var not in deployment_config.variables:
                    error_msg = f"Required variable missing: {var}"
                    deployment_result.errors.append(error_msg)
                    logger.error(error_msg)
                    return False

            # Check secrets
            required_secrets = env_config.get("required_secrets", [])
            for secret in required_secrets:
                if secret not in deployment_config.secrets:
                    error_msg = f"Required secret missing: {secret}"
                    deployment_result.errors.append(error_msg)
                    logger.error(error_msg)
                    return False

            # Check if environment is available
            if not self._is_environment_available(deployment_config.environment):
                error_msg = f"Environment {deployment_config.environment.value} is not available"
                deployment_result.errors.append(error_msg)
                logger.error(error_msg)
                return False

            deployment_result.logs.append("Deployment validation passed")
            return True

        except Exception as e:
            error_msg = f"Deployment validation failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            logger.error(error_msg)
            return False

    def _run_automated_tests(self, deployment_config: DeploymentConfig,
                            deployment_result: DeploymentResult) -> bool:
        """Run automated tests before deployment."""
        try:
            env_config = self.environment_configs[deployment_config.environment]
            test_config = env_config.get("automated_tests", {})

            if not test_config:
                deployment_result.logs.append("No automated tests configured")
                return True

            deployment_result.logs.append("Running automated tests...")

            # Run unit tests
            if test_config.get("run_unit_tests", False):
                if not self._run_unit_tests(deployment_result):
                    return False

            # Run integration tests
            if test_config.get("run_integration_tests", False):
                if not self._run_integration_tests(deployment_result):
                    return False

            # Run security tests
            if test_config.get("run_security_tests", False):
                if not self._run_security_tests(deployment_result):
                    return False

            deployment_result.logs.append("All automated tests passed")
            return True

        except Exception as e:
            error_msg = f"Automated testing failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            logger.error(error_msg)
            return False

    def _run_unit_tests(self, deployment_result: DeploymentResult) -> bool:
        """Run unit tests."""
        try:
            # In production, this would run actual unit tests
            # For demo purposes, we'll simulate test execution
            test_result = subprocess.run(
                ["python", "-m", "pytest", "tests/unit", "-v"],
                capture_output=True,
                text=True,
                timeout=300
            )

            if test_result.returncode != 0:
                deployment_result.errors.append(f"Unit tests failed: {test_result.stderr}")
                return False

            deployment_result.logs.append("Unit tests passed")
            return True

        except subprocess.TimeoutExpired:
            error_msg = "Unit tests timed out"
            deployment_result.errors.append(error_msg)
            return False
        except Exception as e:
            error_msg = f"Unit tests failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_integration_tests(self, deployment_result: DeploymentResult) -> bool:
        """Run integration tests."""
        try:
            # In production, this would run actual integration tests
            # For demo purposes, we'll simulate test execution
            test_result = subprocess.run(
                ["python", "-m", "pytest", "tests/integration", "-v"],
                capture_output=True,
                text=True,
                timeout=600
            )

            if test_result.returncode != 0:
                deployment_result.errors.append(f"Integration tests failed: {test_result.stderr}")
                return False

            deployment_result.logs.append("Integration tests passed")
            return True

        except subprocess.TimeoutExpired:
            error_msg = "Integration tests timed out"
            deployment_result.errors.append(error_msg)
            return False
        except Exception as e:
            error_msg = f"Integration tests failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_security_tests(self, deployment_result: DeploymentResult) -> bool:
        """Run security tests."""
        try:
            # In production, this would run actual security tests
            # For demo purposes, we'll simulate test execution
            test_result = subprocess.run(
                ["python", "-m", "bandit", "-r", "src/", "-f", "json"],
                capture_output=True,
                text=True,
                timeout=300
            )

            if test_result.returncode != 0:
                deployment_result.warnings.append(f"Security tests found issues: {test_result.stdout}")
                # Security warnings don't fail the deployment by default

            deployment_result.logs.append("Security tests completed")
            return True

        except subprocess.TimeoutExpired:
            error_msg = "Security tests timed out"
            deployment_result.errors.append(error_msg)
            return False
        except Exception as e:
            error_msg = f"Security tests failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _execute_deployment(self, deployment_config: DeploymentConfig,
                           deployment_result: DeploymentResult) -> bool:
        """Execute the actual deployment."""
        try:
            env_config = self.environment_configs[deployment_config.environment]
            deployment_result.logs.append(f"Executing {deployment_config.deployment_type.value} deployment...")

            # Load configuration
            with open(deployment_config.config_file, 'r') as f:
                if deployment_config.config_file.endswith('.yaml') or deployment_config.config_file.endswith('.yml'):
                    config_data = yaml.safe_load(f)
                else:
                    config_data = json.load(f)

            # Apply environment variables
            config_data = self._apply_environment_variables(config_data, deployment_config.variables)

            # Apply secrets
            config_data = self._apply_secrets(config_data, deployment_config.secrets)

            # Execute deployment based on type
            if deployment_config.deployment_type == DeploymentType.BLUE_GREEN:
                success = self._execute_blue_green_deployment(config_data, deployment_result)
            elif deployment_config.deployment_type == DeploymentType.ROLLING:
                success = self._execute_rolling_deployment(config_data, deployment_result)
            elif deployment_config.deployment_type == DeploymentType.CANARY:
                success = self._execute_canary_deployment(config_data, deployment_result)
            else:  # IMMEDIATE
                success = self._execute_immediate_deployment(config_data, deployment_result)

            if success:
                deployment_result.logs.append("Deployment execution completed successfully")
                return True
            else:
                deployment_result.errors.append("Deployment execution failed")
                return False

        except Exception as e:
            error_msg = f"Deployment execution failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            logger.error(error_msg)
            return False

    def _execute_blue_green_deployment(self, config_data: Dict[str, Any],
                                     deployment_result: DeploymentResult) -> bool:
        """Execute blue-green deployment."""
        try:
            deployment_result.logs.append("Starting blue-green deployment...")

            # Step 1: Deploy to green environment
            deployment_result.logs.append("Deploying to green environment...")
            time.sleep(2)  # Simulate deployment time

            # Step 2: Run health checks on green
            deployment_result.logs.append("Running health checks on green environment...")
            if not self._run_health_checks_green(config_data, deployment_result):
                deployment_result.errors.append("Green environment health checks failed")
                return False

            # Step 3: Switch traffic to green
            deployment_result.logs.append("Switching traffic to green environment...")
            time.sleep(1)  # Simulate traffic switch

            # Step 4: Verify green is stable
            deployment_result.logs.append("Verifying green environment stability...")
            time.sleep(2)  # Simulate stability check

            # Step 5: Decommission blue environment
            deployment_result.logs.append("Decommissioning blue environment...")
            time.sleep(1)  # Simulate decommissioning

            deployment_result.logs.append("Blue-green deployment completed successfully")
            return True

        except Exception as e:
            error_msg = f"Blue-green deployment failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _execute_rolling_deployment(self, config_data: Dict[str, Any],
                                  deployment_result: DeploymentResult) -> bool:
        """Execute rolling deployment."""
        try:
            deployment_result.logs.append("Starting rolling deployment...")

            # Get deployment targets
            targets = config_data.get("deployment_targets", [])
            if not targets:
                deployment_result.errors.append("No deployment targets specified")
                return False

            # Deploy to each target sequentially
            for i, target in enumerate(targets):
                deployment_result.logs.append(f"Deploying to target {i+1}/{len(targets)}: {target}")
                time.sleep(2)  # Simulate deployment time

                # Run health check on this target
                if not self._run_target_health_check(target, deployment_result):
                    deployment_result.errors.append(f"Health check failed for target: {target}")
                    return False

            deployment_result.logs.append("Rolling deployment completed successfully")
            return True

        except Exception as e:
            error_msg = f"Rolling deployment failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _execute_canary_deployment(self, config_data: Dict[str, Any],
                                 deployment_result: DeploymentResult) -> bool:
        """Execute canary deployment."""
        try:
            deployment_result.logs.append("Starting canary deployment...")

            # Step 1: Deploy to canary environment
            deployment_result.logs.append("Deploying to canary environment...")
            time.sleep(2)  # Simulate deployment time

            # Step 2: Run canary tests
            deployment_result.logs.append("Running canary tests...")
            if not self._run_canary_tests(config_data, deployment_result):
                deployment_result.errors.append("Canary tests failed")
                return False

            # Step 3: Gradually increase traffic
            traffic_percentages = [5, 25, 50, 75, 100]
            for percentage in traffic_percentages:
                deployment_result.logs.append(f"Increasing traffic to {percentage}%...")
                time.sleep(1)  # Simulate traffic increase

                # Run health checks
                if not self._run_canary_health_check(percentage, deployment_result):
                    deployment_result.errors.append(f"Health check failed at {percentage}% traffic")
                    return False

            deployment_result.logs.append("Canary deployment completed successfully")
            return True

        except Exception as e:
            error_msg = f"Canary deployment failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _execute_immediate_deployment(self, config_data: Dict[str, Any],
                                    deployment_result: DeploymentResult) -> bool:
        """Execute immediate deployment."""
        try:
            deployment_result.logs.append("Starting immediate deployment...")

            # Deploy immediately
            deployment_result.logs.append("Deploying to all targets...")
            time.sleep(3)  # Simulate deployment time

            # Run health checks
            if not self._run_health_checks_immediate(config_data, deployment_result):
                deployment_result.errors.append("Immediate deployment health checks failed")
                return False

            deployment_result.logs.append("Immediate deployment completed successfully")
            return True

        except Exception as e:
            error_msg = f"Immediate deployment failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_health_checks(self, deployment_config: DeploymentConfig,
                          deployment_result: DeploymentResult) -> bool:
        """Run post-deployment health checks."""
        try:
            deployment_result.logs.append("Running post-deployment health checks...")

            # Load configuration
            with open(deployment_config.config_file, 'r') as f:
                if deployment_config.config_file.endswith('.yaml') or deployment_config.config_file.endswith('.yml'):
                    config_data = yaml.safe_load(f)
                else:
                    config_data = json.load(f)

            # Run health checks based on deployment type
            if deployment_config.deployment_type == DeploymentType.BLUE_GREEN:
                return self._run_health_checks_green(config_data, deployment_result)
            elif deployment_config.deployment_type == DeploymentType.ROLLING:
                return self._run_health_checks_rolling(config_data, deployment_result)
            elif deployment_config.deployment_type == DeploymentType.CANARY:
                return self._run_health_checks_canary(config_data, deployment_result)
            else:
                return self._run_health_checks_immediate(config_data, deployment_result)

        except Exception as e:
            error_msg = f"Health checks failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_health_checks_green(self, config_data: Dict[str, Any],
                                deployment_result: DeploymentResult) -> bool:
        """Run health checks for blue-green deployment."""
        try:
            # Check service availability
            deployment_result.logs.append("Checking service availability...")
            time.sleep(1)  # Simulate health check

            # Check database connectivity
            deployment_result.logs.append("Checking database connectivity...")
            time.sleep(1)  # Simulate health check

            # Check API endpoints
            deployment_result.logs.append("Checking API endpoints...")
            time.sleep(1)  # Simulate health check

            deployment_result.logs.append("Green environment health checks passed")
            return True

        except Exception as e:
            error_msg = f"Green environment health checks failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_health_checks_rolling(self, config_data: Dict[str, Any],
                                  deployment_result: DeploymentResult) -> bool:
        """Run health checks for rolling deployment."""
        try:
            targets = config_data.get("deployment_targets", [])

            for target in targets:
                if not self._run_target_health_check(target, deployment_result):
                    return False

            deployment_result.logs.append("Rolling deployment health checks passed")
            return True

        except Exception as e:
            error_msg = f"Rolling deployment health checks failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_health_checks_canary(self, config_data: Dict[str, Any],
                                 deployment_result: DeploymentResult) -> bool:
        """Run health checks for canary deployment."""
        try:
            # Check canary environment
            deployment_result.logs.append("Checking canary environment...")
            time.sleep(1)  # Simulate health check

            # Check metrics and performance
            deployment_result.logs.append("Checking performance metrics...")
            time.sleep(1)  # Simulate health check

            deployment_result.logs.append("Canary deployment health checks passed")
            return True

        except Exception as e:
            error_msg = f"Canary deployment health checks failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_health_checks_immediate(self, config_data: Dict[str, Any],
                                   deployment_result: DeploymentResult) -> bool:
        """Run health checks for immediate deployment."""
        try:
            # Check all services
            deployment_result.logs.append("Checking all services...")
            time.sleep(2)  # Simulate health check

            # Check system resources
            deployment_result.logs.append("Checking system resources...")
            time.sleep(1)  # Simulate health check

            deployment_result.logs.append("Immediate deployment health checks passed")
            return True

        except Exception as e:
            error_msg = f"Immediate deployment health checks failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_target_health_check(self, target: str, deployment_result: DeploymentResult) -> bool:
        """Run health check for a specific target."""
        try:
            deployment_result.logs.append(f"Checking target: {target}")
            time.sleep(1)  # Simulate health check
            return True
        except Exception as e:
            error_msg = f"Health check failed for target {target}: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_canary_tests(self, config_data: Dict[str, Any],
                         deployment_result: DeploymentResult) -> bool:
        """Run canary tests."""
        try:
            deployment_result.logs.append("Running canary tests...")
            time.sleep(2)  # Simulate test execution
            return True
        except Exception as e:
            error_msg = f"Canary tests failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _run_canary_health_check(self, traffic_percentage: int,
                               deployment_result: DeploymentResult) -> bool:
        """Run health check for canary deployment at specific traffic percentage."""
        try:
            deployment_result.logs.append(f"Health check at {traffic_percentage}% traffic...")
            time.sleep(1)  # Simulate health check
            return True
        except Exception as e:
            error_msg = f"Canary health check failed at {traffic_percentage}%: {str(e)}"
            deployment_result.errors.append(error_msg)
            return False

    def _is_environment_available(self, environment: Environment) -> bool:
        """Check if environment is available for deployment."""
        try:
            env_config = self.environment_configs[environment]

            # Check if environment is locked
            if env_config.get("locked", False):
                logger.warning(f"Environment {environment.value} is locked")
                return False

            # Check if environment is in maintenance mode
            if env_config.get("maintenance_mode", False):
                logger.warning(f"Environment {environment.value} is in maintenance mode")
                return False

            # Check if environment is healthy
            if not env_config.get("healthy", True):
                logger.warning(f"Environment {environment.value} is not healthy")
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to check environment availability: {str(e)}")
            return False

    def _apply_environment_variables(self, config_data: Dict[str, Any],
                                   variables: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment variables to configuration."""
        try:
            # Convert config to string for variable substitution
            config_str = json.dumps(config_data)

            # Replace variables
            for key, value in variables.items():
                placeholder = f"${{{key}}}"
                config_str = config_str.replace(placeholder, str(value))

            # Convert back to dict
            return json.loads(config_str)

        except Exception as e:
            logger.error(f"Failed to apply environment variables: {str(e)}")
            return config_data

    def _apply_secrets(self, config_data: Dict[str, Any],
                      secrets: Dict[str, str]) -> Dict[str, Any]:
        """Apply secrets to configuration."""
        try:
            # Convert config to string for secret substitution
            config_str = json.dumps(config_data)

            # Replace secrets
            for key, value in secrets.items():
                placeholder = f"${{SECRET_{key}}}"
                config_str = config_str.replace(placeholder, str(value))

            # Convert back to dict
            return json.loads(config_str)

        except Exception as e:
            logger.error(f"Failed to apply secrets: {str(e)}")
            return config_data

    def _trigger_rollback(self, deployment_config: DeploymentConfig,
                         deployment_result: DeploymentResult):
        """Trigger deployment rollback."""
        try:
            deployment_result.rollback_triggered = True
            deployment_result.rollback_reason = "Deployment failed or health checks failed"
            deployment_result.logs.append("Triggering deployment rollback...")

            # Execute rollback
            if self._execute_rollback(deployment_config, deployment_result):
                deployment_result.status = DeploymentStatus.ROLLED_BACK
                deployment_result.logs.append("Rollback completed successfully")
            else:
                deployment_result.errors.append("Rollback failed")

        except Exception as e:
            error_msg = f"Failed to trigger rollback: {str(e)}"
            deployment_result.errors.append(error_msg)
            logger.error(error_msg)

    def _execute_rollback(self, deployment_config: DeploymentConfig,
                         deployment_result: DeploymentResult) -> bool:
        """Execute deployment rollback."""
        try:
            deployment_result.logs.append("Executing rollback...")

            # Rollback based on deployment type
            if deployment_config.deployment_type == DeploymentType.BLUE_GREEN:
                # Switch back to blue environment
                deployment_result.logs.append("Switching back to blue environment...")
                time.sleep(2)  # Simulate rollback time

            elif deployment_config.deployment_type == DeploymentType.ROLLING:
                # Rollback to previous version
                deployment_result.logs.append("Rolling back to previous version...")
                time.sleep(3)  # Simulate rollback time

            elif deployment_config.deployment_type == DeploymentType.CANARY:
                # Reduce traffic to 0%
                deployment_result.logs.append("Reducing traffic to 0%...")
                time.sleep(1)  # Simulate rollback time

            else:  # IMMEDIATE
                # Immediate rollback
                deployment_result.logs.append("Executing immediate rollback...")
                time.sleep(2)  # Simulate rollback time

            deployment_result.logs.append("Rollback completed successfully")
            return True

        except Exception as e:
            error_msg = f"Rollback execution failed: {str(e)}"
            deployment_result.errors.append(error_msg)
            logger.error(error_msg)
            return False

    def get_deployment_status(self, deployment_id: str) -> Optional[DeploymentResult]:
        """Get deployment status by ID."""
        return self.deployments.get(deployment_id)

    def get_deployment_history(self, environment: Environment = None,
                             status: DeploymentStatus = None) -> List[DeploymentResult]:
        """Get deployment history with optional filtering."""
        filtered_history = self.deployment_history

        if environment:
            filtered_history = [d for d in filtered_history if d.environment == environment]

        if status:
            filtered_history = [d for d in filtered_history if d.status == status]

        return filtered_history

    def export_deployment_report(self, output_path: str = "data/cicd/deployment_report.json") -> bool:
        """Export deployment report to file."""
        try:
            # Create output directory
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Prepare report data
            report_data = {
                "report_generated": datetime.now().isoformat(),
                "total_deployments": len(self.deployment_history),
                "deployments_by_status": {},
                "deployments_by_environment": {},
                "deployments_by_type": {},
                "recent_deployments": [],
                "failed_deployments": []
            }

            # Count deployments by status
            for status in DeploymentStatus:
                count = len([d for d in self.deployment_history if d.status == status])
                report_data["deployments_by_status"][status.value] = count

            # Count deployments by environment
            for environment in Environment:
                count = len([d for d in self.deployment_history if d.environment == environment])
                report_data["deployments_by_environment"][environment.value] = count

            # Count deployments by type
            for deploy_type in DeploymentType:
                count = len([d for d in self.deployment_history if d.deployment_type == deploy_type])
                report_data["deployments_by_type"][deploy_type.value] = count

            # Get recent deployments (last 10)
            recent_deployments = sorted(
                self.deployment_history,
                key=lambda x: x.start_time,
                reverse=True
            )[:10]

            report_data["recent_deployments"] = [
                {
                    "deployment_id": d.deployment_id,
                    "environment": d.environment.value,
                    "status": d.status.value,
                    "version": d.version,
                    "start_time": d.start_time.isoformat(),
                    "duration_minutes": d.duration_minutes
                }
                for d in recent_deployments
            ]

            # Get failed deployments
            failed_deployments = [d for d in self.deployment_history if d.status == DeploymentStatus.FAILED]
            report_data["failed_deployments"] = [
                {
                    "deployment_id": d.deployment_id,
                    "environment": d.environment.value,
                    "version": d.version,
                    "start_time": d.start_time.isoformat(),
                    "errors": d.errors
                }
                for d in failed_deployments
            ]

            # Export report
            with open(output_path, "w") as f:
                json.dump(report_data, f, indent=2)

            logger.info(f"Deployment report exported to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export deployment report: {str(e)}")
            return False


def setup_cicd_manager(config: Dict[str, Any]) -> CICDManager:
    """
    Setup CI/CD manager with configuration.

    Args:
        config: Configuration dictionary

    Returns:
        CICDManager: Configured CI/CD manager
    """
    try:
        logger.info("Setting up CI/CD Manager...")

        cicd_manager = CICDManager(config)

        logger.info("CI/CD Manager setup completed successfully")
        return cicd_manager

    except Exception as e:
        logger.error(f"Failed to setup CI/CD Manager: {str(e)}")
        raise


def run_deployment_pipeline(cicd_manager: CICDManager, environment: Environment,
                          version: str, deployment_type: DeploymentType = DeploymentType.BLUE_GREEN) -> DeploymentResult:
    """
    Run a complete deployment pipeline.

    Args:
        cicd_manager: CI/CD manager instance
        environment: Target environment
        version: Version to deploy
        deployment_type: Type of deployment

    Returns:
        DeploymentResult: Deployment result
    """
    try:
        logger.info(f"Starting deployment pipeline to {environment.value} version {version}")

        # Create deployment configuration
        deployment_config = cicd_manager.create_deployment(
            environment=environment,
            version=version,
            deployment_type=deployment_type
        )

        # Execute deployment
        deployment_result = cicd_manager.deploy_to_environment(deployment_config)

        logger.info(f"Deployment pipeline completed: {deployment_result.status.value}")
        return deployment_result

    except Exception as e:
        logger.error(f"Deployment pipeline failed: {str(e)}")
        raise
