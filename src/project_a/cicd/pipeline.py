"""
CI/CD Pipeline System for Project_A

Manages automated deployment pipelines for data engineering code.
"""
import json
import subprocess
import tempfile
import shutil
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from pathlib import Path
import logging
import time
import yaml
from enum import Enum


class PipelineStage(Enum):
    BUILD = "build"
    TEST = "test"
    INTEGRATION_TEST = "integration_test"
    STAGING = "staging"
    PRODUCTION = "production"


class PipelineStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PipelineJob:
    """Represents a single job in the pipeline"""
    job_id: str
    name: str
    stage: PipelineStage
    command: str
    dependencies: List[str]
    timeout_seconds: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: PipelineStatus = PipelineStatus.PENDING
    output_log: str = ""
    error_log: str = ""


@dataclass
class PipelineRun:
    """Represents a complete pipeline run"""
    run_id: str
    pipeline_name: str
    trigger_type: str  # manual, scheduled, webhook
    trigger_source: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: PipelineStatus = PipelineStatus.PENDING
    jobs: List[PipelineJob] = None
    artifacts: List[str] = None
    commit_hash: Optional[str] = None
    branch: Optional[str] = None


class BuildManager:
    """Manages build processes"""
    
    def __init__(self, builds_path: str = "builds"):
        self.builds_path = Path(builds_path)
        self.builds_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def build_artifact(self, source_path: str, artifact_name: str, 
                      build_script: str = "./build.sh") -> str:
        """Build an artifact from source"""
        build_dir = self.builds_path / f"build_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        build_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy source to build directory
        source_path_obj = Path(source_path)
        if source_path_obj.is_file():
            shutil.copy2(source_path_obj, build_dir / source_path_obj.name)
        else:
            shutil.copytree(source_path_obj, build_dir / source_path_obj.name)
        
        # Change to build directory
        original_cwd = Path.cwd()
        try:
            os.chdir(build_dir)
            
            # Run build script
            result = subprocess.run(
                build_script,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode != 0:
                raise Exception(f"Build failed: {result.stderr}")
            
            # Find built artifact
            artifact_path = None
            for file_path in build_dir.rglob(f"*{artifact_name}*"):
                if file_path.is_file():
                    artifact_path = file_path
                    break
            
            if not artifact_path:
                raise Exception(f"Artifact {artifact_name} not found after build")
            
            # Move artifact to final location
            final_artifact = self.builds_path / f"{artifact_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.whl"
            shutil.move(artifact_path, final_artifact)
            
            self.logger.info(f"Build successful: {final_artifact}")
            return str(final_artifact)
            
        finally:
            os.chdir(original_cwd)
    
    def run_unit_tests(self, test_command: str = "python -m pytest tests/") -> Dict[str, Any]:
        """Run unit tests as part of build"""
        try:
            result = subprocess.run(
                test_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            return {
                'success': result.returncode == 0,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'return_code': result.returncode
            }
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'error': 'Tests timed out',
                'stdout': '',
                'stderr': 'Timeout expired'
            }


class DeploymentManager:
    """Manages deployment processes"""
    
    def __init__(self, deployments_path: str = "deployments"):
        self.deployments_path = Path(deployments_path)
        self.deployments_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def deploy_to_environment(self, artifact_path: str, environment: str, 
                            deployment_script: str = "./deploy.sh") -> Dict[str, Any]:
        """Deploy artifact to specified environment"""
        deployment_id = f"deploy_{environment}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        deployment_dir = self.deployments_path / deployment_id
        deployment_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy artifact to deployment directory
        artifact_name = Path(artifact_path).name
        deployed_artifact = deployment_dir / artifact_name
        shutil.copy2(artifact_path, deployed_artifact)
        
        # Run deployment script
        original_cwd = Path.cwd()
        try:
            os.chdir(deployment_dir)
            
            # Prepare deployment command with environment
            cmd = f"ENVIRONMENT={environment} ARTIFACT_PATH={deployed_artifact} {deployment_script}"
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=900  # 15 minute timeout
            )
            
            return {
                'success': result.returncode == 0,
                'deployment_id': deployment_id,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'return_code': result.returncode,
                'deployed_artifact': str(deployed_artifact)
            }
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'error': 'Deployment timed out',
                'deployment_id': deployment_id
            }
        finally:
            os.chdir(original_cwd)
    
    def rollback_deployment(self, deployment_id: str) -> Dict[str, Any]:
        """Rollback a deployment"""
        # In a real system, this would restore from backup
        # For now, just log the rollback
        self.logger.info(f"Rollback initiated for deployment: {deployment_id}")
        return {
            'success': True,
            'deployment_id': deployment_id,
            'rolled_back_at': datetime.utcnow().isoformat()
        }


class PipelineManager:
    """Manages the complete CI/CD pipeline"""
    
    def __init__(self, pipeline_config_path: str = "config/pipeline.yaml",
                 artifacts_path: str = "artifacts"):
        self.pipeline_config_path = Path(pipeline_config_path)
        self.artifacts_path = Path(artifacts_path)
        self.artifacts_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        # Initialize managers
        self.build_manager = BuildManager()
        self.deployment_manager = DeploymentManager()
        
        # Load pipeline configuration
        self.pipeline_config = self._load_pipeline_config()
    
    def _load_pipeline_config(self) -> Dict[str, Any]:
        """Load pipeline configuration"""
        if self.pipeline_config_path.exists():
            with open(self.pipeline_config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            # Default configuration
            return {
                'stages': {
                    'build': {
                        'command': 'python setup.py bdist_wheel',
                        'timeout': 300
                    },
                    'test': {
                        'command': 'python -m pytest tests/',
                        'timeout': 600
                    },
                    'integration_test': {
                        'command': 'python -m pytest tests/integration/',
                        'timeout': 1200
                    },
                    'staging': {
                        'command': './deploy_staging.sh',
                        'timeout': 900
                    },
                    'production': {
                        'command': './deploy_production.sh',
                        'timeout': 1800,
                        'requires_approval': True
                    }
                }
            }
    
    def create_pipeline_run(self, pipeline_name: str, trigger_type: str = "manual",
                          trigger_source: str = "unknown", branch: str = "main") -> PipelineRun:
        """Create a new pipeline run"""
        run_id = f"run_{pipeline_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Get current commit hash if in git repo
        commit_hash = None
        try:
            result = subprocess.run(['git', 'rev-parse', 'HEAD'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                commit_hash = result.stdout.strip()
        except:
            pass  # Not in git repo or git not available
        
        pipeline_run = PipelineRun(
            run_id=run_id,
            pipeline_name=pipeline_name,
            trigger_type=trigger_type,
            trigger_source=trigger_source,
            created_at=datetime.utcnow(),
            status=PipelineStatus.PENDING,
            jobs=[],
            artifacts=[],
            commit_hash=commit_hash,
            branch=branch
        )
        
        self.logger.info(f"Pipeline run created: {run_id}")
        return pipeline_run
    
    def execute_pipeline(self, pipeline_run: PipelineRun) -> PipelineRun:
        """Execute a complete pipeline run"""
        pipeline_run.started_at = datetime.utcnow()
        pipeline_run.status = PipelineStatus.RUNNING
        
        try:
            # Execute each stage in order
            stages = [
                PipelineStage.BUILD,
                PipelineStage.TEST,
                PipelineStage.INTEGRATION_TEST,
                PipelineStage.STAGING,
                PipelineStage.PRODUCTION
            ]
            
            for stage in stages:
                if pipeline_run.status != PipelineStatus.RUNNING:
                    break
                
                job_result = self._execute_stage(pipeline_run, stage)
                
                if job_result['success']:
                    self.logger.info(f"Stage {stage.value} completed successfully")
                else:
                    self.logger.error(f"Stage {stage.value} failed: {job_result.get('error')}")
                    pipeline_run.status = PipelineStatus.FAILED
                    break
            
            if pipeline_run.status == PipelineStatus.RUNNING:
                pipeline_run.status = PipelineStatus.SUCCESS
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            pipeline_run.status = PipelineStatus.FAILED
        
        finally:
            pipeline_run.completed_at = datetime.utcnow()
        
        return pipeline_run
    
    def _execute_stage(self, pipeline_run: PipelineRun, stage: PipelineStage) -> Dict[str, Any]:
        """Execute a single pipeline stage"""
        stage_config = self.pipeline_config['stages'].get(stage.value, {})
        
        # Create job for this stage
        job_id = f"job_{pipeline_run.run_id}_{stage.value}"
        job = PipelineJob(
            job_id=job_id,
            name=f"{pipeline_run.pipeline_name}_{stage.value}",
            stage=stage,
            command=stage_config.get('command', ''),
            dependencies=[],
            timeout_seconds=stage_config.get('timeout', 300),
            created_at=datetime.utcnow()
        )
        
        pipeline_run.jobs.append(job)
        
        # Update job status
        job.started_at = datetime.utcnow()
        job.status = PipelineStatus.RUNNING
        
        try:
            # Execute stage-specific logic
            if stage == PipelineStage.BUILD:
                result = self._execute_build_stage(job, stage_config)
            elif stage == PipelineStage.TEST:
                result = self._execute_test_stage(job, stage_config)
            elif stage == PipelineStage.INTEGRATION_TEST:
                result = self._execute_integration_test_stage(job, stage_config)
            elif stage == PipelineStage.STAGING:
                result = self._execute_deploy_stage(job, 'staging', stage_config)
            elif stage == PipelineStage.PRODUCTION:
                # Check if production requires approval
                if stage_config.get('requires_approval', False):
                    self.logger.warning("Production deployment requires manual approval")
                    result = {'success': False, 'error': 'Manual approval required for production'}
                else:
                    result = self._execute_deploy_stage(job, 'production', stage_config)
            else:
                result = {'success': False, 'error': f'Unknown stage: {stage}'}
            
            # Update job status based on result
            if result['success']:
                job.status = PipelineStatus.SUCCESS
                job.output_log = result.get('stdout', '')
            else:
                job.status = PipelineStatus.FAILED
                job.error_log = result.get('error', result.get('stderr', 'Unknown error'))
            
            job.completed_at = datetime.utcnow()
            
            return result
            
        except Exception as e:
            job.status = PipelineStatus.FAILED
            job.error_log = str(e)
            job.completed_at = datetime.utcnow()
            return {'success': False, 'error': str(e)}
    
    def _execute_build_stage(self, job: PipelineJob, stage_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute build stage"""
        try:
            # For demo purposes, we'll create a dummy artifact
            # In a real system, this would call build_manager.build_artifact()
            artifact_name = f"project_a_artifact_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.whl"
            artifact_path = self.artifacts_path / artifact_name
            
            # Create a dummy artifact file
            with open(artifact_path, 'w') as f:
                f.write(f"# Dummy artifact for pipeline {job.job_id}\n")
                f.write(f"Built at: {datetime.utcnow().isoformat()}\n")
            
            return {
                'success': True,
                'artifact_path': str(artifact_path),
                'stdout': f"Build successful: {artifact_path}"
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _execute_test_stage(self, job: PipelineJob, stage_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute test stage"""
        # Run unit tests
        test_result = self.build_manager.run_unit_tests(job.command or "python -m pytest tests/")
        return test_result
    
    def _execute_integration_test_stage(self, job: PipelineJob, stage_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute integration test stage"""
        # Similar to test stage but for integration tests
        test_result = self.build_manager.run_unit_tests(job.command or "python -m pytest tests/integration/")
        return test_result
    
    def _execute_deploy_stage(self, job: PipelineJob, environment: str, stage_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute deployment stage"""
        # In a real system, this would deploy to the environment
        # For demo, we'll just simulate the deployment
        
        # Find the artifact from previous build stage
        build_artifact = None
        for prev_job in job.dependencies or []:
            # In a real system, we'd get the actual artifact path
            pass
        
        # Simulate deployment
        deployment_result = {
            'success': True,
            'deployment_id': f"deploy_{environment}_{job.job_id}",
            'stdout': f"Deployment to {environment} successful",
            'deployed_at': datetime.utcnow().isoformat()
        }
        
        return deployment_result
    
    def get_pipeline_status(self, run_id: str) -> Optional[PipelineRun]:
        """Get status of a pipeline run"""
        # In a real system, this would retrieve from persistent storage
        # For now, we'll just return None
        return None
    
    def cancel_pipeline(self, run_id: str) -> bool:
        """Cancel a running pipeline"""
        # In a real system, this would cancel the actual running processes
        self.logger.info(f"Pipeline cancellation requested: {run_id}")
        return True


class PipelineOrchestrator:
    """Orchestrates multiple pipeline runs"""
    
    def __init__(self, pipeline_manager: PipelineManager):
        self.pipeline_manager = pipeline_manager
        self.active_runs = {}
        self.logger = logging.getLogger(__name__)
    
    def trigger_pipeline(self, pipeline_name: str, trigger_type: str = "manual",
                        trigger_source: str = "unknown", branch: str = "main") -> PipelineRun:
        """Trigger a new pipeline run"""
        pipeline_run = self.pipeline_manager.create_pipeline_run(
            pipeline_name, trigger_type, trigger_source, branch
        )
        
        # Execute pipeline asynchronously (in this simplified version, synchronously)
        pipeline_run = self.pipeline_manager.execute_pipeline(pipeline_run)
        
        return pipeline_run
    
    def schedule_pipeline(self, pipeline_name: str, schedule: str,
                         trigger_source: str = "scheduler") -> Dict[str, Any]:
        """Schedule a pipeline to run on a schedule"""
        # In a real system, this would integrate with a scheduler like cron or Airflow
        return {
            'scheduled': True,
            'pipeline_name': pipeline_name,
            'schedule': schedule,
            'trigger_source': trigger_source,
            'scheduled_at': datetime.utcnow().isoformat()
        }
    
    def get_active_runs(self) -> List[PipelineRun]:
        """Get all active pipeline runs"""
        # In a real system, this would return actual active runs
        return []


# Global instances
_pipeline_manager = None
_pipeline_orchestrator = None


def get_pipeline_manager() -> PipelineManager:
    """Get the global pipeline manager instance"""
    global _pipeline_manager
    if _pipeline_manager is None:
        from ..config_loader import load_config_resolved
        config = load_config_resolved('local/config/local.yaml')
        pipeline_config_path = config.get('paths', {}).get('pipeline_config_root', 'config/pipeline.yaml')
        artifacts_path = config.get('paths', {}).get('artifacts_root', 'artifacts')
        _pipeline_manager = PipelineManager(pipeline_config_path, artifacts_path)
    return _pipeline_manager


def get_pipeline_orchestrator() -> PipelineOrchestrator:
    """Get the global pipeline orchestrator instance"""
    global _pipeline_orchestrator
    if _pipeline_orchestrator is None:
        pipeline_mgr = get_pipeline_manager()
        _pipeline_orchestrator = PipelineOrchestrator(pipeline_mgr)
    return _pipeline_orchestrator


def trigger_pipeline(pipeline_name: str, trigger_type: str = "manual",
                   trigger_source: str = "unknown", branch: str = "main") -> PipelineRun:
    """Trigger a new pipeline run"""
    orchestrator = get_pipeline_orchestrator()
    return orchestrator.trigger_pipeline(pipeline_name, trigger_type, trigger_source, branch)


def schedule_pipeline(pipeline_name: str, schedule: str,
                     trigger_source: str = "scheduler") -> Dict[str, Any]:
    """Schedule a pipeline to run on a schedule"""
    orchestrator = get_pipeline_orchestrator()
    return orchestrator.schedule_pipeline(pipeline_name, schedule, trigger_source)


def get_pipeline_status(run_id: str) -> Optional[PipelineRun]:
    """Get status of a pipeline run"""
    pipeline_mgr = get_pipeline_manager()
    return pipeline_mgr.get_pipeline_status(run_id)


def cancel_pipeline(run_id: str) -> bool:
    """Cancel a running pipeline"""
    pipeline_mgr = get_pipeline_manager()
    return pipeline_mgr.cancel_pipeline(run_id)