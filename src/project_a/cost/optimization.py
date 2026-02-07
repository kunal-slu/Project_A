"""
Cost Optimization System for Project_A

Monitors and optimizes resource consumption for cloud resources and compute costs.
"""
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import logging
import psutil
import boto3
from botocore.exceptions import ClientError
import pandas as pd


@dataclass
class ResourceUsage:
    """Represents resource usage at a point in time"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    disk_percent: float
    network_io: Dict[str, int]  # bytes sent/received
    spark_resources: Dict[str, Any]  # driver/executor metrics


@dataclass
class CostOptimizationRecommendation:
    """A cost optimization recommendation"""
    recommendation_id: str
    title: str
    description: str
    estimated_savings: float  # in dollars
    priority: str  # high, medium, low
    implementation_effort: str  # high, medium, low
    tags: List[str]


class ResourceMonitor:
    """Monitors resource usage for cost optimization"""
    
    def __init__(self, metrics_path: str = "data/cost_metrics"):
        self.metrics_path = Path(metrics_path)
        self.metrics_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.metrics_file = self.metrics_path / "resource_usage.jsonl"
    
    def collect_current_usage(self) -> ResourceUsage:
        """Collect current system resource usage"""
        # System metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_percent = psutil.virtual_memory().percent
        disk_percent = psutil.disk_usage('/').percent
        
        # Network IO
        net_io = psutil.net_io_counters()
        network_io = {
            'bytes_sent': net_io.bytes_sent,
            'bytes_recv': net_io.bytes_recv,
            'packets_sent': net_io.packets_sent,
            'packets_recv': net_io.packets_recv
        }
        
        # Placeholder for Spark metrics (would connect to Spark UI in real implementation)
        spark_resources = {
            'driver_memory_used_mb': 0,
            'executor_memory_used_mb': 0,
            'active_tasks': 0,
            'completed_tasks': 0
        }
        
        usage = ResourceUsage(
            timestamp=datetime.utcnow(),
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            disk_percent=disk_percent,
            network_io=network_io,
            spark_resources=spark_resources
        )
        
        # Save to metrics log
        self._log_resource_usage(usage)
        
        return usage
    
    def _log_resource_usage(self, usage: ResourceUsage):
        """Log resource usage to file"""
        with open(self.metrics_file, 'a') as f:
            f.write(json.dumps({
                'timestamp': usage.timestamp.isoformat(),
                'cpu_percent': usage.cpu_percent,
                'memory_percent': usage.memory_percent,
                'disk_percent': usage.disk_percent,
                'network_io': usage.network_io,
                'spark_resources': usage.spark_resources
            }) + '\n')
    
    def analyze_usage_patterns(self, days_back: int = 7) -> Dict[str, Any]:
        """Analyze resource usage patterns over time"""
        cutoff_time = datetime.utcnow() - timedelta(days=days_back)
        
        # Read recent metrics
        metrics = []
        with open(self.metrics_file, 'r') as f:
            for line in f:
                try:
                    metric = json.loads(line.strip())
                    metric_time = datetime.fromisoformat(metric['timestamp'])
                    if metric_time >= cutoff_time:
                        metrics.append(metric)
                except:
                    continue  # Skip malformed lines
        
        if not metrics:
            return {'error': 'No metrics available for analysis'}
        
        # Calculate averages and peaks
        cpu_values = [m['cpu_percent'] for m in metrics]
        memory_values = [m['memory_percent'] for m in metrics]
        disk_values = [m['disk_percent'] for m in metrics]
        
        analysis = {
            'period_days': days_back,
            'total_samples': len(metrics),
            'cpu': {
                'avg': sum(cpu_values) / len(cpu_values),
                'peak': max(cpu_values),
                'min': min(cpu_values),
                'percentile_95': sorted(cpu_values)[int(len(cpu_values) * 0.95)]
            },
            'memory': {
                'avg': sum(memory_values) / len(memory_values),
                'peak': max(memory_values),
                'min': min(memory_values),
                'percentile_95': sorted(memory_values)[int(len(memory_values) * 0.95)]
            },
            'disk': {
                'avg': sum(disk_values) / len(disk_values),
                'peak': max(disk_values),
                'min': min(disk_values),
                'percentile_95': sorted(disk_values)[int(len(disk_values) * 0.95)]
            },
            'recommendations': []
        }
        
        # Generate recommendations based on usage patterns
        if analysis['cpu']['avg'] < 20:
            analysis['recommendations'].append({
                'category': 'compute',
                'issue': 'Underutilized CPU resources',
                'suggestion': 'Consider downsizing compute instances',
                'estimated_savings': analysis['cpu']['avg'] * 0.5  # Simplified calculation
            })
        
        if analysis['memory']['avg'] < 30:
            analysis['recommendations'].append({
                'category': 'memory',
                'issue': 'Underutilized memory resources',
                'suggestion': 'Consider downsizing memory allocation',
                'estimated_savings': analysis['memory']['avg'] * 0.3  # Simplified calculation
            })
        
        return analysis


class AWSCostAnalyzer:
    """Analyzes AWS costs and provides optimization recommendations"""
    
    def __init__(self, aws_access_key_id: str = None, aws_secret_access_key: str = None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.logger = logging.getLogger(__name__)
        
        # Initialize boto3 clients
        session_kwargs = {}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs = {
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key
            }
        
        self.ce_client = boto3.client('ce', **session_kwargs)  # Cost Explorer
        self.ec2_client = boto3.client('ec2', **session_kwargs)
        self.s3_client = boto3.client('s3', **session_kwargs)
    
    def get_cost_analysis(self, days_back: int = 30) -> Dict[str, Any]:
        """Get AWS cost analysis for the specified period"""
        try:
            end_date = datetime.utcnow().date()
            start_date = end_date - timedelta(days=days_back)
            
            # Get cost and usage data
            response = self.ce_client.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.isoformat(),
                    'End': end_date.isoformat()
                },
                Granularity='DAILY',
                Metrics=['UNBLENDEDCOST', 'BLENDEDCOST']
            )
            
            daily_costs = []
            for result in response['ResultsByTime']:
                daily_costs.append({
                    'date': result['TimePeriod']['Start'],
                    'cost': float(result['Total'].get('UnblendedCost', {}).get('Amount', 0)),
                    'unit': result['Total'].get('UnblendedCost', {}).get('Unit', 'USD')
                })
            
            # Calculate totals
            total_cost = sum(d['cost'] for d in daily_costs)
            avg_daily_cost = total_cost / len(daily_costs) if daily_costs else 0
            
            # Get service breakdown
            service_response = self.ce_client.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.isoformat(),
                    'End': end_date.isoformat()
                },
                Granularity='DAILY',
                Metrics=['UNBLENDEDCOST'],
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    }
                ]
            )
            
            service_costs = {}
            for result in service_response['ResultsByTime']:
                for group in result['Groups']:
                    service = group['Keys'][0]
                    cost = float(group['Metrics'].get('UnblendedCost', {}).get('Amount', 0))
                    service_costs[service] = service_costs.get(service, 0) + cost
            
            return {
                'period_days': days_back,
                'total_cost': total_cost,
                'avg_daily_cost': avg_daily_cost,
                'daily_costs': daily_costs,
                'service_breakdown': service_costs,
                'recommendations': self._generate_cost_recommendations(service_costs)
            }
            
        except ClientError as e:
            self.logger.error(f"AWS Cost Explorer error: {e}")
            return {'error': str(e)}
    
    def _generate_cost_recommendations(self, service_costs: Dict[str, float]) -> List[Dict[str, Any]]:
        """Generate cost optimization recommendations based on service usage"""
        recommendations = []
        
        # EC2 recommendations
        ec2_cost = service_costs.get('Amazon Elastic Compute Cloud', 0)
        if ec2_cost > 100:  # If spending more than $100/month on EC2
            recommendations.append({
                'service': 'EC2',
                'category': 'Compute',
                'title': 'Right-size EC2 instances',
                'description': 'Consider rightsizing EC2 instances based on actual usage patterns',
                'estimated_monthly_savings': ec2_cost * 0.2,  # 20% savings estimate
                'priority': 'high'
            })
        
        # S3 recommendations
        s3_cost = service_costs.get('Amazon Simple Storage Service', 0)
        if s3_cost > 50:  # If spending more than $50/month on S3
            recommendations.append({
                'service': 'S3',
                'category': 'Storage',
                'title': 'Optimize S3 storage classes',
                'description': 'Review data access patterns and migrate to appropriate storage classes',
                'estimated_monthly_savings': s3_cost * 0.15,  # 15% savings estimate
                'priority': 'medium'
            })
        
        # EMR recommendations
        emr_cost = service_costs.get('Amazon EMR', 0)
        if emr_cost > 200:  # If spending more than $200/month on EMR
            recommendations.append({
                'service': 'EMR',
                'category': 'Analytics',
                'title': 'Optimize EMR cluster sizing',
                'description': 'Right-size EMR clusters and use spot instances where possible',
                'estimated_monthly_savings': emr_cost * 0.3,  # 30% savings estimate
                'priority': 'high'
            })
        
        return recommendations


class CostOptimizer:
    """Main cost optimization orchestrator"""
    
    def __init__(self, resource_monitor: ResourceMonitor, 
                 aws_analyzer: AWSCostAnalyzer = None):
        self.resource_monitor = resource_monitor
        self.aws_analyzer = aws_analyzer
        self.logger = logging.getLogger(__name__)
    
    def run_cost_optimization_analysis(self, days_back: int = 30) -> Dict[str, Any]:
        """Run comprehensive cost optimization analysis"""
        results = {
            'analysis_timestamp': datetime.utcnow().isoformat(),
            'resource_analysis': self.resource_monitor.analyze_usage_patterns(days_back),
            'cost_recommendations': [],
            'total_estimated_savings': 0.0
        }
        
        # Add AWS cost analysis if available
        if self.aws_analyzer:
            try:
                aws_analysis = self.aws_analyzer.get_cost_analysis(days_back)
                results['aws_cost_analysis'] = aws_analysis
            except Exception as e:
                self.logger.error(f"Error getting AWS cost analysis: {e}")
                results['aws_cost_analysis'] = {'error': str(e)}
        
        # Combine all recommendations
        all_recommendations = []
        
        # Add resource-based recommendations
        if 'recommendations' in results['resource_analysis']:
            for rec in results['resource_analysis']['recommendations']:
                all_recommendations.append({
                    'source': 'resource_monitor',
                    'category': rec.get('category', 'general'),
                    'title': rec.get('suggestion', 'General optimization'),
                    'description': rec.get('issue', 'Resource utilization issue'),
                    'estimated_savings': rec.get('estimated_savings', 0),
                    'priority': 'medium'
                })
        
        # Add AWS recommendations if available
        if 'aws_cost_analysis' in results and 'recommendations' in results['aws_cost_analysis']:
            for rec in results['aws_cost_analysis']['recommendations']:
                all_recommendations.append(rec)
        
        results['cost_recommendations'] = all_recommendations
        results['total_estimated_savings'] = sum(rec.get('estimated_monthly_savings', rec.get('estimated_savings', 0)) 
                                               for rec in all_recommendations)
        
        # Save analysis results
        self._save_analysis_results(results)
        
        return results
    
    def _save_analysis_results(self, results: Dict[str, Any]):
        """Save analysis results to file"""
        results_file = self.resource_monitor.metrics_path / f"cost_analysis_{results['analysis_timestamp'][:10]}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
    
    def get_optimization_dashboard_data(self) -> Dict[str, Any]:
        """Get data for cost optimization dashboard"""
        # Get latest resource analysis
        resource_analysis = self.resource_monitor.analyze_usage_patterns(7)
        
        # Get latest cost analysis
        cost_analysis = None
        if self.aws_analyzer:
            try:
                cost_analysis = self.aws_analyzer.get_cost_analysis(30)
            except:
                pass
        
        # Calculate key metrics
        current_monthly_cost = cost_analysis['total_cost'] if cost_analysis else 0
        estimated_savings = sum(rec.get('estimated_monthly_savings', rec.get('estimated_savings', 0)) 
                               for rec in (cost_analysis or {}).get('recommendations', []))
        potential_cost_reduction = (estimated_savings / current_monthly_cost * 100) if current_monthly_cost > 0 else 0
        
        dashboard_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'current_monthly_cost': current_monthly_cost,
            'estimated_monthly_savings': estimated_savings,
            'potential_cost_reduction_percent': round(potential_cost_reduction, 2),
            'active_recommendations': len((cost_analysis or {}).get('recommendations', [])),
            'resource_utilization': {
                'cpu_avg': resource_analysis.get('cpu', {}).get('avg', 0),
                'memory_avg': resource_analysis.get('memory', {}).get('avg', 0),
                'disk_avg': resource_analysis.get('disk', {}).get('avg', 0)
            }
        }
        
        return dashboard_data


# Global instances
_resource_monitor = None
_aws_analyzer = None
_cost_optimizer = None


def get_resource_monitor() -> ResourceMonitor:
    """Get the global resource monitor instance"""
    global _resource_monitor
    if _resource_monitor is None:
        from ..config_loader import load_config_resolved
        config = load_config_resolved('local/config/local.yaml')
        metrics_path = config.get('paths', {}).get('cost_metrics_root', 'data/cost_metrics')
        _resource_monitor = ResourceMonitor(metrics_path)
    return _resource_monitor


def get_aws_analyzer(aws_access_key_id: str = None, aws_secret_access_key: str = None) -> AWSCostAnalyzer:
    """Get the global AWS analyzer instance"""
    global _aws_analyzer
    if _aws_analyzer is None:
        _aws_analyzer = AWSCostAnalyzer(aws_access_key_id, aws_secret_access_key)
    return _aws_analyzer


def get_cost_optimizer(aws_access_key_id: str = None, aws_secret_access_key: str = None) -> CostOptimizer:
    """Get the global cost optimizer instance"""
    global _cost_optimizer
    if _cost_optimizer is None:
        resource_monitor = get_resource_monitor()
        aws_analyzer = get_aws_analyzer(aws_access_key_id, aws_secret_access_key)
        _cost_optimizer = CostOptimizer(resource_monitor, aws_analyzer)
    return _cost_optimizer


def collect_resource_usage() -> ResourceUsage:
    """Collect current resource usage"""
    monitor = get_resource_monitor()
    return monitor.collect_current_usage()


def analyze_resource_usage(days_back: int = 7) -> Dict[str, Any]:
    """Analyze resource usage patterns"""
    monitor = get_resource_monitor()
    return monitor.analyze_usage_patterns(days_back)


def run_cost_optimization_analysis(days_back: int = 30, 
                                 aws_access_key_id: str = None, 
                                 aws_secret_access_key: str = None) -> Dict[str, Any]:
    """Run comprehensive cost optimization analysis"""
    optimizer = get_cost_optimizer(aws_access_key_id, aws_secret_access_key)
    return optimizer.run_cost_optimization_analysis(days_back)


def get_optimization_dashboard_data(aws_access_key_id: str = None, 
                                 aws_secret_access_key: str = None) -> Dict[str, Any]:
    """Get data for cost optimization dashboard"""
    optimizer = get_cost_optimizer(aws_access_key_id, aws_secret_access_key)
    return optimizer.get_optimization_dashboard_data()