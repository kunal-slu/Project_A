"""
Automated Data Quality Framework for Project_A

Provides comprehensive data quality checks, profiling, and anomaly detection.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum
import json
import logging
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')


class QualityCheckType(Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"


@dataclass
class QualityResult:
    """Result of a quality check"""
    check_type: QualityCheckType
    check_name: str
    passed: bool
    score: float  # 0.0 to 1.0
    details: Dict[str, Any]
    timestamp: datetime


class DataQualityProfiler:
    """Profile datasets for quality metrics"""
    
    def __init__(self, profile_path: str = "data/profiles"):
        self.profile_path = Path(profile_path)
        self.profile_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def profile_dataset(self, df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
        """Generate comprehensive profile for a dataset"""
        profile = {
            'dataset_name': dataset_name,
            'profile_timestamp': datetime.utcnow().isoformat(),
            'schema': self._extract_schema(df),
            'basic_stats': self._calculate_basic_stats(df),
            'completeness': self._calculate_completeness(df),
            'uniqueness': self._calculate_uniqueness(df),
            'data_types': self._analyze_data_types(df),
            'outliers': self._detect_outliers(df),
            'correlations': self._calculate_correlations(df),
            'quality_score': 0.0
        }
        
        # Calculate overall quality score
        profile['quality_score'] = self._calculate_quality_score(profile)
        
        # Save profile
        self._save_profile(profile)
        
        return profile
    
    def _extract_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Extract schema information"""
        schema = {}
        for col in df.columns:
            schema[col] = str(df[col].dtype)
        return schema
    
    def _calculate_basic_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate basic statistics"""
        stats = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'numeric_columns': [],
            'categorical_columns': [],
            'date_columns': []
        }
        
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                stats['numeric_columns'].append(col)
            elif pd.api.types.is_datetime64_any_dtype(df[col]) or 'date' in col.lower():
                stats['date_columns'].append(col)
            else:
                stats['categorical_columns'].append(col)
        
        return stats
    
    def _calculate_completeness(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate completeness scores per column"""
        completeness = {}
        total_records = len(df)
        
        for col in df.columns:
            non_null_count = df[col].notna().sum()
            completeness[col] = non_null_count / total_records if total_records > 0 else 0.0
        
        return completeness
    
    def _calculate_uniqueness(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate uniqueness scores per column"""
        uniqueness = {}
        
        for col in df.columns:
            if len(df) == 0:
                uniqueness[col] = 1.0
            else:
                unique_count = df[col].nunique()
                uniqueness[col] = unique_count / len(df)
        
        return uniqueness
    
    def _analyze_data_types(self, df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
        """Analyze data types and their distributions"""
        type_analysis = {}
        
        for col in df.columns:
            series = df[col].dropna()
            if len(series) == 0:
                type_analysis[col] = {'consistent': True, 'types': {}}
                continue
            
            # Determine the most common type
            type_counts = {}
            for val in series.head(1000):  # Sample to improve performance
                val_type = type(val).__name__
                type_counts[val_type] = type_counts.get(val_type, 0) + 1
            
            most_common_type = max(type_counts, key=type_counts.get) if type_counts else 'unknown'
            consistent = len(type_counts) <= 1
            
            type_analysis[col] = {
                'consistent': consistent,
                'most_common_type': most_common_type,
                'type_distribution': type_counts
            }
        
        return type_analysis
    
    def _detect_outliers(self, df: pd.DataFrame, method: str = 'iqr') -> Dict[str, List[int]]:
        """Detect outliers in numeric columns"""
        outliers = {}
        
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                series = df[col].dropna()
                if len(series) == 0:
                    outliers[col] = []
                    continue
                
                if method == 'iqr':
                    Q1 = series.quantile(0.25)
                    Q3 = series.quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    outlier_indices = df[(df[col] < lower_bound) | (df[col] > upper_bound)].index.tolist()
                    outliers[col] = outlier_indices
                else:  # z-score method
                    mean_val = series.mean()
                    std_val = series.std()
                    if std_val == 0:
                        outliers[col] = []
                        continue
                    
                    z_scores = abs((series - mean_val) / std_val)
                    outlier_indices = df[z_scores > 3].index.tolist()
                    outliers[col] = outlier_indices
            else:
                outliers[col] = []
        
        return outliers
    
    def _calculate_correlations(self, df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """Calculate correlations between numeric columns"""
        numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        if len(numeric_cols) < 2:
            return {}
        
        corr_matrix = df[numeric_cols].corr()
        correlations = {}
        
        for col1 in numeric_cols:
            for col2 in numeric_cols:
                if col1 != col2:
                    corr_val = corr_matrix.loc[col1, col2]
                    if not pd.isna(corr_val):
                        if col1 not in correlations:
                            correlations[col1] = {}
                        correlations[col1][col2] = corr_val
        
        return correlations
    
    def _calculate_quality_score(self, profile: Dict[str, Any]) -> float:
        """Calculate overall quality score based on profile"""
        scores = []
        
        # Completeness score (weight: 0.3)
        avg_completeness = sum(profile['completeness'].values()) / len(profile['completeness']) if profile['completeness'] else 0
        scores.append(avg_completeness * 0.3)
        
        # Uniqueness score (weight: 0.2)
        avg_uniqueness = sum(profile['uniqueness'].values()) / len(profile['uniqueness']) if profile['uniqueness'] else 0
        scores.append(avg_uniqueness * 0.2)
        
        # Data type consistency score (weight: 0.2)
        consistent_types = sum(1 for v in profile['data_types'].values() if v['consistent']) 
        type_consistency = consistent_types / len(profile['data_types']) if profile['data_types'] else 0
        scores.append(type_consistency * 0.2)
        
        # Missing data score (weight: 0.3)
        min_completeness = min(profile['completeness'].values()) if profile['completeness'] else 1
        scores.append(min_completeness * 0.3)
        
        return sum(scores)
    
    def _save_profile(self, profile: Dict[str, Any]):
        """Save profile to file"""
        filename = f"profile_{profile['dataset_name']}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.profile_path / filename
        
        with open(filepath, 'w') as f:
            json.dump(profile, f, indent=2, default=str)


class DataQualityChecker:
    """Performs specific quality checks on datasets"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.checks_history = []
    
    def run_completeness_check(self, df: pd.DataFrame, required_columns: List[str] = None) -> QualityResult:
        """Check completeness of required columns"""
        if required_columns is None:
            required_columns = df.columns.tolist()
        
        total_required = len(required_columns)
        complete_records = 0
        
        for idx, row in df.iterrows():
            if all(pd.notna(row[col]) for col in required_columns if col in df.columns):
                complete_records += 1
        
        completeness_score = complete_records / len(df) if len(df) > 0 else 1.0
        
        result = QualityResult(
            check_type=QualityCheckType.COMPLETENESS,
            check_name='completeness_check',
            passed=completeness_score >= 0.95,  # 95% threshold
            score=completeness_score,
            details={
                'total_records': len(df),
                'complete_records': complete_records,
                'incomplete_records': len(df) - complete_records,
                'required_columns': required_columns,
                'completeness_by_column': {col: df[col].notna().sum() / len(df) if len(df) > 0 else 0 for col in required_columns if col in df.columns}
            },
            timestamp=datetime.utcnow()
        )
        
        self.checks_history.append(result)
        return result
    
    def run_uniqueness_check(self, df: pd.DataFrame, unique_columns: List[str]) -> QualityResult:
        """Check uniqueness of specified columns"""
        duplicate_records = 0
        total_records = len(df)
        
        if unique_columns:
            duplicates = df.duplicated(subset=unique_columns, keep=False)
            duplicate_records = duplicates.sum()
        
        uniqueness_score = (total_records - duplicate_records) / total_records if total_records > 0 else 1.0
        
        result = QualityResult(
            check_type=QualityCheckType.UNIQUENESS,
            check_name='uniqueness_check',
            passed=uniqueness_score == 1.0,  # No duplicates allowed
            score=uniqueness_score,
            details={
                'total_records': total_records,
                'unique_records': total_records - duplicate_records,
                'duplicate_records': duplicate_records,
                'unique_columns': unique_columns,
                'duplicate_details': df[df.duplicated(subset=unique_columns, keep=False)].head(10).to_dict('records') if duplicate_records > 0 else []
            },
            timestamp=datetime.utcnow()
        )
        
        self.checks_history.append(result)
        return result
    
    def run_range_check(self, df: pd.DataFrame, column: str, min_val: float = None, max_val: float = None) -> QualityResult:
        """Check if values in a column fall within expected range"""
        if column not in df.columns:
            raise ValueError(f"Column {column} not found in dataframe")
        
        series = df[column]
        if min_val is not None:
            below_min = (series < min_val).sum()
        else:
            below_min = 0
            
        if max_val is not None:
            above_max = (series > max_val).sum()
        else:
            above_max = 0
        
        total_out_of_range = below_min + above_max
        total_valid = len(series) - total_out_of_range
        range_score = total_valid / len(series) if len(series) > 0 else 1.0
        
        result = QualityResult(
            check_type=QualityCheckType.VALIDITY,
            check_name=f'range_check_{column}',
            passed=range_score == 1.0,  # All values in range
            score=range_score,
            details={
                'column': column,
                'total_records': len(series),
                'valid_records': total_valid,
                'out_of_range_records': total_out_of_range,
                'below_min': below_min,
                'above_max': above_max,
                'min_val': min_val,
                'max_val': max_val,
                'actual_range': (series.min(), series.max()) if len(series) > 0 else (None, None)
            },
            timestamp=datetime.utcnow()
        )
        
        self.checks_history.append(result)
        return result
    
    def run_pattern_check(self, df: pd.DataFrame, column: str, pattern: str) -> QualityResult:
        """Check if values in a column match expected pattern (regex)"""
        import re
        
        if column not in df.columns:
            raise ValueError(f"Column {column} not found in dataframe")
        
        series = df[column].astype(str)
        matches = series.apply(lambda x: bool(re.match(pattern, x)) if pd.notna(x) else False)
        matches_count = matches.sum()
        pattern_score = matches_count / len(series) if len(series) > 0 else 1.0
        
        result = QualityResult(
            check_type=QualityCheckType.VALIDITY,
            check_name=f'pattern_check_{column}',
            passed=pattern_score == 1.0,  # All values match pattern
            score=pattern_score,
            details={
                'column': column,
                'total_records': len(series),
                'matching_records': matches_count,
                'non_matching_records': len(series) - matches_count,
                'pattern': pattern,
                'sample_non_matching': series[~matches].head(10).tolist() if (len(series) - matches_count) > 0 else []
            },
            timestamp=datetime.utcnow()
        )
        
        self.checks_history.append(result)
        return result
    
    def generate_quality_report(self, dataset_name: str) -> Dict[str, Any]:
        """Generate a comprehensive quality report"""
        report = {
            'dataset_name': dataset_name,
            'report_timestamp': datetime.utcnow().isoformat(),
            'total_checks': len(self.checks_history),
            'passed_checks': sum(1 for r in self.checks_history if r.passed),
            'failed_checks': sum(1 for r in self.checks_history if not r.passed),
            'overall_quality_score': np.mean([r.score for r in self.checks_history]) if self.checks_history else 1.0,
            'checks': [
                {
                    'check_type': r.check_type.value,
                    'check_name': r.check_name,
                    'passed': r.passed,
                    'score': r.score,
                    'timestamp': r.timestamp.isoformat(),
                    'details': r.details
                } for r in self.checks_history
            ]
        }
        
        return report


# Global instances
_profiler = None
_checker = None


def get_profiler() -> DataQualityProfiler:
    """Get the global profiler instance"""
    global _profiler
    if _profiler is None:
        from ..config_loader import load_config_resolved
        config = load_config_resolved('local/config/local.yaml')
        profile_path = config.get('paths', {}).get('profiles_root', 'data/profiles')
        _profiler = DataQualityProfiler(profile_path)
    return _profiler


def get_checker() -> DataQualityChecker:
    """Get the global checker instance"""
    global _checker
    if _checker is None:
        _checker = DataQualityChecker()
    return _checker


def profile_dataset(df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
    """Profile a dataset for quality metrics"""
    profiler = get_profiler()
    return profiler.profile_dataset(df, dataset_name)


def run_quality_check(check_type: str, df: pd.DataFrame, **kwargs) -> QualityResult:
    """Run a specific quality check"""
    checker = get_checker()
    
    if check_type == 'completeness':
        return checker.run_completeness_check(df, kwargs.get('required_columns'))
    elif check_type == 'uniqueness':
        return checker.run_uniqueness_check(df, kwargs.get('unique_columns', []))
    elif check_type == 'range':
        return checker.run_range_check(
            df, 
            kwargs['column'], 
            kwargs.get('min_val'), 
            kwargs.get('max_val')
        )
    elif check_type == 'pattern':
        return checker.run_pattern_check(
            df, 
            kwargs['column'], 
            kwargs['pattern']
        )
    else:
        raise ValueError(f"Unknown check type: {check_type}")


def generate_quality_report(dataset_name: str) -> Dict[str, Any]:
    """Generate a comprehensive quality report"""
    checker = get_checker()
    return checker.generate_quality_report(dataset_name)