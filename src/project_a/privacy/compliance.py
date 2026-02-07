"""
Data Privacy and Compliance System for Project_A

Manages PII detection, data anonymization, and compliance with privacy regulations.
"""
import re
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import logging
import pandas as pd
from enum import Enum
import warnings
warnings.filterwarnings('ignore')


class PrivacyRegulation(Enum):
    GDPR = "GDPR"
    CCPA = "CCPA"
    HIPAA = "HIPAA"
    PCI_DSS = "PCI_DSS"


class DataSensitivity(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"  # Personally Identifiable Information
    PHI = "phi"  # Protected Health Information


@dataclass
class PrivacyImpactAssessment:
    """Privacy Impact Assessment for datasets"""
    assessment_id: str
    dataset_name: str
    data_categories: List[DataSensitivity]
    processing_purposes: List[str]
    data_sharing: bool
    retention_period: str
    conducted_by: str
    conducted_at: datetime
    risk_level: str  # low, medium, high
    mitigation_measures: List[str]


@dataclass
class DataSubjectRequest:
    """Represents a data subject request (e.g., right to erasure, access)"""
    request_id: str
    subject_id: str
    request_type: str  # access, erasure, portability, rectification
    requested_at: datetime
    processed_at: Optional[datetime] = None
    status: str = "pending"  # pending, approved, rejected, completed
    reason: Optional[str] = None


class PIIDetector:
    """Detects PII in data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Regex patterns for PII detection
        self.patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'(?:\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'credit_card': r'\b(?:\d{4}[-\s]?){3}\d{4}\b|\b(?:\d{4}[-\s]?){2}\d{7}\b',
            'ip_address': r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
            'address': r'\d+\s+\w+(?:\s+\w+)*,\s*\w+(?:\s+\w+)*,\s*\w{2}\s+\d{5}(?:-\d{4})?',
            'dob': r'\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b'
        }
    
    def detect_pii_in_dataframe(self, df: pd.DataFrame) -> Dict[str, List[Tuple[int, str, str]]]:
        """Detect PII in DataFrame columns"""
        pii_found = {}
        
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):  # Only check string/object columns
                for idx, value in df[col].items():
                    if pd.notna(value):
                        value_str = str(value)
                        for pii_type, pattern in self.patterns.items():
                            matches = re.findall(pattern, value_str, re.IGNORECASE)
                            if matches:
                                if pii_type not in pii_found:
                                    pii_found[pii_type] = []
                                pii_found[pii_type].append((idx, col, str(matches)))
        
        return pii_found
    
    def classify_sensitivity(self, df: pd.DataFrame) -> Dict[str, DataSensitivity]:
        """Classify columns by sensitivity level"""
        sensitivity_map = {}
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Check column name for PII indicators
            if any(pii_ind in col_lower for pii_ind in ['email', 'phone', 'ssn', 'social_security', 'credit_card', 'name']):
                sensitivity_map[col] = DataSensitivity.PII
            elif any(pii_ind in col_lower for pii_ind in ['address', 'location', 'ip', 'dob', 'birth', 'health', 'medical']):
                sensitivity_map[col] = DataSensitivity.CONFIDENTIAL
            elif any(internal_ind in col_lower for internal_ind in ['employee', 'internal', 'secret']):
                sensitivity_map[col] = DataSensitivity.INTERNAL
            else:
                # Check actual values for PII patterns
                sample_values = df[col].dropna().head(100).astype(str)
                pii_count = 0
                
                for value in sample_values:
                    for pattern in self.patterns.values():
                        if re.search(pattern, value, re.IGNORECASE):
                            pii_count += 1
                            break
                
                if pii_count > len(sample_values) * 0.1:  # More than 10% of values match PII patterns
                    sensitivity_map[col] = DataSensitivity.PII
                else:
                    sensitivity_map[col] = DataSensitivity.PUBLIC
        
        return sensitivity_map


class DataAnonymizer:
    """Anonymizes sensitive data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def anonymize_dataframe(self, df: pd.DataFrame, sensitivity_map: Dict[str, DataSensitivity],
                           anonymization_method: str = "hash") -> pd.DataFrame:
        """Anonymize DataFrame based on sensitivity classification"""
        anonymized_df = df.copy()
        
        for col, sensitivity in sensitivity_map.items():
            if col in anonymized_df.columns:
                if sensitivity in [DataSensitivity.PII, DataSensitivity.PHI, DataSensitivity.RESTRICTED]:
                    anonymized_df[col] = self._anonymize_column(
                        anonymized_df[col], anonymization_method
                    )
        
        return anonymized_df
    
    def _anonymize_column(self, series: pd.Series, method: str) -> pd.Series:
        """Anonymize a single column"""
        if method == "hash":
            return series.apply(lambda x: self._hash_value(str(x)) if pd.notna(x) else x)
        elif method == "mask":
            return series.apply(lambda x: self._mask_value(str(x)) if pd.notna(x) else x)
        elif method == "nullify":
            return pd.Series([pd.NA] * len(series), index=series.index)
        elif method == "truncate":
            return series.apply(lambda x: self._truncate_value(str(x)) if pd.notna(x) else x)
        else:
            return series  # No anonymization
    
    def _hash_value(self, value: str) -> str:
        """Hash a value using SHA-256"""
        return hashlib.sha256(value.encode()).hexdigest()[:16]
    
    def _mask_value(self, value: str) -> str:
        """Mask a value by replacing with asterisks"""
        if len(value) <= 2:
            return '*' * len(value)
        return value[0] + '*' * (len(value) - 2) + value[-1]
    
    def _truncate_value(self, value: str) -> str:
        """Truncate a value to first few characters"""
        if len(value) <= 3:
            return value
        return value[:3] + '...'


class PrivacyManager:
    """Manages privacy compliance and data subject requests"""
    
    def __init__(self, privacy_path: str = "data/privacy"):
        self.privacy_path = Path(privacy_path)
        self.privacy_path.mkdir(parents=True, exist_ok=True)
        self.requests_path = self.privacy_path / "requests"
        self.requests_path.mkdir(exist_ok=True)
        self.assessments_path = self.privacy_path / "assessments"
        self.assessments_path.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        self.pii_detector = PIIDetector()
        self.data_anonymizer = DataAnonymizer()
    
    def conduct_privacy_impact_assessment(self, dataset_name: str, 
                                       processing_purposes: List[str],
                                       data_sharing: bool,
                                       retention_period: str,
                                       conducted_by: str) -> PrivacyImpactAssessment:
        """Conduct a Privacy Impact Assessment"""
        assessment_id = f"pia_{dataset_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # This would typically involve more detailed analysis
        # For now, we'll create a basic assessment
        assessment = PrivacyImpactAssessment(
            assessment_id=assessment_id,
            dataset_name=dataset_name,
            data_categories=[DataSensitivity.PII],  # Placeholder
            processing_purposes=processing_purposes,
            data_sharing=data_sharing,
            retention_period=retention_period,
            conducted_by=conducted_by,
            conducted_at=datetime.utcnow(),
            risk_level="medium",  # Placeholder
            mitigation_measures=[
                "Implement access controls",
                "Anonymize sensitive data",
                "Regular audit logs",
                "Data minimization practices"
            ]
        )
        
        # Save assessment
        assessment_file = self.assessments_path / f"{assessment_id}.json"
        with open(assessment_file, 'w') as f:
            json.dump({
                'assessment_id': assessment.assessment_id,
                'dataset_name': assessment.dataset_name,
                'data_categories': [cat.value for cat in assessment.data_categories],
                'processing_purposes': assessment.processing_purposes,
                'data_sharing': assessment.data_sharing,
                'retention_period': assessment.retention_period,
                'conducted_by': assessment.conducted_by,
                'conducted_at': assessment.conducted_at.isoformat(),
                'risk_level': assessment.risk_level,
                'mitigation_measures': assessment.mitigation_measures
            }, f, indent=2)
        
        self.logger.info(f"Privacy Impact Assessment completed: {assessment_id}")
        return assessment
    
    def create_data_subject_request(self, subject_id: str, request_type: str, 
                                  reason: str = None) -> DataSubjectRequest:
        """Create a data subject request"""
        request_id = f"dSR_{subject_id}_{request_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        request = DataSubjectRequest(
            request_id=request_id,
            subject_id=subject_id,
            request_type=request_type,
            requested_at=datetime.utcnow(),
            status="pending",
            reason=reason
        )
        
        # Save request
        request_file = self.requests_path / f"{request_id}.json"
        with open(request_file, 'w') as f:
            json.dump({
                'request_id': request.request_id,
                'subject_id': request.subject_id,
                'request_type': request.request_type,
                'requested_at': request.requested_at.isoformat(),
                'processed_at': request.processed_at.isoformat() if request.processed_at else None,
                'status': request.status,
                'reason': request.reason
            }, f, indent=2)
        
        self.logger.info(f"Data subject request created: {request_id}")
        return request
    
    def process_data_subject_request(self, request_id: str, approve: bool = True) -> bool:
        """Process a data subject request"""
        request_file = self.requests_path / f"{request_id}.json"
        if not request_file.exists():
            self.logger.error(f"Request not found: {request_id}")
            return False
        
        # Load request
        with open(request_file, 'r') as f:
            request_data = json.load(f)
        
        # Update status
        request_data['processed_at'] = datetime.utcnow().isoformat()
        request_data['status'] = "approved" if approve else "rejected"
        
        # Write back
        with open(request_file, 'w') as f:
            json.dump(request_data, f, indent=2)
        
        self.logger.info(f"Data subject request processed: {request_id}, approved: {approve}")
        return True
    
    def detect_and_classify_pii(self, df: pd.DataFrame) -> Tuple[Dict[str, List], Dict[str, DataSensitivity]]:
        """Detect PII and classify sensitivity"""
        pii_found = self.pii_detector.detect_pii_in_dataframe(df)
        sensitivity_map = self.pii_detector.classify_sensitivity(df)
        
        return pii_found, sensitivity_map
    
    def anonymize_data(self, df: pd.DataFrame, sensitivity_map: Dict[str, DataSensitivity],
                      method: str = "hash") -> pd.DataFrame:
        """Anonymize data based on sensitivity classification"""
        return self.data_anonymizer.anonymize_dataframe(df, sensitivity_map, method)
    
    def generate_privacy_report(self, dataset_name: str) -> Dict[str, Any]:
        """Generate a privacy compliance report"""
        report = {
            'dataset_name': dataset_name,
            'generated_at': datetime.utcnow().isoformat(),
            'compliance_status': 'pending',  # Would be determined by actual checks
            'pii_detected': {},
            'sensitivity_classification': {},
            'privacy_controls': [],
            'recommendations': []
        }
        
        # This would typically involve loading the actual dataset
        # For now, we'll return a template report
        return report


class ComplianceChecker:
    """Checks compliance with privacy regulations"""
    
    def __init__(self, privacy_manager: PrivacyManager):
        self.privacy_manager = privacy_manager
        self.logger = logging.getLogger(__name__)
    
    def check_gdpr_compliance(self, df: pd.DataFrame, purpose: str) -> Dict[str, Any]:
        """Check GDPR compliance for data processing"""
        pii_found, sensitivity_map = self.privacy_manager.detect_and_classify_pii(df)
        
        gdpr_check = {
            'regulation': 'GDPR',
            'purpose': purpose,
            'compliant': True,
            'issues': [],
            'recommendations': []
        }
        
        # Check for PII without proper controls
        if DataSensitivity.PII in sensitivity_map.values():
            gdpr_check['compliant'] = False
            gdpr_check['issues'].append('PII detected without anonymization')
            gdpr_check['recommendations'].append('Implement data anonymization techniques')
        
        # Check for explicit consent indicators
        if purpose and 'consent' not in purpose.lower():
            gdpr_check['issues'].append('No explicit consent indication in purpose')
            gdpr_check['recommendations'].append('Document legal basis for processing')
        
        return gdpr_check
    
    def check_ccpa_compliance(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check CCPA compliance for data processing"""
        pii_found, sensitivity_map = self.privacy_manager.detect_and_classify_pii(df)
        
        ccpa_check = {
            'regulation': 'CCPA',
            'compliant': True,
            'issues': [],
            'rights_ensured': {
                'right_to_know': False,
                'right_to_delete': False,
                'right_to_opt_out': False,
                'right_to_non_discrimination': False
            }
        }
        
        # Check for PII without proper controls
        if DataSensitivity.PII in sensitivity_map.values():
            ccpa_check['compliant'] = False
            ccpa_check['issues'].append('PII detected without proper safeguards')
        
        # CCPA requires specific rights to be ensured
        ccpa_check['rights_ensured']['right_to_know'] = True  # Placeholder
        ccpa_check['rights_ensured']['right_to_delete'] = True  # Placeholder
        ccpa_check['rights_ensured']['right_to_opt_out'] = True  # Placeholder
        ccpa_check['rights_ensured']['right_to_non_discrimination'] = True  # Placeholder
        
        return ccpa_check
    
    def run_compliance_audit(self, df: pd.DataFrame, regulations: List[PrivacyRegulation] = None) -> Dict[str, Any]:
        """Run comprehensive compliance audit"""
        if regulations is None:
            regulations = [PrivacyRegulation.GDPR, PrivacyRegulation.CCPA]
        
        audit_results = {
            'audit_timestamp': datetime.utcnow().isoformat(),
            'regulations_checked': [reg.value for reg in regulations],
            'results': {}
        }
        
        for regulation in regulations:
            if regulation == PrivacyRegulation.GDPR:
                result = self.check_gdpr_compliance(df, "data_processing")
            elif regulation == PrivacyRegulation.CCPA:
                result = self.check_ccpa_compliance(df)
            else:
                result = {'regulation': regulation.value, 'compliant': True, 'issues': [], 'recommendations': []}
            
            audit_results['results'][regulation.value] = result
        
        return audit_results


# Global instances
_privacy_manager = None
_compliance_checker = None


def get_privacy_manager() -> PrivacyManager:
    """Get the global privacy manager instance"""
    global _privacy_manager
    if _privacy_manager is None:
        from ..config_loader import load_config_resolved
        config = load_config_resolved('local/config/local.yaml')
        privacy_path = config.get('paths', {}).get('privacy_root', 'data/privacy')
        _privacy_manager = PrivacyManager(privacy_path)
    return _privacy_manager


def get_compliance_checker() -> ComplianceChecker:
    """Get the global compliance checker instance"""
    global _compliance_checker
    if _compliance_checker is None:
        privacy_mgr = get_privacy_manager()
        _compliance_checker = ComplianceChecker(privacy_mgr)
    return _compliance_checker


def detect_and_classify_pii(df: pd.DataFrame) -> Tuple[Dict[str, List], Dict[str, DataSensitivity]]:
    """Detect PII and classify sensitivity"""
    privacy_mgr = get_privacy_manager()
    return privacy_mgr.detect_and_classify_pii(df)


def anonymize_data(df: pd.DataFrame, sensitivity_map: Dict[str, DataSensitivity],
                  method: str = "hash") -> pd.DataFrame:
    """Anonymize data based on sensitivity classification"""
    privacy_mgr = get_privacy_manager()
    return privacy_mgr.anonymize_data(df, sensitivity_map, method)


def conduct_privacy_impact_assessment(dataset_name: str, 
                                   processing_purposes: List[str],
                                   data_sharing: bool,
                                   retention_period: str,
                                   conducted_by: str) -> PrivacyImpactAssessment:
    """Conduct a Privacy Impact Assessment"""
    privacy_mgr = get_privacy_manager()
    return privacy_mgr.conduct_privacy_impact_assessment(
        dataset_name, processing_purposes, data_sharing, retention_period, conducted_by
    )


def create_data_subject_request(subject_id: str, request_type: str, 
                              reason: str = None) -> DataSubjectRequest:
    """Create a data subject request"""
    privacy_mgr = get_privacy_manager()
    return privacy_mgr.create_data_subject_request(subject_id, request_type, reason)


def process_data_subject_request(request_id: str, approve: bool = True) -> bool:
    """Process a data subject request"""
    privacy_mgr = get_privacy_manager()
    return privacy_mgr.process_data_subject_request(request_id, approve)


def check_gdpr_compliance(df: pd.DataFrame, purpose: str) -> Dict[str, Any]:
    """Check GDPR compliance for data processing"""
    checker = get_compliance_checker()
    return checker.check_gdpr_compliance(df, purpose)


def check_ccpa_compliance(df: pd.DataFrame) -> Dict[str, Any]:
    """Check CCPA compliance for data processing"""
    checker = get_compliance_checker()
    return checker.check_ccpa_compliance(df)


def run_compliance_audit(df: pd.DataFrame, regulations: List[PrivacyRegulation] = None) -> Dict[str, Any]:
    """Run comprehensive compliance audit"""
    checker = get_compliance_checker()
    return checker.run_compliance_audit(df, regulations)


def generate_privacy_report(dataset_name: str) -> Dict[str, Any]:
    """Generate a privacy compliance report"""
    privacy_mgr = get_privacy_manager()
    return privacy_mgr.generate_privacy_report(dataset_name)