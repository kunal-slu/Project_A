"""
Change Data Capture (CDC) System for Project_A

Implements incremental processing with watermarking and change tracking.
"""
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import logging
import pandas as pd
from enum import Enum


class ChangeType(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    UPSERT = "UPSERT"


@dataclass
class ChangeRecord:
    """Represents a single change record"""
    change_id: str
    table_name: str
    change_type: ChangeType
    primary_key: str
    old_values: Optional[Dict[str, Any]]
    new_values: Optional[Dict[str, Any]]
    timestamp: datetime
    source: str
    partition_key: Optional[str] = None


class WatermarkManager:
    """Manages watermarks for incremental processing"""
    
    def __init__(self, watermark_path: str = "data/watermarks"):
        self.watermark_path = Path(watermark_path)
        self.watermark_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def get_watermark(self, source: str, table: str) -> Optional[datetime]:
        """Get the current watermark for a source and table"""
        watermark_file = self.watermark_path / f"{source}_{table}_watermark.json"
        
        if watermark_file.exists():
            with open(watermark_file, 'r') as f:
                data = json.load(f)
                return datetime.fromisoformat(data['watermark'])
        return None
    
    def set_watermark(self, source: str, table: str, watermark: datetime):
        """Set the watermark for a source and table"""
        watermark_file = self.watermark_path / f"{source}_{table}_watermark.json"
        
        with open(watermark_file, 'w') as f:
            json.dump({
                'source': source,
                'table': table,
                'watermark': watermark.isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }, f, indent=2)
    
    def update_watermark(self, source: str, table: str, new_watermark: datetime):
        """Update watermark to newer value if applicable"""
        current_watermark = self.get_watermark(source, table)
        
        if current_watermark is None or new_watermark > current_watermark:
            self.set_watermark(source, table, new_watermark)
            self.logger.info(f"Watermark updated for {source}.{table}: {new_watermark}")
        else:
            self.logger.debug(f"Watermark not updated for {source}.{table}, new watermark is older")


class ChangeCaptureBuffer:
    """Buffer for capturing and storing change records"""
    
    def __init__(self, buffer_path: str = "data/changes"):
        self.buffer_path = Path(buffer_path)
        self.buffer_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def append_change(self, change_record: ChangeRecord):
        """Append a change record to the buffer"""
        # Create partition directory based on date
        date_partition = change_record.timestamp.strftime('%Y/%m/%d')
        partition_path = self.buffer_path / date_partition
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Create filename based on table and time
        time_partition = change_record.timestamp.strftime('%H')
        filename = f"changes_{change_record.table_name}_{time_partition}.jsonl"
        filepath = partition_path / filename
        
        # Append the change record
        with open(filepath, 'a') as f:
            f.write(json.dumps({
                'change_id': change_record.change_id,
                'table_name': change_record.table_name,
                'change_type': change_record.change_type.value,
                'primary_key': change_record.primary_key,
                'old_values': change_record.old_values,
                'new_values': change_record.new_values,
                'timestamp': change_record.timestamp.isoformat(),
                'source': change_record.source,
                'partition_key': change_record.partition_key
            }) + '\n')
    
    def get_changes_since(self, table_name: str, since: datetime, 
                         change_types: List[ChangeType] = None) -> List[ChangeRecord]:
        """Get changes for a table since a specific time"""
        changes = []
        
        # Look in all date partitions since the given time
        start_date = since.date()
        current_date = datetime.utcnow().date()
        
        while start_date <= current_date:
            date_partition = start_date.strftime('%Y/%m/%d')
            partition_path = self.buffer_path / date_partition
            
            if partition_path.exists():
                # Check all hourly files for this date
                for hour_file in partition_path.glob(f"changes_{table_name}_*.jsonl"):
                    with open(hour_file, 'r') as f:
                        for line in f:
                            try:
                                record_data = json.loads(line.strip())
                                record_time = datetime.fromisoformat(record_data['timestamp'])
                                
                                if record_time >= since:
                                    # Filter by change type if specified
                                    if change_types is None or ChangeType(record_data['change_type']) in change_types:
                                        change_record = ChangeRecord(
                                            change_id=record_data['change_id'],
                                            table_name=record_data['table_name'],
                                            change_type=ChangeType(record_data['change_type']),
                                            primary_key=record_data['primary_key'],
                                            old_values=record_data['old_values'],
                                            new_values=record_data['new_values'],
                                            timestamp=record_time,
                                            source=record_data['source'],
                                            partition_key=record_data.get('partition_key')
                                        )
                                        changes.append(change_record)
                            except json.JSONDecodeError:
                                continue  # Skip malformed lines
            
            start_date += timedelta(days=1)
        
        # Sort changes by timestamp
        changes.sort(key=lambda x: x.timestamp)
        return changes
    
    def compact_changes(self, days_to_keep: int = 30):
        """Compact old change records to save space"""
        cutoff_date = datetime.utcnow().date() - timedelta(days=days_to_keep)
        
        for date_dir in self.buffer_path.iterdir():
            if date_dir.is_dir():
                date_part = datetime.strptime(date_dir.name, '%Y/%m/%d').date()
                if date_part < cutoff_date:
                    # Create a compacted file for this day
                    compacted_file = self.buffer_path / f"compacted_{date_dir.name}.jsonl"
                    
                    with open(compacted_file, 'w') as compacted_f:
                        for hour_file in date_dir.glob("*.jsonl"):
                            with open(hour_file, 'r') as f:
                                for line in f:
                                    compacted_f.write(line)
                    
                    # Remove the original date directory
                    import shutil
                    shutil.rmtree(date_dir)
                    
                    self.logger.info(f"Compacted changes for {date_dir.name}")


class IncrementalProcessor:
    """Processes data incrementally using CDC and watermarks"""
    
    def __init__(self, watermark_manager: WatermarkManager, 
                 change_buffer: ChangeCaptureBuffer):
        self.watermark_manager = watermark_manager
        self.change_buffer = change_buffer
        self.logger = logging.getLogger(__name__)
    
    def detect_changes(self, source_df: pd.DataFrame, table_name: str, 
                      primary_key: str, timestamp_column: str = None) -> List[ChangeRecord]:
        """Detect changes between current data and previous snapshot"""
        changes = []
        current_watermark = self.watermark_manager.get_watermark('source', table_name)
        
        # Identify new, updated, and deleted records
        for idx, row in source_df.iterrows():
            row_dict = row.to_dict()
            
            # Create change record based on timestamp or other logic
            change_time = datetime.utcnow()
            if timestamp_column and pd.notna(row[timestamp_column]):
                change_time = pd.to_datetime(row[timestamp_column])
            
            # For now, treat all records as inserts if no watermark exists
            # In a real system, you'd compare with previous snapshot
            change_record = ChangeRecord(
                change_id=hashlib.md5(f"{table_name}_{row[primary_key]}_{change_time}".encode()).hexdigest(),
                table_name=table_name,
                change_type=ChangeType.INSERT if current_watermark is None else ChangeType.UPSERT,
                primary_key=str(row[primary_key]),
                old_values=None,  # Would come from previous snapshot
                new_values=row_dict,
                timestamp=change_time,
                source='source_system'
            )
            
            changes.append(change_record)
        
        return changes
    
    def process_incremental(self, source_df: pd.DataFrame, table_name: str, 
                           primary_key: str, timestamp_column: str = None) -> Dict[str, Any]:
        """Process incremental changes and update watermark"""
        # Detect changes
        changes = self.detect_changes(source_df, table_name, primary_key, timestamp_column)
        
        # Store changes
        for change in changes:
            self.change_buffer.append_change(change)
        
        # Update watermark to the latest change timestamp
        if changes:
            latest_timestamp = max(change.timestamp for change in changes)
            self.watermark_manager.update_watermark('source', table_name, latest_timestamp)
        
        # Return summary
        change_counts = {
            ChangeType.INSERT.value: 0,
            ChangeType.UPDATE.value: 0,
            ChangeType.DELETE.value: 0,
            ChangeType.UPSERT.value: 0
        }
        
        for change in changes:
            change_counts[change.change_type.value] += 1
        
        result = {
            'table_name': table_name,
            'changes_detected': len(changes),
            'change_breakdown': change_counts,
            'latest_watermark': latest_timestamp.isoformat() if changes else None,
            'processed_at': datetime.utcnow().isoformat()
        }
        
        self.logger.info(f"Incremental processing completed for {table_name}: {result}")
        return result
    
    def get_incremental_data(self, table_name: str, since: datetime = None, 
                           limit: int = None) -> pd.DataFrame:
        """Get incremental data since last watermark or specific time"""
        if since is None:
            since = self.watermark_manager.get_watermark('source', table_name)
            if since is None:
                since = datetime.min
        
        changes = self.change_buffer.get_changes_since(table_name, since)
        
        # Convert changes to DataFrame
        if not changes:
            return pd.DataFrame()
        
        # For simplicity, returning new values as DataFrame
        # In a real system, you'd apply the changes to existing data
        records = []
        for change in changes[:limit] if limit else changes:
            if change.new_values:
                records.append(change.new_values)
        
        if records:
            df = pd.DataFrame(records)
            return df.drop_duplicates(subset=[change.primary_key for change in changes[:1]], keep='last')
        
        return pd.DataFrame()


# Global instances
_watermark_manager = None
_change_buffer = None
_incremental_processor = None


def get_watermark_manager() -> WatermarkManager:
    """Get the global watermark manager instance"""
    global _watermark_manager
    if _watermark_manager is None:
        from ..config_loader import load_config_resolved
        config = load_config_resolved('local/config/local.yaml')
        watermark_path = config.get('paths', {}).get('watermarks_root', 'data/watermarks')
        _watermark_manager = WatermarkManager(watermark_path)
    return _watermark_manager


def get_change_buffer() -> ChangeCaptureBuffer:
    """Get the global change buffer instance"""
    global _change_buffer
    if _change_buffer is None:
        from ..config_loader import load_config_resolved
        config = load_config_resolved('local/config/local.yaml')
        buffer_path = config.get('paths', {}).get('changes_root', 'data/changes')
        _change_buffer = ChangeCaptureBuffer(buffer_path)
    return _change_buffer


def get_incremental_processor() -> IncrementalProcessor:
    """Get the global incremental processor instance"""
    global _incremental_processor
    if _incremental_processor is None:
        watermark_mgr = get_watermark_manager()
        change_buf = get_change_buffer()
        _incremental_processor = IncrementalProcessor(watermark_mgr, change_buf)
    return _incremental_processor


def process_incremental_data(source_df: pd.DataFrame, table_name: str, 
                           primary_key: str, timestamp_column: str = None) -> Dict[str, Any]:
    """Process incremental data changes"""
    processor = get_incremental_processor()
    return processor.process_incremental(source_df, table_name, primary_key, timestamp_column)


def get_incremental_data(table_name: str, since: datetime = None, 
                        limit: int = None) -> pd.DataFrame:
    """Get incremental data since last watermark"""
    processor = get_incremental_processor()
    return processor.get_incremental_data(table_name, since, limit)


def get_watermark(source: str, table: str) -> Optional[datetime]:
    """Get the current watermark for a source and table"""
    watermark_mgr = get_watermark_manager()
    return watermark_mgr.get_watermark(source, table)


def set_watermark(source: str, table: str, watermark: datetime):
    """Set the watermark for a source and table"""
    watermark_mgr = get_watermark_manager()
    watermark_mgr.set_watermark(source, table, watermark)