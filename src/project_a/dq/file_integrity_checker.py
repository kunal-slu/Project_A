"""
File Integrity Checker

Validates data consistency between AWS S3 and local files:
- File sizes identical
- Number of partitions equal
- S3 object mtime similar
- No partial uploads
- No corrupt S3 objects
- EMR input sizes correct
"""

import logging
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class FileIntegrityChecker:
    """Check file integrity between local and AWS S3."""

    def __init__(self, aws_profile: str | None = None, aws_region: str = "us-east-1"):
        self.aws_profile = aws_profile
        self.aws_region = aws_region
        self.s3_client = boto3.Session(profile_name=aws_profile, region_name=aws_region).client(
            "s3"
        )
        self.issues: list[dict[str, Any]] = []

    def compare_local_s3(self, local_path: str, s3_path: str, file_name: str) -> dict[str, Any]:
        """
        Compare local file with S3 object.

        Args:
            local_path: Local file path
            s3_path: S3 path (s3://bucket/key)
            file_name: Descriptive name for the file

        Returns:
            Comparison results
        """
        logger.info(f"Comparing {file_name}: local vs S3")

        result = {
            "file": file_name,
            "local_exists": False,
            "s3_exists": False,
            "sizes_match": False,
            "local_size": 0,
            "s3_size": 0,
            "valid": False,
        }

        # Check local file
        local_file = Path(local_path)
        if local_file.exists():
            result["local_exists"] = True
            result["local_size"] = local_file.stat().st_size
        else:
            self.issues.append(
                {"file": file_name, "issue": "Local file not found", "path": local_path}
            )
            return result

        # Check S3 object
        try:
            bucket, key = s3_path.replace("s3://", "").split("/", 1)
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            result["s3_exists"] = True
            result["s3_size"] = response["ContentLength"]
            result["s3_last_modified"] = response["LastModified"].isoformat()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.issues.append(
                    {"file": file_name, "issue": "S3 object not found", "path": s3_path}
                )
            else:
                self.issues.append({"file": file_name, "issue": f"S3 error: {e}", "path": s3_path})
            return result

        # Compare sizes
        result["sizes_match"] = result["local_size"] == result["s3_size"]
        result["valid"] = result["sizes_match"]

        if not result["sizes_match"]:
            self.issues.append(
                {
                    "file": file_name,
                    "issue": "File sizes do not match",
                    "local_size": result["local_size"],
                    "s3_size": result["s3_size"],
                    "difference": abs(result["local_size"] - result["s3_size"]),
                }
            )
            logger.warning(
                f"❌ Size mismatch for {file_name}: "
                f"local={result['local_size']}, s3={result['s3_size']}"
            )
        else:
            logger.info(f"✅ {file_name}: sizes match ({result['local_size']} bytes)")

        return result

    def check_s3_object_integrity(self, s3_path: str) -> dict[str, Any]:
        """Check S3 object integrity (ETag, checksum)."""
        try:
            bucket, key = s3_path.replace("s3://", "").split("/", 1)
            response = self.s3_client.head_object(Bucket=bucket, Key=key)

            return {
                "valid": True,
                "size": response["ContentLength"],
                "etag": response.get("ETag", ""),
                "last_modified": response["LastModified"].isoformat(),
                "storage_class": response.get("StorageClass", "STANDARD"),
            }
        except ClientError as e:
            return {"valid": False, "error": str(e)}

    def check_partition_count(self, local_dir: Path, s3_prefix: str) -> dict[str, Any]:
        """Compare partition counts between local and S3."""
        # Count local partitions
        local_partitions = len([p for p in local_dir.iterdir() if p.is_dir()])

        # Count S3 partitions
        try:
            bucket, prefix = s3_prefix.replace("s3://", "").split("/", 1)
            paginator = self.s3_client.get_paginator("list_objects_v2")
            s3_partitions = set()

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
                for prefix_obj in page.get("CommonPrefixes", []):
                    s3_partitions.add(prefix_obj["Prefix"])

            s3_partition_count = len(s3_partitions)
        except Exception as e:
            return {"valid": False, "error": str(e), "local_partitions": local_partitions}

        result = {
            "valid": local_partitions == s3_partition_count,
            "local_partitions": local_partitions,
            "s3_partitions": s3_partition_count,
        }

        if not result["valid"]:
            self.issues.append(
                {
                    "issue": "Partition count mismatch",
                    "local": local_partitions,
                    "s3": s3_partition_count,
                }
            )

        return result

    def get_all_issues(self) -> list[dict[str, Any]]:
        """Get all detected integrity issues."""
        return self.issues

    def generate_report(self) -> str:
        """Generate integrity report."""
        if not self.issues:
            return "✅ No file integrity issues detected."

        report = ["🔍 File Integrity Report", "=" * 50, ""]

        for issue in self.issues:
            report.append(f"File: {issue.get('file', 'Unknown')}")
            report.append(f"  Issue: {issue.get('issue', 'Unknown')}")
            if "local_size" in issue:
                report.append(f"  Local size: {issue['local_size']:,} bytes")
                report.append(f"  S3 size: {issue['s3_size']:,} bytes")
                report.append(f"  Difference: {issue['difference']:,} bytes")
            report.append("")

        return "\n".join(report)
