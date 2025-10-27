# AWS Production Runbook 2025

## On-Call Procedures

### Job Failures

#### EMR Serverless Job Fails

1. **Check CloudWatch Logs**
   ```bash
   aws logs tail /aws/emr-serverless/spark --follow
   ```

2. **Common Issues**:
   - Memory: Increase `spark.executor.memory`
   - Timeout: Increase job timeout in config
   - Credentials: Check Secrets Manager

3. **Restart Job**
   - If transient, restart via EMR Console
   - If persistent, check upstream data source

#### Data Quality Failure

1. **Identify Failed Table**
   ```bash
   aws logs filter-log-events \
     --log-group-name /aws/data-quality/failures \
     --start-time $(date -u -d '1 hour ago' +%s)000
   ```

2. **Check Expectations**
   - Review `src/pyspark_interview_project/dq/suites/*.yml`
   - Verify if thresholds need adjustment

3. **Investigate Root Cause**
   - Check upstream data source
   - Review recent schema changes
   - Check data freshness

4. **Fix and Re-run**
   - Fix data at source or adjust expectations
   - Manually trigger DQ check
   - Resume downstream jobs

### Streaming Issues

#### Kafka Lag

1. **Check Consumer Lag**
   ```bash
   aws kinesis describe-stream --stream-name orders_stream
   ```

2. **If Lag Detected**
   - Scale up stream processing
   - Check downstream bottlenecks
   - Review checkpoint location

3. **Recovery**
   - Resume from last checkpoint
   - Replay if necessary
   - Verify data integrity

### Performance Issues

#### Slow Jobs

1. **Identify Bottleneck**
   - Check Spark UI: `http://<emr-app>:18080`
   - Review CloudWatch metrics
   - Check data volume growth

2. **Optimize**
   - Partition data properly
   - Adjust Spark configs
   - Consider caching intermediate results

3. **Scale**
   - Add more executor memory/cores
   - Use EMR Serverless auto-scaling

## Escalation

### When to Escalate
- Data loss detected
- Multi-system outage
- SLA breach imminent
- Security incident

### Escalation Path
1. On-call engineer (first 30 minutes)
2. Data engineering lead (+1 hour)
3. Head of engineering (+2 hours)

### Emergency Contacts
- Data Team Pager: (555) 123-4567
- On-call Slack: #datops-alerts
- Runbook: docs/runbooks/

## Maintenance

### Weekly
- Review DQ pass rates
- Check Delta Lake vacuum/optimize
- Verify lineage accuracy

### Monthly
- Delta Lake maintenance
- Performance tuning review
- Cost optimization
- Runbook updates

## Appendix

### Useful Commands
```bash
# Check Delta table history
DESCRIBE HISTORY bronze.hubspot_contacts;

# Vacuum old files
VACUUM bronze.hubspot_contacts RETAIN 168 HOURS;

# Optimize table
OPTIMIZE bronze.hubspot_contacts;
```

### Reference Links
- [AWS Runbook](docs/runbooks/RUNBOOK_AWS_2025.md)
- [DQ Failover](docs/runbooks/RUNBOOK_DQ_FAILOVER.md)
- [Streaming Recovery](docs/runbooks/RUNBOOK_STREAMING_RECOVERY.md)
