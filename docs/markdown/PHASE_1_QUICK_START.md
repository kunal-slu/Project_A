# 🚀 Phase 1: Quick Start Guide

## ⚡ Fast Track (5 minutes)

```bash
# 1. Navigate to Terraform directory
cd ~/IdeaProjects/pyspark_data_engineer_project/aws/terraform

# 2. Initialize (first time only)
terraform init

# 3. Validate
terraform validate

# 4. Plan (preview)
terraform plan -var-file=env/dev.tfvars

# 5. Apply (create resources)
terraform apply -var-file=env/dev.tfvars -auto-approve

# 6. Export outputs
terraform output -json > terraform-outputs.dev.json

# 7. Verify
cd ../..
./aws/scripts/verify_phase1.sh
```

## ✅ Success Criteria

After running the commands above, you should have:

- ✅ `terraform-outputs.dev.json` file created
- ✅ S3 buckets visible in AWS Console
- ✅ EMR Serverless app created
- ✅ Glue databases created
- ✅ IAM roles created

## 📋 Detailed Guide

For step-by-step instructions with explanations, see:
- `PHASE_1_COMPLETE_GUIDE.md` - Full detailed guide
- `AWS_VERIFICATION_CHECKLIST.md` - What to check in AWS Console

## 🆘 Need Help?

If any step fails:
1. Check error message
2. See troubleshooting section in `PHASE_1_COMPLETE_GUIDE.md`
3. Run verification script: `./aws/scripts/verify_phase1.sh`
