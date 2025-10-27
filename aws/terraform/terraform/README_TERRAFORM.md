# Terraform Infrastructure

## Overview
This directory contains the complete infrastructure as code for deploying the data platform on AWS.

## Files

- `main.tf` - Core resources (S3, EMR Serverless, MWAA)
- `iam.tf` - IAM roles and policies with least privilege
- `secrets.tf` - Secrets Manager for credentials
- `networking.tf` - VPC, subnets, security groups
- `cloudwatch.tf` - Log groups and alarms
- `glue_catalog.tf` - Glue databases for Bronze/Silver/Gold
- `variables.tf` - Input variables
- `outputs.tf` - Output values
- `terraform.tfvars` - Environment-specific values

## Usage

### Initialize
```bash
cd aws/infra/terraform
terraform init
```

### Plan
```bash
terraform plan -var-file=terraform.tfvars.dev
```

### Apply
```bash
terraform apply -var-file=terraform.tfvars.dev
```

### Destroy
```bash
terraform destroy -var-file=terraform.tfvars.dev
```

## Environments

- `terraform.tfvars.dev` - Development
- `terraform.tfvars.staging` - Staging
- `terraform.tfvars.prod` - Production

## Best Practices

1. Always use `-var-file` to specify environment
2. Review plan before apply
3. Use workspace for isolation if needed
4. Store state in S3 backend (configure backend.tf)

