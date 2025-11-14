# 🌟 Complete AWS Deployment Guide for Beginners

## 👋 Welcome!

This guide is designed for beginners who are deploying a data engineering project to AWS for the first time. I'll explain everything in simple terms and walk you through each step.

---

## 📚 Table of Contents

1. [What You'll Learn](#what-youll-learn)
2. [Prerequisites](#prerequisites)
3. [Understanding What We're Building](#understanding-what-were-building)
4. [Getting Started](#getting-started)
5. [Step 1: Setting Up Your Computer](#step-1-setting-up-your-computer)
6. [Step 2: Creating Your AWS Account](#step-2-creating-your-aws-account)
7. [Step 3: Preparing Your Project](#step-3-preparing-your-project)
8. [Step 4: Deploying to AWS](#step-4-deploying-to-aws)
9. [Step 5: Running Your First ETL Job](#step-5-running-your-first-etl-job)
10. [Step 6: Viewing Your Results](#step-6-viewing-your-results)
11. [Troubleshooting Common Issues](#troubleshooting-common-issues)
12. [Next Steps](#next-steps)

---

## 🎯 What You'll Learn

By the end of this guide, you'll be able to:
- ✅ Set up an AWS account
- ✅ Upload your data pipeline code to AWS
- ✅ Run data processing jobs in the cloud
- ✅ Store and query your data
- ✅ Monitor your pipeline

---

## 📋 Prerequisites

### What You Need Before Starting

**1. A Computer** (Mac, Windows, or Linux)
- We'll use Terminal/Command Prompt
- Don't worry if you're new to it - I'll guide you!

**2. Basic Skills**
- Can download and install software
- Comfortable following step-by-step instructions
- Willing to learn (that's the most important part!)

**3. An Email Address**
- To create an AWS account

**4. A Credit Card** (for AWS)
- AWS offers free tier for 12 months
- We'll stay in free tier to avoid charges
- But you need a card to sign up

**5. About 2-3 Hours**
- The first deployment takes time
- Subsequent deployments are much faster

---

## 🏗️ Understanding What We're Building

Let's understand what this project does in simple terms:

### The Big Picture

Think of your data pipeline like a factory:

```
📦 Raw Materials (Data Sources)
    ↓
🏭 Factory (AWS Processing)
    ↓
📊 Finished Products (Analytics & Reports)
```

### In Technical Terms

**1. Data Sources** (Where your data comes from)
- Like orders from a website
- Customer information
- Sales data

**2. Processing Layer** (AWS EMR Serverless)
- Cleans the data
- Removes errors
- Organizes it nicely

**3. Storage** (AWS S3)
- Stores your data in three levels:
  - **Bronze**: Raw, unprocessed data (like a backup)
  - **Silver**: Clean, organized data
  - **Gold**: Business-ready data for reports

**4. Analytics** (AWS Athena)
- Query your data
- Build dashboards
- Get insights

---

## 🚀 Getting Started

### Quick Overview

We'll follow these steps:
1. Install tools on your computer
2. Create AWS account
3. Upload project to AWS
4. Run first job
5. View results

Let's go!

---

## 💻 Step 1: Setting Up Your Computer

### 1.1 Install Python

**What is Python?**
Python is a programming language we use for data processing.

**Check if you have Python:**
```bash
python3 --version
```

**If you see something like "Python 3.10.0" or higher, you're good!**

**If not, install Python:**
- **Mac**: Download from https://www.python.org/downloads/
- **Windows**: Download from https://www.python.org/downloads/
- **Linux**: Run `sudo apt-get install python3 python3-pip`

### 1.2 Install Git

**What is Git?**
Git helps us manage our code.

**Check if you have Git:**
```bash
git --version
```

**If not, install Git:**
- **Mac**: Download from https://git-scm.com/download/mac
- **Windows**: Download from https://git-scm.com/download/win
- **Linux**: Run `sudo apt-get install git`

### 1.3 Install AWS CLI

**What is AWS CLI?**
It's a tool to control AWS from your computer.

**Install AWS CLI:**

**Mac:**
```bash
curl "https://awscli.amazonaws.com/awscli-exe-darwin-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

**Windows:**
Download MSI installer from https://awscli.amazonaws.com/AWSCLIV2.msi and run it.

**Linux:**
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

**Verify installation:**
```bash
aws --version
```
You should see: `aws-cli/2.x.x`

### 1.4 Install Terraform

**What is Terraform?**
It creates AWS resources (servers, storage, etc.) automatically.

**Install Terraform:**

**Mac:**
```bash
brew install terraform
```

**Windows/Linux:**
1. Go to https://www.terraform.io/downloads
2. Download for your OS
3. Extract and add to PATH

**Verify installation:**
```bash
terraform version
```

---

## ☁️ Step 2: Creating Your AWS Account

### 2.1 Sign Up for AWS

1. Go to https://aws.amazon.com/
2. Click "Create an AWS Account"
3. Enter your email and choose a password
4. Choose "Personal" account type
5. Enter your name and phone number
6. Enter payment information (don't worry, we'll use free tier!)
7. Complete identity verification (they'll call you)
8. Choose a support plan (select "Basic" - it's free)

### 2.2 Create IAM User

**Why?** For security, we don't use the root account for daily work.

**Steps:**
1. Sign into AWS Console
2. Search for "IAM" in the search bar
3. Click "IAM" service
4. Click "Users" in left menu
5. Click "Create user"
6. Username: `etl-admin`
7. ✅ Check "Provide user access to AWS Management Console"
8. Choose "Programmatic access"
9. Click "Next"
10. Click "Attach policies directly"
11. Search for and select:
    - ✅ AdministratorAccess
12. Click "Next"
13. Click "Create user"
14. **IMPORTANT**: Copy these values NOW:
    - Access Key ID
    - Secret Access Key
15. Save them in a safe place (Password Manager)

### 2.3 Configure AWS CLI on Your Computer

**This connects your computer to AWS:**

```bash
aws configure
```

**Enter when prompted:**
- AWS Access Key ID: [paste the Access Key ID from step 2.2]
- AWS Secret Access Key: [paste the Secret Access Key from step 2.2]
- Default region name: `us-east-1`
- Default output format: `json`

**Verify it worked:**
```bash
aws sts get-caller-identity
```

**You should see your account info!**

---

## 📁 Step 3: Preparing Your Project

### 3.1 Navigate to Project Directory

**Open Terminal/Command Prompt:**

```bash
# Navigate to your project
cd /path/to/pyspark_data_engineer_project

# Verify you're in the right place
ls
```

**You should see:**
```
README.md
config/
src/
aws/
jobs/
...
```

### 3.2 Set Up Python Environment

**Create a virtual environment (isolated Python space):**

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
# On Mac/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

**You should now see `(venv)` before your prompt!**

### 3.3 Install Project Dependencies

**These are the Python packages we need:**

```bash
# Install all required packages
pip install -r requirements.txt

# This might take 2-5 minutes
```

**What's happening?** Python is downloading all the libraries our project needs.

### 3.4 Test Local Installation

**Run a simple test:**

```bash
python -c "import pyspark; print('✅ PySpark installed successfully!')"
```

**If you see the checkmark, you're good!**

---

## 🚀 Step 4: Deploying to AWS

This is the exciting part! We'll use Terraform to create everything automatically.

### 4.1 Set Environment Variables

**These tell Terraform what to name your resources:**

```bash
# Get your AWS account ID
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Set other variables
export AWS_REGION=us-east-1
export PROJECT_NAME=pyspark-etl-learning
export ENVIRONMENT=dev

# Create bucket names (must be unique globally)
export DATA_LAKE_BUCKET="${PROJECT_NAME}-data-lake-${AWS_ACCOUNT_ID}"
export CODE_BUCKET="${PROJECT_NAME}-code-${AWS_ACCOUNT_ID}"

# Verify
echo "Account ID: ${AWS_ACCOUNT_ID}"
echo "Data Lake Bucket: ${DATA_LAKE_BUCKET}"
```

**Why unique bucket names?** S3 bucket names must be globally unique across all AWS accounts!

### 4.2 Navigate to Terraform Directory

```bash
cd aws/terraform
```

### 4.3 Create Terraform Variables File

**Create a file called `terraform.tfvars`:**

```bash
# Create the file
cat > terraform.tfvars << EOF
aws_region = "us-east-1"
project_name = "${PROJECT_NAME}"
environment = "dev"

# Bucket names
data_lake_bucket = "${DATA_LAKE_BUCKET}"
code_bucket = "${CODE_BUCKET}"
EOF
```

### 4.4 Initialize Terraform

**This downloads the AWS provider:**

```bash
terraform init
```

**Expected output:**
```
Initializing the backend...
Initializing provider plugins...
- Finding latest version of hashicorp/aws...
- Installing hashicorp/aws v5.x.x...
...

✅ Terraform has been successfully initialized!
```

### 4.5 Preview What Will Be Created

**This shows what Terraform wants to create WITHOUT actually creating it:**

```bash
terraform plan
```

**Expected output:**
```
Plan: X to add, 0 to change, 0 to destroy.

Changes:
  + aws_s3_bucket.data_lake
  + aws_s3_bucket.code
  + aws_iam_role.emr_serverless_role
  ...

Note: You didn't use the -out option...
```

**Review the list - does it look reasonable?**
- S3 buckets: ✅
- IAM roles: ✅
- EMR application: ✅

### 4.6 Apply Terraform (Create AWS Resources)

**THIS IS THE BIG STEP! This actually creates resources:**

```bash
terraform apply
```

**Terraform will ask:**
```
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value:
```

**Type:** `yes` and press Enter

**What happens next:**
- Takes 5-10 minutes
- You'll see progress
- Terraform creates resources one by one

**When it's done, you'll see:**
```
Apply complete! Resources: X added, 0 changed, 0 destroyed.

Outputs:
data_lake_bucket = "pyspark-etl-learning-data-lake-123456789012"
code_bucket = "pyspark-etl-learning-code-123456789012"
emr_application_id = "00f1234567890abc"
```

**🎉 Congratulations! You just created your AWS infrastructure!**

### 4.7 Upload Your Code to S3

**Now we upload our Python code:**

```bash
# Go back to project root
cd ../../

# Create a zip file of your code
zip -r code.zip src/ config/ jobs/ requirements.txt -x "*.pyc" "__pycache__/*" "*.git/*"

# Upload to S3
aws s3 cp code.zip s3://${CODE_BUCKET}/etl-code.zip

# Also upload sample data
aws s3 sync data/samples/ s3://${DATA_LAKE_BUCKET}/bronze/samples/
```

---

## 🏃 Step 5: Running Your First ETL Job

Now for the fun part - running your data pipeline!

### 5.1 Create EMR Serverless Job

**First, let's create a simple test job:**

```bash
aws emr-serverless create-application \
  --name "pyspark-etl-test" \
  --type "SPARK" \
  --release-label "emr-6.15.0" \
  --region us-east-1
```

**Save the Application ID from the output!**

### 5.2 Submit Your First Job

**Create a job configuration file:**

```bash
cat > job-config.json << EOF
{
  "applicationId": "YOUR_APPLICATION_ID_HERE",
  "executionRoleArn": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/EMRServerlessExecutionRole",
  "jobDriver": {
    "sparkSubmit": {
      "entryPoint": "s3://${CODE_BUCKET}/etl-code.zip",
      "entryPointArguments": ["--config", "local.yaml"],
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }
  },
  "configurationOverrides": {
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://${DATA_LAKE_BUCKET}/logs/"
      }
    }
  }
}
EOF
```

**Replace `YOUR_APPLICATION_ID_HERE` with your actual Application ID!**

### 5.3 Start the Job

```bash
aws emr-serverless start-job-run \
  --cli-input-json file://job-config.json \
  --region us-east-1
```

**Save the Job Run ID from the output!**

### 5.4 Monitor Job Status

**Check if your job is running:**

```bash
# Get the job run ID from previous step
JOB_RUN_ID="your-job-run-id"

# Check status
aws emr-serverless get-job-run \
  --application-id YOUR_APPLICATION_ID \
  --job-run-id $JOB_RUN_ID \
  --region us-east-1
```

**Wait for status to be:**
- ✅ COMPLETED - Success!
- ❌ FAILED - Check logs
- ⏳ RUNNING - Still processing

**Check every 30 seconds:**
```bash
# On Mac/Linux
watch -n 30 aws emr-serverless get-job-run \
  --application-id YOUR_APPLICATION_ID \
  --job-run-id $JOB_RUN_ID
```

---

## 📊 Step 6: Viewing Your Results

Your job is done! Let's see what we created.

### 6.1 Check Your Data in S3

**List what was created:**

```bash
# List bronze layer
aws s3 ls s3://${DATA_LAKE_BUCKET}/bronze/ --recursive

# List silver layer
aws s3 ls s3://${DATA_LAKE_BUCKET}/silver/ --recursive

# List gold layer
aws s3 ls s3://${DATA_LAKE_BUCKET}/gold/ --recursive
```

**You should see Delta Lake tables!**

### 6.2 Query with AWS Athena

**Athena lets you query your data with SQL:**

**In AWS Console:**
1. Search for "Athena"
2. Click "Query editor"
3. Click "Create database"
4. Database name: `etl_project`
5. Click "Create database"

**Run your first query:**

```sql
-- Create a table pointing to your data
CREATE EXTERNAL TABLE bronze_orders
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/bronze/orders/';

-- Query your data
SELECT * FROM bronze_orders LIMIT 10;
```

---

## 🔧 Troubleshooting Common Issues

### Issue 1: "Access Denied"

**Problem:** AWS says you don't have permission.

**Solution:**
```bash
# Check your AWS identity
aws sts get-caller-identity

# Verify IAM permissions
aws iam get-user
```

### Issue 2: "Bucket name already taken"

**Problem:** S3 bucket name must be globally unique.

**Solution:**
```bash
# Use a more unique name
export PROJECT_NAME="pyspark-etl-${USER}-$(date +%s)"
echo "New project name: ${PROJECT_NAME}"
```

### Issue 3: "Terraform plan fails"

**Problem:** Terraform can't connect to AWS.

**Solution:**
```bash
# Test AWS connection
aws sts get-caller-identity

# If that fails, reconfigure AWS CLI
aws configure
```

### Issue 4: "Job keeps failing"

**Problem:** EMR Serverless job fails to start.

**Solution:**
```bash
# Check job logs
aws s3 cp s3://${DATA_LAKE_BUCKET}/logs/APPLICATION_ID/JOB_RUN_ID/spark-logs/ . --recursive

# Look for errors
tail -100 driver/stdout.gz
```

### Issue 5: "Out of memory errors"

**Problem:** Job needs more memory.

**Solution:**
```bash
# In job config, increase memory:
"sparkSubmitParameters": "--conf spark.executor.memory=8g --conf spark.executor.cores=2"
```

---

## 🎓 Next Steps

Congratulations! You've successfully deployed your data pipeline to AWS. Here's what to learn next:

### Learn More About
1. **AWS Services**
   - EMR Serverless: https://docs.aws.amazon.com/emr/
   - S3: https://docs.aws.amazon.com/s3/
   - Athena: https://docs.aws.amazon.com/athena/

2. **PySpark**
   - Official guide: https://spark.apache.org/docs/latest/
   - Python API: https://spark.apache.org/docs/latest/api/python/

3. **Terraform**
   - Tutorials: https://learn.hashicorp.com/terraform

### Common Next Tasks
- ✅ Run jobs on a schedule (use AWS EventBridge)
- ✅ Add monitoring (use AWS CloudWatch)
- ✅ Add data quality checks
- ✅ Create more data sources
- ✅ Build dashboards (use Amazon QuickSight)

---

## 💰 Cost Considerations

### Free Tier
AWS provides a free tier for 12 months:
- 750 hours of EMR Serverless per month
- 5 GB of S3 storage
- Most of our test jobs fit within this!

### Estimated Costs (After Free Tier)
**For this project:**
- EMR Serverless: ~$5-10 per job run
- S3 Storage: ~$0.023 per GB/month
- Athena Queries: ~$5 per TB scanned

**Total for light use:** ~$20-50/month

### To Keep Costs Low
1. ✅ Use spot instances (60-90% cheaper)
2. ✅ Delete old data regularly
3. ✅ Use Athena for queries (no infrastructure cost)
4. ✅ Stop EMR applications when not in use

---

## 🆘 Getting Help

**If you get stuck:**

1. **Check AWS Documentation**
   - Search for your error message
   - AWS has excellent docs!

2. **Stack Overflow**
   - Search your error
   - Someone probably had the same issue!

3. **AWS Support**
   - For production issues
   - (Paid support plans)

4. **This Project's README**
   - Check README.md for project-specific help

---

## 🎉 Congratulations!

You've completed your first AWS deployment! 

You now know how to:
- ✅ Set up AWS environment
- ✅ Deploy infrastructure with Terraform
- ✅ Run data processing jobs
- ✅ Query results with Athena
- ✅ Troubleshoot issues

**What's Next?**
- Try modifying the pipeline
- Add new data sources
- Create dashboards
- Schedule jobs to run automatically

**Remember:** Every expert was once a beginner. Keep learning! 🚀

---

## 📝 Quick Reference

**Most Common Commands:**

```bash
# Check AWS connection
aws sts get-caller-identity

# Deploy infrastructure
cd aws/terraform && terraform apply

# Upload code
aws s3 cp code.zip s3://BUCKET/

# Check job status
aws emr-serverless get-job-run --application-id APP_ID --job-run-id JOB_ID

# View logs
aws s3 cp s3://BUCKET/logs/ . --recursive

# List data
aws s3 ls s3://BUCKET/ --recursive
```

**Need this file again?**
Just open: `BEGINNERS_AWS_DEPLOYMENT_GUIDE.md`

---

**Last Updated:** 2025-01-15  
**Version:** 1.0  
**Project:** PySpark Data Engineering

Good luck with your deployment! 🎉

