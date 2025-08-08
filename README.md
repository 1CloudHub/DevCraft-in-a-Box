# TextOps CDK Deployment Guide

## Overview

**TextOps** is an AI-powered OCR (Optical Character Recognition) application designed to streamline document processing workflows. It leverages AWS services to extract, analyze, and organize textual data from structured and unstructured documents.

> **Disclaimer**: This CDK setup is strictly configured for deployment in the `us-west-2` (Oregon) AWS region. Deploying to other regions may result in errors or incompatibility.

---

## Prerequisites

Before deploying TextOps, ensure the following:

* You have access to the target AWS account.
* You are working in the **`us-west-2`** region.
* You have a valid GitHub Personal Access Token (PAT) for accessing the source repository.

---

## Deployment Steps

### 1. Login to the AWS Console

Log in to the designated AWS account using IAM credentials or SSO as instructed.

### 2. Set the Region

In the AWS Console, select **US West (Oregon) – `us-west-2`** from the region dropdown.

> This region is required for all services and integrations in the CDK stack.

![Region Navigation](./assets/region-navigation.png)

---

### 3. Launch AWS CloudShell

From the AWS Console, open the **CloudShell** environment.

> CloudShell includes all required tools pre-installed and avoids local configuration overhead.

![Cloudshell Navigation](./assets/cloudshell-navigatioin.png)

---

### 4. Clone the CDK Repository

Replace `{branch_name}` and `{pat_token}` with actual values.

```bash
git clone --branch {branch_name} https://{pat_token}@github.com/Hakash1CH/CEXP_CDK.git
```

> Clones the specified branch of the TextOps CDK infrastructure repository.

---

### 5. Export GitHub Token

```bash
export GITHUB_TOKEN={pat_token}
```

> Exports the GitHub token as an environment variable for private repository access.

---

### 6. Install AWS CDK CLI

```bash
sudo npm install -g aws-cdk
```

> Installs the AWS CDK CLI globally in the CloudShell session.

---

### 7. Install Python Dependencies

```bash
cd CEXP_CDK
pip install --user -r requirements.txt
```

> Installs the Python dependencies required by the CDK app.

---

### 8. Bootstrap the CDK Environment

```bash
cdk bootstrap
```

> Sets up initial infrastructure required for deploying CDK stacks.

---

### 9. Deploy the CDK Stack

```bash
cdk deploy
```

> Provisions the necessary AWS resources to run the TextOps application.

---

## Post Deployment Steps

### 10. Set Up Textract and Bedrock (Optional)

If your deployment includes AI model integrations:

* Navigate to **Amazon Bedrock** > **Model Access**
* Request access to relevant models like:

  ```
  - Claude 3.5 Sonnet V2
  ```

> Approval might take a few minutes depending on your AWS account.

![Model Access Navigation](./assets/model-access-navigation.png)
![Model Access](./assets/enable-model.png)
![Model Access](./assets/request-for-access.png)
![Bedrock Confirmation](./assets/bedrock-confirmation-page.png)

---

### 11. Retrieve the Application URL

Go to the **CloudFront** service:

* Locate the distribution created by the deployment.
* Copy the **Domain Name** under general settings.

> This domain will serve as your live application URL.

![Cloudfront URL Retrival](./assets/frontend.png)

---

## Accessing TextOps

Once CloudFront is active and model access (if needed) is approved:

* Visit the application URL
* Begin uploading documents for intelligent processing

Enjoy seamless OCR and document processing powered by AI.

---

## About TextOps

**TextOps** simplifies data extraction from documents using AI-based OCR pipelines. It supports formats such as PDFs, scanned images, and structured templates to automate data capture and reduce manual effort.


---

## Legal Notice

© 1CloudHub. All rights reserved.

This project is developed for internal demo or POC purposes and is not production-ready without proper security, scalability, and compliance review.

---
