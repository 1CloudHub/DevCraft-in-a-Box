# Connect Xperience CDK Deployment Guide

## Overview

**Connect Xperience** is a Generative AI-based RAG (Retrieval-Augmented Generation) application developed using Amazon Bedrock. It allows users to upload documents and interact with them through a chatbot interface.

> **Disclaimer**: This CDK setup is strictly designed and tested for the `us-west-2` region (Oregon). Please ensure that all resources are deployed only within this region to avoid compatibility issues.

---

## Prerequisites

Before beginning the deployment process:

* Ensure you have access to the correct AWS account.
* You must have a valid GitHub PAT (Personal Access Token) with repository read access.
* You must be using the **`us-west-2`** AWS region.

---

## Deployment Steps

### 1. Login to the AWS Console

Log in to the provided AWS account using the IAM credentials or SSO as per the shared instructions.

### 2. Set Region to `us-west-2`

Navigate to the region selector in the AWS Console and ensure that **`US West (Oregon) - us-west-2`** is selected.

> This is critical, as all the CDK resources are scoped and supported only in this region.

![Region Navigation](./assets/region-navigation.png)

---

### 3. Open AWS CloudShell

Launch the AWS CloudShell service from the AWS Console.

> CloudShell provides a pre-configured environment with AWS CLI and CDK support, making it ideal for deployments.

![Cloudshell Navigation](./assets/cloudshell-navigatioin.png)

---

### 4. Clone the Repository

Replace `{branch_name}` and `{pat_token}` with the appropriate values.

```bash
git clone --branch {branch_name} https://{pat_token}@github.com/Hakash1CH/CEXP_CDK.git
```

> Clones the specific branch of the Connect Xperience CDK repository to your CloudShell environment.

---

### 5. Export GitHub Token

```bash
export GITHUB_TOKEN={pat_token}
```

> Sets your GitHub token in the current session, required for any actions needing GitHub access during deployment.

---

### 6. Install AWS CDK CLI

```bash
sudo npm install -g aws-cdk
```

> Installs the AWS CDK Command Line Interface globally in CloudShell.

---

### 7. Install Python Dependencies

```bash
cd CEXP_CDK
pip install --user -r requirements.txt
```

> Installs the required Python packages for the CDK app to function properly.

---

### 8. Bootstrap CDK

```bash
cdk bootstrap
```

> Prepares your AWS environment for deploying CDK applications by provisioning necessary resources like the CDK toolkit stack.

---

### 9. Deploy the Stack

```bash
cdk deploy
```

> Deploys the defined CDK infrastructure into your AWS account. This may take several minutes. Wait until the deployment completes successfully.

---

## Post Deployment Steps

### 10. Request Model Access in Bedrock

Navigate to the **Amazon Bedrock** service in the AWS Console.

* Open the **Model access** tab.
* Request access to the following models:

  ```
  - Claude 3.5 Sonnet V2
  - Claude 3.5 Haiku
  - Claude 3.7 Sonnet
  - Amazon Rerank
  - Amazon Titan Embedding V2
  ```

> It may take a few minutes for the model access to be approved.

![Model Access Navigation](./assets/model-access-navigation.png)
![Model Access](./assets/enable-model.png)
![Model Access](./assets/request-for-access.png)
![Bedrock Confirmation](./assets/bedrock-confirmation-page.png)
---

### 11. Get the Application URL

Navigate to the **CloudFront** service.

* Select the newly created distribution.
* Copy the **Domain Name** listed under **General settings**.

> This is your application's public URL. Note that it may take **5–6 minutes** post-deployment for the CloudFront distribution to become active.

![Cloudfront URL Retrival](./assets/frontend.png)

---

## Accessing the Application

Once the CloudFront distribution is active and model access is approved, open the copied domain name in your browser to start using **Connect Xperience**.

Enjoy the application experience.

---

## About Connect Xperience

**Connect Xperience** is an AI-powered document interaction platform. Users can upload documents and chat with an intelligent assistant that provides contextual answers derived from the content.

---

## Legal Notice

© 1CloudHub. All rights reserved.

This project and its components are intended for internal demonstration purposes only and should not be used in production without review and security assessment.

---