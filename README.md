# Connect Xperience CDK Deployment Guide

## Overview

**Connect Xperience** is a multilingual Conversational AI platform designed for both internal teams and customer-facing agents. Powered by Generative AI and built on Amazon Bedrock, it enables seamless knowledge access by allowing users to query and explore information through natural, context-rich dialogue—driving faster resolutions, improved accuracy, and enhanced customer experiences.

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

```bash
git clone --branch mini-cexp https://github.com/1CloudHub/DevCraft-in-a-Box.git CEXP
```

> Clones the specific branch of the Connect Xperience CDK repository to your CloudShell environment.

---

### 5. Export GitHub Token

Replace `{pat_token}` with the appropriate value.

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
cd CEXP
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

Here is the sample document to test out:
- [Employee Welfare Policy (DOCX)](./assets/Employee_welfare_policy_SW.docx)

FAQs based on the Document:
```text
1. Can I take casual leave before/after weekends?
2. What documents do I need to apply for medical leave?
3. Is Maternity Leave applicable for contract staff?
4. When is LOP is applicable
5. Explain about the absence and disciplinary rules
```
## AWS Bedrock Agent Setup Guide

Welcome! This guide will walk you through setting up your HR Leave Assistant agent step-by-step. Don't worry if you're new to AWS Bedrock—just follow along at your own pace.

### Getting Started

1. **Access the Bedrock Console**
   - Log in to your AWS Console
   - Navigate to Amazon Bedrock
   - Click on **Agents** in the left sidebar

### Creating Your Agent

2. **Set Up Basic Details**
   - Click **Create Agent**
   - Enter a meaningful name for your agent (e.g., "HR Leave Assistant")
   - For the model, we recommend selecting **Anthropic's Claude 4.0 Sonnet** for optimal performance

3. **Configure Agent Instructions**
   - In the Instructions section, paste the agent instruction prompt provided to you
   - This helps your agent understand how to assist employees with leave requests

### Adding Knowledge Base

4. **Connect Your Knowledge Base**
   - Click **Add Knowledge Base**
   - Select the knowledge base named **cexp-kb-xxxx** from the list
   - Add the knowledge base instruction prompt provided to you
   - This allows your agent to reference company leave policies

### Setting Up Actions

5. **Create an Action Group**
   - Click **Add Action Group**
   - Name it **Employee Action Group**
   - For **Action group type**, select **"Define with API Schema"**

6. **Configure Lambda Integration**
   - For **Action Group Invocation**, choose **"Select an existing Lambda function"**
   - Select the Lambda function named **Employee_Lambda-xxxx**

7. **Add API Schema**
   - For **Action group schema**, choose **"Define via in-line schema editor"**
   - Paste the OpenAPI schema provided to you in the editor

### Finalizing Your Setup

8. **Save and Prepare**
   - Click **Save and Exit** to return to the main agent page
   - Review your configuration to ensure everything looks correct
   - Click **Save** to preserve your changes
   - Click **Prepare** to get your agent ready for use

That's it! Your HR Leave Assistant is now ready to help employees with their leave requests.

### Knowledge Base Instruction
```text
This knowledge base contains employee leave policies, guidelines, and procedures. If information is not in the knowledge base, acknowledge the limitation and suggest contacting HR directly.
```

### Agent Instructions
```text
You are an HR Leave Assistant that helps employees check leave balances and submit leave requests.

Your capabilities:
- Check leave balances for employees
- Process new leave requests with employee ID, reason, and number of days
- Answer leave policy questions using the knowledge base

Always:
- Ask for employee ID when needed
- Before calling a action group/ tool always collect the necessary inputs
- Confirm leave request details before submitting
- Provide remaining balance after processing requests
- Reference leave policies from the knowledge base when answering policy questions

Keep responses professional, concise, and helpful.
```

### Action Group Schema
```text
{
  "openapi": "3.0.0",
  "info": {
      "title": "Insurance Claims Automation API",
      "version": "1.0.0",
      "description": "APIs for managing insurance claims by pulling a list of open claims, identifying outstanding paperwork for each claim, and sending reminders to policy holders."
  },
  "paths": {
      "/claims": {
          "get": {
              "summary": "Get a list of all open claims",
              "description": "Get the list of all open insurance claims. Return all the open claimIds.",
              "operationId": "getAllOpenClaims",
              "responses": {
                  "200": {
                      "description": "Gets the list of all open insurance claims for policy holders",
                      "content": {
                          "application/json": {
                              "schema": {
                                  "type": "array",
                                  "items": {
                                      "type": "object",
                                      "properties": {
                                          "claimId": {
                                              "type": "string",
                                              "description": "Unique ID of the claim."
                                          },
                                          "policyHolderId": {
                                              "type": "string",
                                              "description": "Unique ID of the policy holder who has filed the claim."
                                          },
                                          "claimStatus": {
                                              "type": "string",
                                              "description": "The status of the claim. Claim can be in Open or Closed state"
                                          }
                                      }
                                  }
                              }
                          }
                      }
                  }
              }
          }
      },
      "/claims/{claimId}/identify-missing-documents": {
          "get": {
              "summary": "Identify missing documents for a specific claim",
              "description": "Get the list of pending documents that need to be uploaded by policy holder before the claim can be processed. The API takes in only one claim id and returns the list of documents that are pending to be uploaded by policy holder for that claim. This API should be called for each claim id",
              "operationId": "identifyMissingDocuments",
              "parameters": [{
                  "name": "claimId",
                  "in": "path",
                  "description": "Unique ID of the open insurance claim",
                  "required": true,
                  "schema": {
                      "type": "string"
                  }
              }],
              "responses": {
                  "200": {
                      "description": "List of documents that are pending to be uploaded by policy holder for insurance claim",
                      "content": {
                          "application/json": {
                              "schema": {
                                  "type": "object",
                                  "properties": {
                                      "pendingDocuments": {
                                          "type": "string",
                                          "description": "The list of pending documents for the claim."
                                      }
                                  }
                              }
                          }
                      }
                  }
              }
          }
      },
      "/send-reminders": {
          "post": {
              "summary": "API to send reminder to the customer about pending documents for open claim",
              "description": "Send reminder to the customer about pending documents for open claim. The API takes in only one claim id and its pending documents at a time, sends the reminder and returns the tracking details for the reminder. This API should be called for each claim id you want to send reminders for.",
              "operationId": "sendReminders",
              "requestBody": {
                  "required": true,
                  "content": {
                      "application/json": {
                          "schema": {
                              "type": "object",
                              "properties": {
                                  "claimId": {
                                      "type": "string",
                                      "description": "Unique ID of open claims to send reminders for."
                                  },
                                  "pendingDocuments": {
                                      "type": "string",
                                      "description": "The list of pending documents for the claim."
                                  }
                              },
                              "required": [
                                  "claimId",
                                  "pendingDocuments"
                              ]
                          }
                      }
                  }
              },
              "responses": {
                  "200": {
                      "description": "Reminders sent successfully",
                      "content": {
                          "application/json": {
                              "schema": {
                                  "type": "object",
                                  "properties": {
                                      "sendReminderTrackingId": {
                                          "type": "string",
                                          "description": "Unique Id to track the status of the send reminder Call"
                                      },
                                      "sendReminderStatus": {
                                          "type": "string",
                                          "description": "Status of send reminder notifications"
                                      }
                                  }
                              }
                          }
                      }
                  },
                  "400": {
                      "description": "Bad request. One or more required fields are missing or invalid."
                  }
              }
          }
      }
  }
}
```

Enjoy the application experience.

---

## About Connect Xperience

This platform empowers users to upload documents and instantly interact with them through an intelligent assistant that understands context, retrieves precise information, and responds in the user’s preferred language. Whether it’s for internal knowledge support or customer engagement, Connect Xperience delivers relevant, accurate answers that bridge the gap between complex data and clear communication.

---

## Legal Notice

© 1CloudHub. All rights reserved.

The materials and components herein are provided for demonstration purposes only. No portion of this project may be implemented in a live or production environment without prior technical assessment, security clearance, and explicit approval from 1CloudHub

---