# AWS Quick Suite Workshop – Guide

## Introduction

This workshop guide is designed to walk participants through a structured, hands-on AWS Quick Suite experience. It covers the end-to-end journey of setting up the environment, exploring core Quick Suite capabilities, and applying them to real-world business scenarios. The guide is intended to be followed sequentially during the workshop, enabling participants to execute each step confidently within the allotted time while gaining practical exposure to analytics, knowledge management, AI-assisted insights, and automation.

---

## Prerequisites

Before starting the workshop, ensure the following are available:

- An active AWS account with appropriate permissions (Admin access is preferred to avoid delays or hindrance during the workshop due to time constraints)
- Access to **AWS Quick Suite** in the US East (N. Virginia) – **`us-east-1`** region (recommended, as this region supports all available Quick Suite features)
- Stable internet connectivity
- Modern web browser (Chrome / Firefox recommended)

> **Note:** IAM permissions and region availability must be validated before the workshop begins. Using regions other than `us-east-1` may result in limited functionality.

---

## Workshop Overview

During this workshop, you will:

1. Explore the core capabilities of AWS Quick Suite
2. Work hands-on with **Quick Sight** for analytics, dashboards, scenarios, and stories
3. Use **Quick Spaces** to organize and manage structured and unstructured knowledge
4. Create and interact with **Quick Chat Agents** for product queries and internal analytics
5. Perform market and competitor analysis using **Quick Research**
6. Build and execute automated workflows using **Quick Flows**

> By the end of the workshop, participants will have a clear and practical understanding of how each Quick Suite capability can be applied to real-world business use cases

### Users Set-up

1. Log in to the AWS Management Console and open IAM service.
2. Create a policy with full access to the `quicksight` service in the IAM Console.

![](screenshots/createPolicy.png)

3. Create users with access to the AWS Console and attach the `quicksight` policy created.

![](screenshots/user_creation.png)
![](screenshots/attachPolicy.png)

4. Once the user logs in into both the `AWS Console` and `QuickSuite Dashboard`, Go to the `Manage Users` tab in the Admin panel and change the user role to `Admin Pro` to allow the user to use AI features of `Quick Suite`.

![](screenshots/admin_pro_permission.png)

### Quick Suite Set-up

1. Log in to the AWS Management Console
2. Select the US East (N. Virginia) – **`us-east-1`** region
3. Navigate to **AWS Quick Suite**

![](screenshots/awsQuickSuiteSearch.png)

4. Create a Quick Suite account by providing a `Account Name` and `Contact email`
5. Within the Quick Suite Console, Make sure the region is **`us-east-1`**

![](screenshots/quickSuiteRegionSelect.png)

---

## Quick Sight

[Amazon Quick Sight](https://docs.aws.amazon.com/quicksuite/latest/userguide/quick-bi.html) delivers AI-powered BI capabilities within Quick, transforming your scattered data into strategic insights for everyone— enabling you to make faster decisions and achieve better business outcomes.

### Step 1: Upload the Datasets

1. Before Uploading any data, ensure that sufficient SPICE Capacity is available. This can be checked in the QuickSuite Admin Dashboard which can be accessed from Navbar.

![](screenshots/quickSuiteAdminNav.png)

2. From the sidebar choose `SPICE Capacity` and make sure to provide sufficient SPICE capacity using the `Purchase capacity` option. Once done, go back to the `QuickSuite Dashboard` and select the `Datasets` tab within `QuickSight` in the sidebar.

![](screenshots/spiceCapacityPurchase.png)

3. Upload the provided **sample dataset:** **[SaaS-Sales.csv](/data/SaaS-Sales.csv)**.
   This data consists of customer and account details, the SaaS plans or products purchased, associated revenue and subscription information, engagement and usage signals, and the current stage of each customer in the sales and retention lifecycle.

![](screenshots/uploadDataDatasetsHome.png)

4. Before validating, use the `Edit/Preview data` option to map appropriate datatypes to the columns in the dataset. Use the `Change data type` step to alter the data types (such as `Row ID` and `Customer ID` to `Integer` and `Order Date` to `Date`)

![](screenshots/quickSightEditDataset.png)

5. Once finished, Save it using the `Save & Publish` button.

> It may take a few minutes for the dataset to be processed. Wait for it to complete before proceeding to the next step.

### Step 2: Create Analysis

1. With the dataset created in the previous step, we can now visualize and perform an analysis on it using the **Quick Sight Analyses** feature. Select the Analyses Dashboard using the sidebar, and click on `Create Analysis`.
2. Select the `Saas-Sales.csv` dataset as source and proceed to the Analysis Sheet.

![](screenshots/analysesHome.png)

3. Create these visuals manually.
   - **Total Sales** - `KPI`
   - **Total Profit** - `KPI`
   - **Monthly Sales Trend** - `Line Graph` (Sales(Sum) vs Order Date)
   - **Top Products by Sales** - `Horizontal Bar Chart` (Products vs Sales)

![](screenshots/monthlySalesAnalyses.png)

### Step 3: Add Visualizations with AI

1. Use the build button to add a visualization representing **Total Sales by month and Region** with the following prompt:
   `Show monthly sales by region using order date visually`

![](screenshots/totalSalesMonthRegion.png)

2. Similarly, add visualizations to represent **Total Sales by Region**, **Total Sales by Month and Segment** and **Total Sales by Segment**
3. Use the `Edit with Q` button present in the visualizations to make changes to the visualizations (For example, removing the value points in charts with overwhelming values)
4. Finally, `Publish` the analysis into a Dashboard.
5. The published dashboard can be accessed from the sidebar in quick suite dashboard.

![](screenshots/quickSightDashboard.png)

### Step 4: Analyzing with Scenarios to understand data

1. Dashboards created can be used as source in QuickSight Scenarios. This can be created either as a new Scenario from the QuickSight sidebar or directly from the `Analyze this dashboard in a Scenario` option at the top of the dashboard.
2. Create threads with the following questions to explore the data and understand the impact of changes with AI.

```
- How would our profit change if we increased the price of Marketing Suite - Gold by 10%?
- what will happen if I double the sales in APJ region
- What products are contributing most to our overall profit?
```

3. A thread can also be continued, modified or branched as desired.

![](screenshots/quickSightScenariosDoubleSalesAPJ.png)

### Step 5: Creating Stories to highlight key details

1. Add a prompt describing the story.

![](screenshots/quickSightStoryPrompt.png)

2. Add visuals as source to create the story points.

![](screenshots/quickSightStoryAddVisuals.png)

3. Once the story is completed, customize and modify the story as desired.

![](screenshots/quickSightStoryResultTheme.png)

4. The stories show live data from the dashboards. Use the preview option provided in the editor to view the latest snapshot.

## ![](screenshots/quickSightStoryResultPreview.png)

---

## Quick Space

[AWS Quick Space](https://docs.aws.amazon.com/quicksuite/latest/userguide/working-with-spaces.html) will help you organize a collection of related data sources in one place and share them with your team in Quick

1. Before uploading any data, ensure that sufficient Index Capacity is available. This can be checked in the QuickSuite Admin Dashboard which can be accessed from Navbar.
2. From the sidebar choose `Index Capacity` and make sure to provide sufficient Index capacity using the `Manage capacity` option.

![](screenshots/indexCapacityModify.png)

3. With enough index capacity, create a new space using the `Create Space` option from the `Quick Space` dashboard

![](screenshots/quickSpaceHome.png)

4.  Create a space `proprietarySpace` and upload relevant files for managing internal proprietary data.

**Product Brochure:** **[Airstep_Sample_Product_Brochure.pdf](data/Airstep_Sample_Product_Brochure.pdf)**

![](screenshots/quickSpaceProductSpace.png)

---

## Quick Research

[AWS Quick Research](https://docs.aws.amazon.com/quicksuite/latest/userguide/using-amazon-quick-research.html) helps you to accelerate complex business research by combining enterprise knowledge and internet data for faster, expert-level insights

1. Start a new research from the `Quick Research` dashboard.

![](screenshots/quickResearchHome.png)

2. Provide a description to research on and configure the materials to use such as `Web search` and `Spaces`.

**Sample description:** `Do a detailed research on the airstep product and give other comptetitors in the market`

![](screenshots/quickResearchConfigure.png)

![](screenshots/quickResearchAssetsMaterials.png)

3. Review the plan once to understand how the research is going to be approached and then click on `Start researching`.

![](screenshots/quickResearchProcessing.png)

4. The research will take around 30 minutes to complete. Once ready, a detailed report on the research topic will be produced along with the citations.

![](screenshots/quickResearchResult.png)

---

## Quick Chat Agent

[AWS Quick Chat Agent](https://docs.aws.amazon.com/quicksuite/latest/userguide/working-with-agents.html) accelerates creating chatbots to get answers to your questions by chatting with Quick in natural language

1. Build a **Chat Agent** that helps to accelerate the marketing aspect of the business and improve sales to be used from the business end.

**Sample Prompt:** `I want an agent that aims to improve the sales and enhance the marketing aspect of the business`

![](screenshots/quickAgentInternalAgentPrompt.png)

2. In the `Configure chat agent` screen, customize the prompts, style/tone, suggestions and response instructions as desired.
3. Link the `proprietarySpace` in the `Knowledge sources` section.

![](screenshots/quickAgentInternalAgentSpaceLink.png)

4. Publish the `Chat Agent` using the `Launch chat agent` button. This chat agent can then be accessed from the quick suite console, or can also be embedded in other websites (after whitelisting) using the iframe code snippet provided.

**Sample Q & A:**

![](screenshots/quickAgentInternalAgentLinkedinPrompt.png)

![](screenshots/quickAgentInternalAgentLinkedinResult3.png)

---

## Quick Flows

[AWS Quick Flows](https://docs.aws.amazon.com/quicksuite/latest/userguide/using-amazon-quick-flows.html) will help you automate routine tasks with AI-powered workflows to fast-track processes.

1. From the `Quick Flows` dashboard use the `Create flow` button and generate a flow that reads review data of the product as a csv file, analyze, classify, perform sentiment analysis and finally produce a summary report.

![](screenshots/quickFlowsHome.png)

**Sample Prompt:** `Analyze customer reviews from CSV files by classifying feedback types, performing sentiment analysis, and generating comprehensive summaries`

![](screenshots/quickFlowEditor.png)

2. Review the flow structure generated and publish it using the `Share and publish` button available at the top.
3. The flow will now be accessible in the chat panel, where we can attach the review data csv and invoke the flow.

**Review Sample File: [airstep_customer_reviews.csv](data/airstep_customer_reviews.csv)**

![](screenshots/quickFlowsRun.png)

![](screenshots/quickFlowsProcessing.png)

4. The summary report will be generated in a few minutes which can be copied or downloaded.

![](screenshots/quickFlowsResult.png)

---

## Additional References

- [Quick Suite User Guide](https://docs.aws.amazon.com/quicksuite/latest/userguide/what-is.html)
- [SPICE Capacity](https://docs.aws.amazon.com/quicksuite/latest/userguide/spice.html)
- [Manage SPICE](https://docs.aws.amazon.com/quicksuite/latest/userguide/managing-spice-capacity.html)
- [Index Capacity](https://docs.aws.amazon.com/quicksuite/latest/userguide/manage-data-capacity.html)
