from aws_cdk import (
    Stack,
    Duration,
    Tags,
    RemovalPolicy,
    SecretValue,
    CfnOutput,
    CfnMapping,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_apigateway as apigateway,
    aws_apigatewayv2 as apigatewayv2,
    aws_amplify as amplify,
    aws_rds as rds
)
from constructs import Construct
import os
import json
import boto3
import botocore
import random, string
from cdk_ec2_key_pair import KeyPair
import secrets

# === Architecture & Runtime maps ===
arch_map = {
    "x86_64": _lambda.Architecture.X86_64,
    "arm64": _lambda.Architecture.ARM_64
}

runtime_map = {
    "python3.9": _lambda.Runtime.PYTHON_3_9,
    "python3.10": _lambda.Runtime.PYTHON_3_10,
    # Add more if needed
}

def generate_random_id(length=4, prefix=''):
    characters = string.ascii_lowercase + string.digits
    random_id = ''.join(random.choices(characters, k=length))
    return f"{prefix}{random_id}"

def get_available_bucket_name(base_name: str, max_attempts: int = 3) -> str:
    s3_client = boto3.client("s3")

    def is_available(name: str) -> bool:
        try:
            s3_client.head_bucket(Bucket=name)
            return False  # Bucket exists
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                return True
            return False  # Might be 403: bucket exists but not ours

    if is_available(base_name):
        return base_name

    for attempt in range(1, max_attempts + 1):
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        new_name = f"{base_name}-{suffix}"
        if is_available(new_name):
            print(f"✅ Retrying with new name: {new_name}")
            return new_name

    raise ValueError(f"❌ Failed to find available bucket name after {max_attempts} attempts.")

def generate_api_key(length=40):
    if length < 12 or length > 64:
        raise ValueError("Length must be between 12 and 64 characters")
    chars = string.ascii_letters + string.digits
    return ''.join(secrets.choice(chars) for _ in range(length))

class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        github_token_f = os.environ.get("GITHUB_TOKEN_F")
        if not github_token_f:
            raise ValueError("GITHUB_TOKEN_F environment variable not set")
        
        github_token_b = os.environ.get("GITHUB_TOKEN_B")
        if not github_token_b:
            raise ValueError("GITHUB_TOKEN_B environment variable not set")
        
        api_key_value = generate_api_key()
        self.suffix = generate_random_id()
        
        # === Global Tags and Identifiers ===
        self.global_tags = {
            "project": "cexp"
        }
 
        self.identifiers = {
            "rds_instance": f"ce-cexp-ocr-db-{self.suffix}"
        }
 
        # === VPC ===
        self.vpc = ec2.Vpc(
            self, "CexpOCR-VPC",
            vpc_name=f"CexpOCR-VPC-{self.suffix}",
            cidr="10.0.0.0/16",
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="PublicSubnet",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="PrivateSubnet",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24
                )
            ],
            nat_gateways=1,
            enable_dns_support=True,
            enable_dns_hostnames=True
        )
 
        for key, value in self.global_tags.items():
            Tags.of(self.vpc).add(key, value)
            
        CfnOutput(self, "VPCId", value=self.vpc.vpc_id)
 
 
        # === Security Group ===
        self.sg_main = ec2.SecurityGroup(
            self, "CexpOCRSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Security group for CEXP project",
            security_group_name=f"cexp-ocr-sg-{self.suffix}"
        )
 
        self.sg_main.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.all_traffic(),
            description="Allow all inbound from 0.0.0.0/0"
        )
 
        for key, value in self.global_tags.items():
            Tags.of(self.sg_main).add(key, value)
            
        CfnOutput(self, "SecurityGroupId", value=self.sg_main.security_group_id)
        
        
        # === RDS Subnet Group ===
        self.rds_subnet_group = rds.SubnetGroup(
            self, "CexpRdsSubnetGroup",
            description="Subnet group for RDS in CEXP project",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            subnet_group_name=f"cexp-ocr-rds-subnet-group-{self.suffix}"
        )

        for key, value in self.global_tags.items():
            Tags.of(self.rds_subnet_group).add(key, value)

        # === RDS Credentials ===
        rds_credentials = rds.Credentials.from_password(
            username="postgres",
            password=SecretValue.plain_text("Cexp$2025")
        )
        CfnOutput(self, "RdsSubnetGroupName", value=self.rds_subnet_group.subnet_group_name)


        # === RDS Instance ===
        self.rds_instance = rds.DatabaseInstance(
            self, "CexpPostgresDb",
            instance_identifier=self.identifiers["rds_instance"],
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_17_4
            ),
            credentials=rds_credentials,
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3, ec2.InstanceSize.MICRO
            ),
            allocated_storage=20,
            storage_type=rds.StorageType.GP3,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            multi_az=False,
            subnet_group=self.rds_subnet_group,
            security_groups=[self.sg_main],
            publicly_accessible=False,
            backup_retention=Duration.days(0),
            delete_automated_backups=True,
            removal_policy=RemovalPolicy.DESTROY,
            deletion_protection=False,
            auto_minor_version_upgrade=False,
            enable_performance_insights=False,
            monitoring_interval=Duration.seconds(0)
        )

        for key, value in self.global_tags.items():
            Tags.of(self.rds_instance).add(key, value)
        CfnOutput(self, "RdsInstanceId", value=self.rds_instance.instance_identifier)
        CfnOutput(self, "RdsEndpointAddress", value=self.rds_instance.db_instance_endpoint_address)
        
        
        # === EC2 IAM Role ===
        ec2_role = iam.Role(
            self, "CexpOCR-Ec2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambda_FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonBedrockFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonVPCFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudFrontFullAccess"),
            ],
            role_name=f"CexpOCR-EC2Role-{self.suffix}"
        )

        ec2_role.add_to_policy(
                iam.PolicyStatement(
                    actions=[
                        "aoss:*",  # or limit to specific actions like "aoss:APIAccessAll", "aoss:CreateCollection", etc.
                    ],
                    resources=["*"]
                )
            )
        for key, value in self.global_tags.items():
            Tags.of(ec2_role).add(key, value)
        ec2_role_arn = ec2_role.role_arn
        
        
        # === Create S3 ===
        bucket_name = "cexp-ocr-bucket"
        final_bucket_name = get_available_bucket_name(bucket_name)

        # Create the bucket with your original CORS config
        doc_bucket = s3.Bucket(
            self, "doc_bucket",
            bucket_name=final_bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            cors=[
                s3.CorsRule(
                    allowed_methods=[
                        s3.HttpMethods.GET,
                        s3.HttpMethods.PUT,
                        s3.HttpMethods.POST,
                        s3.HttpMethods.DELETE
                    ],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
                    max_age=3000
                )
            ]
        )
        
        # Add tag to S3 bucket
        Tags.of(doc_bucket).add("project", "cexp")
        
        # === Creating Role and Policy Needed ===
        CEXP_OCR_S3_Upload_Role = iam.Role(
            self, "CEXP_OCR_S3_Upload_Role",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
            description="Allows API Gateway to log to CloudWatch and write to S3",
            role_name=f"CEXP_OCR_S3_Upload_Role-{self.suffix}"
        )

        CEXP_OCR_S3_Upload_Role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams",
                    "logs:PutLogEvents",
                    "logs:GetLogEvents",
                    "logs:FilterLogEvents"
                ],
                resources=["*"]
            )
        )

        # Add inline policy for S3 PutObject
        CEXP_OCR_S3_Upload_Role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject"],
                resources=[f"arn:aws:s3:::{final_bucket_name}/*"]
            )
        )
        
        # Add tag to IAM Role
        Tags.of(CEXP_OCR_S3_Upload_Role).add("project", "cexp")
        
        
        # === Create the Lambda Role ===
        lambda_role = iam.Role(
            self, "CEXP_Lambda_Role",
            role_name=f"CEXP_Lambda_Role-{self.suffix}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role for Lambda with full access to Bedrock, S3, SageMaker, etc.",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonTextractFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonBedrockFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSQSFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambda_FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ]
        )

        # === Add custom inline policy ===
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="AmazonBedrockKMSPolicy",
                effect=iam.Effect.ALLOW,
                actions=[
                    "kms:GenerateDataKey",
                    "kms:Decrypt",
                    "es:*",
                    "aoss:*",
                    "execute-api:Invoke",
                    "execute-api:ManageConnections",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DescribeInstances",
                    "ec2:DeleteNetworkInterface",
                    "ec2:CreateNetworkInterface",
                    "ec2:AttachNetworkInterface",
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:UnassignPrivateIpAddresses"
                ],
                resources=["*"]
            )
        )

        # === Add tag to Lambda Role ===
        Tags.of(lambda_role).add("project", "cexp")
        
        

        # === Load Lambda config ===
        with open("lambda_config.json") as f:
            lambda_defs = json.load(f)

        # === Layer lookup ===
        layer_dir = os.path.join(os.getcwd(), "layer_zips")
        layer_versions = {}

        for filename in os.listdir(layer_dir):
            if filename.endswith(".zip"):
                layer_name = os.path.splitext(filename)[0]
                layer_versions[layer_name] = _lambda.LayerVersion(
                    self,
                    id=layer_name.replace("-", "_").replace(".", "_"),
                    layer_version_name=layer_name,
                    code=_lambda.Code.from_asset(
                        os.path.join(layer_dir, filename)),
                    compatible_architectures=[_lambda.Architecture.X86_64],
                    # You can generalize this if needed
                    compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
                    description=f"Layer from {filename}"
                )

        lambda_map = {}
        
        rds_host = self.rds_instance.db_instance_endpoint_address
        
        db_env = {
            "db_database": "postgres",
            "db_host": rds_host,
            "db_password": "Cexp$2025",
            "db_port": "5432",
            "db_user": "postgres",
            "bucket_name": final_bucket_name,
            "region_name": self.region
        }

        # === Lambda Creation ===
        for ldef in lambda_defs:
            name = ldef["name"]
            code_path = os.path.abspath(ldef["code_zip"])

            fn = _lambda.Function(
                self,
                id=name,
                function_name=name,
                runtime=runtime_map[ldef["runtime"]],
                architecture=arch_map[ldef["architecture"]],
                memory_size=ldef['memory_size'],
                handler="lambda_function.lambda_handler",
                code=_lambda.Code.from_asset(code_path),
                timeout=Duration.seconds(ldef.get("timeout", 300)),
                role=lambda_role,
                environment={
                    **ldef.get("environment_variable", {}),
                    **db_env
                },
                layers=[
                    layer_versions[layer] for layer in ldef.get("layers", []) if layer in layer_versions
                ],
                vpc=self.vpc,
                security_groups=[self.sg_main],
                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
            )

            for key, val in ldef.get("tags", {}).items():
                Tags.of(fn).add(key, val)

            lambda_map[name] = fn

        # REST API
        api = apigateway.RestApi(self, "OCR_api",
            rest_api_name=f"OCR_api-{self.suffix}",
            binary_media_types=[
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/pdf",
                "multipart/form-data",
                "image/png",
                "image/jpg"
            ],
            deploy_options=apigateway.StageOptions(stage_name="production")
        )

        # Apply tag to API stage
        Tags.of(api.deployment_stage).add("project", "cexp")
        
        # Add CORS headers to 4XX and 5XX responses
        cors_response_parameters = {
            "gatewayresponse.header.Access-Control-Allow-Headers": "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
            "gatewayresponse.header.Access-Control-Allow-Methods": "'OPTIONS,POST'",
            "gatewayresponse.header.Access-Control-Allow-Origin": "'*'",
        }

        # Default 4XX
        api.add_gateway_response(
            "Default4xx",
            type=apigateway.ResponseType.DEFAULT_4_XX,
            response_headers=cors_response_parameters
        )

        # Default 5XX
        api.add_gateway_response(
            "Default5xx",
            type=apigateway.ResponseType.DEFAULT_5_XX,
            response_headers=cors_response_parameters
        )
        
        
        # /ocr
        ocr = api.root.add_resource("ocr")
        
        # OPTIONS Route
        ocr.add_method(
            "OPTIONS",
            apigateway.MockIntegration(
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code="200",
                        response_parameters={
                            "method.response.header.Access-Control-Allow-Origin": "'*'",
                            "method.response.header.Access-Control-Allow-Methods": "'DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT'",
                            "method.response.header.Access-Control-Allow-Headers": "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                            "method.response.header.X-Requested-With": "'*'",
                        },
                        response_templates={
                            "application/json": ""
                        }
                    )
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.WHEN_NO_MATCH,
                request_templates={
                    "application/json": "{\"statusCode\": 200}"
                },
            ),
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.X-Requested-With": True,
                    }
                )
            ]
        )



        # POST Integration with CORS headers
        ocr.add_method(
            "POST",
            apigateway.LambdaIntegration(
                lambda_map['CEXP_OCR_Function'],
                proxy=False,
                request_templates={ 
                    "application/json": ""
                },
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code="200",
                        response_templates={
                            "application/json": ""
                        },
                        response_parameters={
                            "method.response.header.Access-Control-Allow-Origin": "'*'",
                            "method.response.header.Access-Control-Allow-Methods": "'DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT'",
                            "method.response.header.Access-Control-Allow-Headers": "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                            "method.response.header.X-Requested-With": "'*'",
                        },
                    ),
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.WHEN_NO_MATCH,
            ),
            request_models={
                "application/json": apigateway.Model.EMPTY_MODEL
            },
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_models={
                        "application/json": apigateway.Model.EMPTY_MODEL
                    },
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.X-Requested-With": True,
                    }
                )
            ],
            api_key_required=True
        )

        # PUT (S3 Integration)
        put_integration = apigateway.AwsIntegration(
            service="s3",
            integration_http_method="PUT",
            path="/" + final_bucket_name + "/{filename}",
            options=apigateway.IntegrationOptions(
                credentials_role=CEXP_OCR_S3_Upload_Role,
                request_parameters={
                    "integration.request.path.filename": "method.request.querystring.filename",
                    "integration.request.path.bucket": "method.request.querystring.bucket"
                },
                passthrough_behavior=apigateway.PassthroughBehavior.WHEN_NO_MATCH,
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code="200",
                        response_parameters={
                            "method.response.header.Access-Control-Allow-Origin": "'*'",
                            "method.response.header.Access-Control-Allow-Methods": "'DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT'",
                            "method.response.header.Access-Control-Allow-Headers": "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                            "method.response.header.X-Requested-With": "'*'",
                        }
                    )
                ]
            )
        )

        put_method_response = apigateway.MethodResponse(
            status_code="200",
            response_models={
                "application/json": apigateway.Model.EMPTY_MODEL
            },
            response_parameters={
                "method.response.header.Access-Control-Allow-Origin": True,
                "method.response.header.Access-Control-Allow-Methods": True,
                "method.response.header.Access-Control-Allow-Headers": True,
                "method.response.header.X-Requested-With": True,
            }
        )

        ocr.add_method(
            "PUT",
            put_integration,
            request_parameters={
                "method.request.querystring.filename": True,
                "method.request.querystring.bucket": True
            },
            method_responses=[put_method_response],
            api_key_required=True
        )
        
        
        # Usage Plan
        plan = apigateway.CfnUsagePlan(self, "OCR_UsagePlan",
            usage_plan_name=f"OCR_Usage_Plan-{self.suffix}",
            throttle=None,
            quota=None,
            api_stages=[apigateway.CfnUsagePlan.ApiStageProperty(
                api_id=api.rest_api_id,
                stage=api.deployment_stage.stage_name
            )]
        )
        Tags.of(plan).add("project", "cexp")

        # API Key
        key = apigateway.CfnApiKey(self, "OCR_API_KEY",
            name=f"OCR_API_KEY-{self.suffix}",
            enabled=True,
            value=api_key_value
        )
        Tags.of(key).add("project", "cexp")

        apigateway.CfnUsagePlanKey(self, "OCRUsagePlanKey",
            key_id=key.ref,
            key_type="API_KEY",
            usage_plan_id=plan.ref
        )
        

        # === Create S3 ===
        site_bucket = "cexp-ocr-site"
        site_bucket_name = get_available_bucket_name(site_bucket)

        # Create the bucket with your original CORS config
        bucket = s3.Bucket(
            self, "site_bucket",
            bucket_name=site_bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        
        # Add tag to S3 bucket
        Tags.of(bucket).add("project", "cexp")
        
        origin_config = {
            "Name": f"CEXPOCR-{self.suffix}",
            "SigningProtocol": "sigv4",
            "SigningBehavior": "always",
            "OriginAccessControlOriginType": "s3"
        }
        
        # === Create User Data Script ===
        user_data_script = ec2.UserData.for_linux()
        
        user_data_script.add_commands(
            # Cloning the Repo
            "sudo yum update -y > firstcmd.log 2>&1", 
            "sudo yum install python3-pip git -y > secondcmd.log 2>&1", 
            "curl -fsSL https://rpm.nodesource.com/setup_18.x | sudo bash -", 
            "sudo yum install -y nodejs > fourthcmd.log 2>&1", 
            "python3 -m ensurepip --upgrade",
            "python3 -m pip install --upgrade pip",
            "mkdir -p /home/ec2-user/cexp_app",
            "cd /home/ec2-user/cexp_app",
            f"git clone https://{github_token_f}@github.com/Hakash1CH/sample-app.git cexpOCR > gitclone.log 2>&1",
            
            "cd cexpOCR", 
            f"""cat <<EOF > .env
REACT_APP_BASE_URL={api.url}ocr
REACT_APP_API_KEY={api_key_value}
EOF""",
            "npm install --legacy-peer-deps > npmimsatll.log 2>&1",
            "npm run build > build.log 2>&1", 
            f"export REGION_NAME={self.region}",
            f"aws s3 sync build/ s3://{site_bucket_name}",
            f'''OAC_OUTPUT=$(aws cloudfront create-origin-access-control \
  --origin-access-control-config '{json.dumps(origin_config)}' --region us-east-1)

DISTRIBUTION_ID=$(echo "$OAC_OUTPUT" | jq -r '.OriginAccessControl.Id')

echo "DISTRIBUTION_ID=$DISTRIBUTION_ID" >> .env
export DISTRIBUTION_ID''',

            f"export BUCKET_NAME={site_bucket_name}",

            '''cat <<EOF > distribution-config.json
{
  "CallerReference": "react-spa-$(date +%s)",
  "Comment": "CEXP OCR CloudFront distribution",
  "Enabled": true,
  "Origins": {
    "Quantity": 1,
    "Items": [
      {
        "Id": "S3Origin",
        "DomainName": "${BUCKET_NAME}.s3.${REGION_NAME}.amazonaws.com",
        "OriginAccessControlId": "$DISTRIBUTION_ID",
        "S3OriginConfig": {
          "OriginAccessIdentity": ""
        }
      }
    ]
  },
  "DefaultRootObject": "index.html",
  "DefaultCacheBehavior": {
    "TargetOriginId": "S3Origin",
    "ViewerProtocolPolicy": "redirect-to-https",
    "AllowedMethods": {
      "Quantity": 2,
      "Items": ["GET", "HEAD"],
      "CachedMethods": {
        "Quantity": 2,
        "Items": ["GET", "HEAD"]
      }
    },
    "Compress": true,
    "ForwardedValues": {
      "QueryString": false,
      "Cookies": {
        "Forward": "none"
      }
    },
    "MinTTL": 0,
    "DefaultTTL": 3600,
    "MaxTTL": 86400
  },
  "CustomErrorResponses": {
    "Quantity": 2,
    "Items": [
      {
        "ErrorCode": 403,
        "ResponsePagePath": "/index.html",
        "ResponseCode": "200",
        "ErrorCachingMinTTL": 0
      },
      {
        "ErrorCode": 404,
        "ResponsePagePath": "/index.html",
        "ResponseCode": "200",
        "ErrorCachingMinTTL": 0
      }
    ]
  }
}
EOF''',
            '''CF_ARN=$(aws cloudfront create-distribution \
  --distribution-config file://distribution-config.json \
  --query "Distribution.ARN" \
  --output text --region us-east-1)''',
            '''cat <<EOF > bucket-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowCloudFrontAccessOnly",
      "Effect": "Allow",
      "Principal": {
        "Service": "cloudfront.amazonaws.com"
      },
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::$BUCKET_NAME/*",
      "Condition": {
        "StringEquals": {
          "AWS:SourceArn": "$CF_ARN"
        }
      }
    }
  ]
}
EOF''',
       
        '''aws s3api put-bucket-policy \
  --bucket "$BUCKET_NAME" \
  --policy file://bucket-policy.json''',

            
            f"git clone https://{github_token_b}@github.com/Rahul1ch/event-test.git DB_table_git",
            "cd DB_table_git",
            f"""cat <<'EOF' > .env
DB_HOST={rds_host}
DB_PORT=5432
DB_DATABASE=postgres
DB_USER=postgres
DB_PASSWORD=Cexp$2025
EOF""",
            "python3 -m pip install psycopg2-binary dotenv > pip_install.log 2>&1",
            "python3 OCR_Table_Creation.py > table_creation.log 2>&1"
        )
        
        
        # === EC2 Key Pair Name ===
        key_pair_name = f"ce-cexp-keypair-{self.suffix}"
        ec2_key_pair = KeyPair(
            self, "MyKeyPair",
            key_pair_name=key_pair_name,
            description="Key pair for my EC2 instance",
            store_public_key=True
        )

        # === EC2 Instance ===
        self.ec2_instance = ec2.Instance(
            self, "CexpocrEc2Instance",
            instance_name=f"CexpocrEc2Instance-{self.suffix}",
            instance_type=ec2.InstanceType("t3.medium"),
            # machine_image=ec2.GenericLinuxImage({
            #     "us-east-1": "ami-08a6efd148b1f7504"
            # }),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            security_group=self.sg_main,
            key_name=ec2_key_pair.key_pair_name,  # <- must be string
            role=ec2_role,
            user_data=user_data_script
        )

        # === Enable auto-assign public IP ===
        self.ec2_instance.instance.launch_template = None  # Ensures CDK uses auto public IP for public subnet

        # === Ensure EC2 depends on KeyPair creation ===
        self.ec2_instance.node.add_dependency(ec2_key_pair)
        # === Outputs ===
        CfnOutput(self, "Ec2InstanceId", value=self.ec2_instance.instance_id)
        CfnOutput(self, "Ec2PublicDns", value=self.ec2_instance.instance_public_dns_name)
        CfnOutput(self, "Ec2PublicIp", value=self.ec2_instance.instance_public_ip)
        
        
        self.sg_main.node.add_dependency(self.vpc)
        self.rds_subnet_group.node.add_dependency(self.vpc)
        self.rds_instance.node.add_dependency(self.sg_main)
        self.rds_instance.node.add_dependency(self.rds_subnet_group)
        self.rds_instance.node.add_dependency(self.vpc)
        
        
        
