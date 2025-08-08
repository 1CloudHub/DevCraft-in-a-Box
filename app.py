#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk.cdk_stack import CdkStack

app = cdk.App()
CdkStack(app, "CEXP-OCR-STACK")

app.synth()


# cdk bootstrap aws://123456789012/ap-south-1
# cdk synth
# cdk deploy
# cdk destroy