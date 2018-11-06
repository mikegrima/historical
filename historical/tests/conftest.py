# pylint: disable=E0401,C0103,W0621,W0613
"""
.. module: historical.tests.test_s3
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Kevin Glisson <kglisson@netflix.com>
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json
import os

import boto3
from mock import patch
from moto import mock_sqs
from moto.dynamodb2 import mock_dynamodb2
from moto.s3 import mock_s3
from moto.iam import mock_iam
from moto.sts import mock_sts
from moto.ec2 import mock_ec2
import pytest


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='function')
def s3(aws_credentials):
    """Mocked S3 Fixture."""
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture(scope='function')
def ec2(aws_credentials):
    """Mocked EC2 Fixture."""
    with mock_ec2():
        yield boto3.client('ec2', region_name='us-east-1')


@pytest.fixture(scope='function')
def sts(aws_credentials):
    """Mocked STS Fixture."""
    with mock_sts():
        yield boto3.client('sts', region_name='us-east-1')


@pytest.fixture(scope='function')
def iam(aws_credentials):
    """Mocked IAM Fixture."""
    with mock_iam():
        yield boto3.client('iam', region_name='us-east-1')


@pytest.fixture(scope='function')
def dynamodb(aws_credentials):
    """Mocked DynamoDB Fixture."""
    with mock_dynamodb2():
        yield boto3.client('dynamodb', region_name='us-east-1')


@pytest.fixture(scope='function')
def retry():
    """Mock the retry library so that it doesn't retry."""
    def mock_retry_decorator(*args, **kwargs):
        def retry(func):
            return func
        return retry

    patch_retry = patch('retrying.retry', mock_retry_decorator)
    yield patch_retry.start()

    patch_retry.stop()


@pytest.fixture(scope='function')
def swag_accounts(s3, retry):
    """Create mocked SWAG Accounts."""
    from swag_client.backend import SWAGManager
    from swag_client.util import parse_swag_config_options

    bucket_name = 'SWAG'
    data_file = 'accounts.json'
    region = 'us-east-1'
    owner = 'third-party'

    s3.create_bucket(Bucket=bucket_name)
    os.environ['SWAG_BUCKET'] = bucket_name
    os.environ['SWAG_DATA_FILE'] = data_file
    os.environ['SWAG_REGION'] = region
    os.environ['SWAG_OWNER'] = owner

    swag_opts = {
        'swag.type': 's3',
        'swag.bucket_name': bucket_name,
        'swag.data_file': data_file,
        'swag.region': region,
        'swag.cache_expires': 0
    }

    swag = SWAGManager(**parse_swag_config_options(swag_opts))

    account = {
        'aliases': ['test'],
        'contacts': ['admins@test.net'],
        'description': 'LOL, Test account',
        'email': 'testaccount@test.net',
        'environment': 'test',
        'id': '012345678910',
        'name': 'testaccount',
        'owner': 'third-party',
        'provider': 'aws',
        'sensitive': False,
        'account_status': 'ready',
        'services': [
            {
                'name': 'historical',
                'status': [
                    {
                        'region': 'all',
                        'enabled': True
                    }
                ]
            }
        ]
    }

    swag.create(account)


@pytest.fixture(scope='function')
def historical_role(iam, sts):
    """Create the mocked Historical IAM role that Historical Lambdas would need to assume to List and
    Collect details about a given technology in the target account.

    """
    iam.create_role(RoleName='historicalrole', AssumeRolePolicyDocument='{}')
    os.environ['HISTORICAL_ROLE'] = 'historicalrole'


@pytest.fixture(scope='function')
def historical_sqs(aws_credentials):
    """Create the Mocked SQS queues that are used throughout Historical."""
    with mock_sqs():
        client = boto3.client('sqs', region_name='us-east-1')

        # Poller Tasker Queue:
        client.create_queue(QueueName='pollertaskerqueue')
        os.environ['POLLER_TASKER_QUEUE_NAME'] = 'pollertaskerqueue'

        # Poller Queue:
        client.create_queue(QueueName='pollerqueue')
        os.environ['POLLER_QUEUE_NAME'] = 'pollerqueue'

        # Event Queue:
        client.create_queue(QueueName='eventqueue')
        os.environ['EVENT_QUEUE_NAME'] = 'eventqueue'

        # Proxy Queue:
        client.create_queue(QueueName='proxyqueue')

        yield client


@pytest.fixture(scope='function')
def buckets(s3):
    """Create Testing S3 buckets for testing the S3 stack."""
    # Create buckets:
    for i in range(0, 50):
        s3.create_bucket(Bucket=f'testbucket{i}')
        s3.put_bucket_tagging(
            Bucket=f'testbucket{i}',
            Tagging={
                'TagSet': [
                    {
                        'Key': 'theBucketName',
                        'Value': f'testbucket{i}'
                    }
                ]
            }
        )
        s3.put_bucket_lifecycle_configuration(Bucket=f'testbucket{i}', LifecycleConfiguration={
            'Rules': [
                {
                    'Expiration': {
                        'Days': 5
                    },
                    'ID': 'string',
                    'Filter': {
                        'Prefix': 'string',
                        'Tag': {
                            'Key': 'string',
                            'Value': 'string'
                        },
                        'And': {
                            'Prefix': 'string',
                            'Tags': [
                                {
                                    'Key': 'string',
                                    'Value': 'string'
                                },
                            ]
                        }
                    },
                    'Status': 'Enabled',
                    'NoncurrentVersionTransitions': [
                        {
                            'NoncurrentDays': 123,
                            'StorageClass': 'GLACIER'
                        },
                    ],
                    'NoncurrentVersionExpiration': {
                        'NoncurrentDays': 123
                    }
                }
            ]
        })


@pytest.fixture(scope='function')
def security_groups(ec2):
    """Creates security groups."""
    sg = ec2.create_security_group(
        Description='test security group',
        GroupName='test',
        VpcId='vpc-test'
    )

    # Tag it:
    ec2.create_tags(Resources=[sg['GroupId']], Tags=[
        {
            "Key": "Some",
            "Value": "Value"
        },
        {
            "Key": "Empty",
            "Value": ""
        }
    ])

    yield sg


@pytest.fixture(scope='function')
def vpcs(ec2):
    """Creates vpcs."""
    yield ec2.create_vpc(
        CidrBlock='192.168.1.1/32',
        AmazonProvidedIpv6CidrBlock=True,
        InstanceTenancy='default'
    )['Vpc']


@pytest.fixture(scope='function')
def mock_lambda_environment():
    """Mocks out the AWS Lambda environment context that AWS Lambda passes into the handler."""
    os.environ['SENTRY_ENABLED'] = 'f'

    class MockedContext:
        """Class that Mocks out the Lambda `context` object."""

        def get_remaining_time_in_millis(self):
            """Mocked method to return the remaining Lambda time in milliseconds."""
            return 99999

    return MockedContext()


@pytest.fixture(scope="function")
def iam_techs(iam):
    """Creates and setups up IAM things"""
    items = dict()

    # Role
    items['role'] = iam.create_role(
        Path='/',
        RoleName='Testrole',
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "ec2.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }),
        Description='Test Description'
    )['Role']

    # Instance Profile:
    iam.create_instance_profile(InstanceProfileName='TestIP')
    iam.add_role_to_instance_profile(InstanceProfileName='TestIP', RoleName='Testrole')

    # User
    items['user'] = iam.create_user(Path='/', UserName='Testuser')['User']

    # Group
    items['group'] = iam.create_group(Path='/', GroupName='Testgroup')['Group']

    # Policy
    items['policy'] = iam.create_policy(
        PolicyName='Testpolicy',
        Path='/',
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "s3:ListBucket",
                    "Resource": "*",
                    "Effect": "Allow",
                }
            ]
        }),
        Description='Test Policy'
    )['Policy']

    # SSL:
    items['serverCertificate'] = iam.upload_server_certificate(
        ServerCertificateName='TestserverCertificate',
        CertificateBody="""-----BEGIN CERTIFICATE-----
            MIIBpzCCARACCQCY5yOdxCTrGjANBgkqhkiG9w0BAQsFADAXMRUwEwYDVQQKDAxt
            b3RvIHRlc3RpbmcwIBcNMTgxMTA1MTkwNTIwWhgPMjI5MjA4MTkxOTA1MjBaMBcx
            FTATBgNVBAoMDG1vdG8gdGVzdGluZzCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkC
            gYEA1Jn3g2h7LD3FLqdpcYNbFXCS4V4eDpuTCje9vKFcC3pi/01147X3zdfPy8Mt
            ZhKxcREOwm4NXykh23P9KW7fBovpNwnbYsbPqj8Hf1ZaClrgku1arTVhEnKjx8zO
            vaR/bVLCss4uE0E0VM1tJn/QGQsfthFsjuHtwx8uIWz35tUCAwEAATANBgkqhkiG
            9w0BAQsFAAOBgQBWdOQ7bDc2nWkUhFjZoNIZrqjyNdjlMUndpwREVD7FQ/DuxJMj
            FyDHrtlrS80dPUQWNYHw++oACDpWO01LGLPPrGmuO/7cOdojPEd852q5gd+7W9xt
            8vUH+pBa6IBLbvBp+szli51V3TLSWcoyy4ceJNQU2vCkTLoFdS0RLd/7tQ==
            -----END CERTIFICATE-----""",
        PrivateKey='SomePrivateKey')

    metadata = items['serverCertificate'].pop('ServerCertificateMetadata')
    items['serverCertificate'].update(metadata)

    return items


@pytest.fixture(scope='function')
def current_security_group_table(dynamodb):
    """Create the Current Security Group Table."""
    from historical.security_group.models import CurrentSecurityGroupModel
    yield CurrentSecurityGroupModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope='function')
def durable_security_group_table(dynamodb):
    """Create the Durable Security Group Table."""
    from historical.security_group.models import DurableSecurityGroupModel
    yield DurableSecurityGroupModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope='function')
def current_vpc_table(dynamodb):
    """Create the Current VPC Table."""
    from historical.vpc.models import CurrentVPCModel
    yield CurrentVPCModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope='function')
def durable_vpc_table(dynamodb):
    """Create the Durable VPC Table."""
    from historical.vpc.models import DurableVPCModel
    yield DurableVPCModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope='function')
def current_s3_table(dynamodb):
    """Create the Current S3 Table."""
    from historical.s3.models import CurrentS3Model
    yield CurrentS3Model.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope='function')
def durable_s3_table(dynamodb):
    """Create the Durable S3 Table."""
    from historical.s3.models import DurableS3Model
    yield DurableS3Model.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope='function')
def current_iam_table(dynamodb):
    """Create the Current IAM Table."""
    from historical.iam.models import CurrentIAMModel
    yield CurrentIAMModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope='function')
def durable_IAM_table(dynamodb):
    """Create the Durable IAM Table."""
    from historical.iam.models import DurableIAMModel
    yield DurableIAMModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
