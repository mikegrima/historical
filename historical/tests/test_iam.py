"""
.. module: historical.tests.test_iam
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json
import os
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from historical.common.sqs import get_queue_url
from historical.iam import capitalize_tech
from historical.iam.models import IAM_TYPES, IAM_POLLING_SCHEMA, VERSION
from historical.models import HistoricalPollerTaskEventModel
from historical.tests.factories import CloudwatchEventFactory, DetailFactory, DynamoDBDataFactory, \
    DynamoDBRecordFactory, RecordsFactory, serialize, SnsDataFactory, SQSDataFactory, UserIdentityFactory


def _get_arn(iam_type: str, iam_name: str, account_id: str):
    """Creates an IAM ARN for use with testing."""
    if iam_type == 'serverCertificate':
        iam_type = 'server-certificate'

    return f'arn:aws:iam::{account_id}:{iam_type}/{iam_name}'


def test_schema_serialization():
    """Test S3 custom event schemas."""
    # Make an object to serialize:
    iam_details = {
        'name': 'therole',
        'type': 'role',
        'arn': 'arn:aws:iam::012345678910:role/therole'
    }

    serialized = IAM_POLLING_SCHEMA.serialize_me("012345678910", iam_details)

    # The dumped data:
    loaded_serialized = json.loads(serialized)

    # The dumped data loaded again:
    loaded_data = IAM_POLLING_SCHEMA.loads(serialized).data

    assert loaded_serialized["version"] == loaded_data["version"] == "1"
    assert loaded_serialized["detail-type"] == loaded_data["detail_type"] == "Poller"
    assert loaded_serialized["source"] == loaded_data["source"] == "historical"
    assert loaded_serialized["account"] == loaded_data["account"] == "012345678910"
    assert loaded_serialized["detail"]["eventTime"] == loaded_data["detail"]["event_time"]
    assert loaded_serialized["detail"]["eventSource"] == loaded_data["detail"]["event_source"] == \
        "historical.iam.poller"
    assert loaded_serialized["detail"]["eventName"] == loaded_data["detail"]["event_name"] == "PollIAM"
    assert loaded_serialized["detail"]["requestParameters"]["name"] == \
        loaded_data["detail"]["request_parameters"]["name"] == "therole"
    assert loaded_serialized["detail"]["requestParameters"]["type"] == \
        loaded_data["detail"]["request_parameters"]["type"] == "role"
    assert loaded_serialized["detail"]["requestParameters"]["arn"] == \
        loaded_data["detail"]["request_parameters"]["arn"] == "arn:aws:iam::012345678910:role/therole"


def test_current_table(current_iam_table):  # pylint: disable=W0613
    """Tests for the Current PynamoDB model."""
    from historical.iam.models import CurrentIAMNameTypeAccountLookupIndex, CurrentIAMModel

    role = {
        "arn": "arn:aws:iam::012345678910:role/historicalrole",
        "principalId": "joe@example.com",
        "userIdentity": {
            "sessionContext": {
                "userName": "oUEKDvMsBwpk",
                "type": "Role",
                "arn": "arn:aws:iam::123456789012:role/historical_poller",
                "principalId": "AROAIKELBS2RNWG7KASDF",
                "accountId": "123456789012"
            },
            "principalId": "AROAIKELBS2RNWG7KASDF:joe@example.com"
        },
        'version': VERSION,
        "accountId": "012345678910",
        "eventTime": "2017-09-08T00:34:34Z",
        "eventSource": "aws.iam",
        "Name": "historicalrole",
        "Tags": {},
        "configuration": {},
        "Type": "role",
        "NameTypeAccount": "historicalrole+role+012345678910"
    }

    CurrentIAMModel(**role).save()

    assert len(list(CurrentIAMModel.scan())) == 1

    # Query the index:
    assert len(list(CurrentIAMNameTypeAccountLookupIndex.query('historicalrole+role+012345678910'))) == 1


# def test_durable_table(durable_s3_table):  # pylint: disable=W0613
#     """Tests for the Durable PynamoDB model."""
#     from historical.s3.models import DurableS3Model
#
#     # We are explicit about our eventTimes because as RANGE_KEY it will need to be unique.
#     S3_BUCKET['eventTime'] = datetime(2017, 5, 11, 23, 30)
#     S3_BUCKET.pop("eventSource")
#     DurableS3Model(**S3_BUCKET).save()
#     items = list(DurableS3Model.query('arn:aws:s3:::testbucket1'))
#     assert len(items) == 1
#     assert not getattr(items[0], "ttl", None)
#
#     S3_BUCKET['eventTime'] = datetime(2017, 5, 12, 23, 30)
#     DurableS3Model(**S3_BUCKET).save()
#     items = list(DurableS3Model.query('arn:aws:s3:::testbucket1'))
#     assert len(items) == 2


def make_poller_events():
    """A sort-of fixture to make polling events for tests."""
    from historical.iam.poller import poller_tasker_handler as handler
    handler({}, None)

    # Need to ensure that all of the accounts and regions were properly tasked (only 1 region for IAM):
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue_url = get_queue_url(os.environ['POLLER_TASKER_QUEUE_NAME'])
    messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)['Messages']

    # 'Body' needs to be made into 'body' for proper parsing later:
    for msg in messages:
        msg['body'] = msg.pop('Body')

    return messages


def test_poller_tasker_handler(mock_lambda_environment, historical_sqs, swag_accounts):  # pylint: disable=W0613
    """Test the Poller tasker."""
    from historical.common.accounts import get_historical_accounts
    from historical.constants import CURRENT_REGION

    messages = make_poller_events()
    all_historical_accounts = get_historical_accounts()
    assert len(messages) == len(all_historical_accounts) * len(IAM_TYPES) + 1   # The extra 1 is for AWS policies.

    # Verify that the data is all set correctly:
    type_verify = list(IAM_TYPES) + ['policy']

    for msg in messages:
        poller_events = HistoricalPollerTaskEventModel().loads(msg['body']).data
        assert poller_events['account_id'] == all_historical_accounts[0]['id']
        assert poller_events['region'] == CURRENT_REGION
        type_verify.remove(poller_events['extra']['iam_type'])

    assert not type_verify


# pylint: disable=W0613,R0914
def test_poller_processor_handler(historical_role, iam, iam_techs, buckets, mock_lambda_environment, historical_sqs,
                                  swag_accounts):
    """Test the Poller's processing component that tasks the collector."""
    from historical.iam.poller import poller_processor_handler as handler

    # Create the events and SQS records:
    messages = make_poller_events()
    event = json.loads(json.dumps(RecordsFactory(records=messages), default=serialize))

    # Run the collector:
    handler(event, None)

    # Need to ensure that 51 total buckets were added into SQS:
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue_url = get_queue_url(os.environ['POLLER_QUEUE_NAME'])

    iam_assets = {iam_type: {} for iam_type in IAM_TYPES}

    # Loop through the queue and make sure all the things are accounted for:
    while True:
        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get('Messages', [])
        if not messages:
            break

        message_ids = []

        for msg in messages:
            message_ids.append({"Id": msg['MessageId'], "ReceiptHandle": msg['ReceiptHandle']})
            data = IAM_POLLING_SCHEMA.loads(msg['Body']).data

            iam_assets[data['detail']['request_parameters']['type']][data['detail']['request_parameters']['name']] \
                = True

            assert data['detail']['request_parameters']['arn']

        sqs.delete_message_batch(QueueUrl=queue_url, Entries=message_ids)

    # Verify that all the things are there:
    # Roles:
    for r in iam.list_roles()['Roles']:
        del iam_assets['role'][r['RoleName']]

    # Users:
    for u in iam.list_users()['Users']:
        del iam_assets['user'][u['UserName']]

    # Groups:
    for g in iam.list_groups()['Groups']:
        del iam_assets['group'][g['GroupName']]

    # Local Policies:
    for p in iam.list_policies(Scope='Local')['Policies']:
        del iam_assets['policy'][p['PolicyName']]

    # AWS Policies:
    for p in iam.list_policies(Scope='AWS', MaxItems=200)['Policies']:
        del iam_assets['policy'][p['PolicyName']]

    # Server Certificates:
    for c in iam.list_server_certificates()['ServerCertificateMetadataList']:
        del iam_assets['serverCertificate'][c['ServerCertificateName']]

    # All items should be accounted for:
    for iam_type in IAM_TYPES:
        assert not iam_assets[iam_type]

    # Check that an exception raised doesn't break things:
    import historical.iam.poller

    def mocked_poller(account, stream, randomize_delay=0):  # pylint: disable=W0613
        raise ClientError({"Error": {"Message": "", "Code": "AccessDenied"}}, "sts:AssumeRole")

    old_method = historical.iam.poller.produce_events  # For pytest inter-test issues...
    historical.iam.poller.produce_events = mocked_poller
    handler(event, None)
    historical.iam.poller.produce_events = old_method
    # ^^ No exception = pass


# TODO:
"""
We need to add tests for:
1.) Testing the collector for Updating Cert names
2.) Testing the collector for Updating User names
3.) Testing the collector for Updating Group names
4.) Testing the collector for Updating Policy versions that are not default
"""


# pylint: disable=W0613
def test_collector(historical_role, mock_lambda_environment, swag_accounts, current_iam_table, iam, iam_techs):
    """Test the Collector."""
    from historical.iam.models import CurrentIAMModel
    from historical.iam.collector import handler

    # Testing polling first:
    now = datetime.utcnow().replace(tzinfo=None, microsecond=0)
    for tech in IAM_TYPES:
        create_event = CloudwatchEventFactory(
            detail=DetailFactory(
                requestParameters={
                    'name': f'Test{tech}',
                    'type': tech,
                    'arn': _get_arn(tech, f'Test{tech}', '123456789012')
                },
                eventSource="historical.iam.poller",
                eventName="PollIAM",
                eventTime=now
            )
        )
        data = json.dumps(create_event, default=serialize)
        data = RecordsFactory(records=[SQSDataFactory(body=data)])
        data = json.dumps(data, default=serialize)
        data = json.loads(data)

        handler(data, mock_lambda_environment)

    # Check that they are all there:
    result = list(CurrentIAMModel.scan())
    assert len(result) == len(IAM_TYPES)

    # Verify that all the things are there:
    for item in result:
        assert iam_techs[item.Type]['Arn'] == item.arn
        assert iam_techs[item.Type][f'{capitalize_tech(item.Type)}Name'] == item.Name
        assert item.NameTypeAccount == f'{item.Name}+{item.Type}+{item.accountId}'
        assert not item.requestParameters
        assert item.eventName == 'PollIAM'
        assert item.eventSource == 'historical.iam.poller'

    # And deletion:
    for tech in IAM_TYPES:
        if tech == 'policy':
            request_parameters = {'policyArn': iam_techs['policy']['Arn']}
        else:
            request_parameters = {f"{tech}Name": f'Test{tech}'}

        delete_event = CloudwatchEventFactory(
            detail=DetailFactory(
                requestParameters=request_parameters,
                eventSource='aws.iam',
                eventName=f"Delete{capitalize_tech(tech)}",
                eventTime=now
            )
        )
        data = json.dumps(delete_event, default=serialize)
        data = RecordsFactory(records=[SQSDataFactory(body=data)])
        data = json.dumps(data, default=serialize)
        data = json.loads(data)
        handler(data, mock_lambda_environment)

    assert not list(CurrentIAMModel.scan())

    # And add them back (no polling this time)
    for tech in IAM_TYPES:
        if tech == 'serverCertificate':
            event_name = 'UploadServerCertificate'
        else:
            event_name = f"Create{capitalize_tech(tech)}"

        # The ARN will be lowercase 'arn' in the CWE:
        response_elements = json.loads(json.dumps(iam_techs[tech], default=serialize).replace('Arn', 'arn'))

        delete_event = CloudwatchEventFactory(
            detail=DetailFactory(
                requestParameters={f"{tech}Name": f'Test{tech}'},
                responseElements={tech: response_elements},
                eventSource='iam.amazonaws.com',
                eventName=event_name,
                eventTime=now
            )
        )
        data = json.dumps(delete_event, default=serialize)
        data = RecordsFactory(records=[SQSDataFactory(body=data)])
        data = json.dumps(data, default=serialize)
        data = json.loads(data)
        handler(data, mock_lambda_environment)

    # Verify that all the things are there:
    result = list(CurrentIAMModel.scan())
    assert len(result) == len(IAM_TYPES)
    for item in result:
        assert iam_techs[item.Type]['Arn'] == item.arn
        assert iam_techs[item.Type][f'{capitalize_tech(item.Type)}Name'] == item.Name
        assert item.NameTypeAccount == f'{item.Name}+{item.Type}+{item.accountId}'
        assert item.requestParameters[f'{item.Type}Name'] == f'Test{item.Type}'
        assert item.eventName
        assert item.eventSource == 'iam.amazonaws.com'


def test_collector_on_deleted_resources(historical_role, mock_lambda_environment, swag_accounts, current_iam_table,
                                        iam, iam_techs):
    """Test the collector's logic for a deleted resource."""
    from historical.iam.models import CurrentIAMModel
    from historical.iam.collector import handler

    # Delete all the resources first:
    iam.delete_role(RoleName='Testrole')
    iam.delete_user(UserName='Testuser')
    iam.delete_group(GroupName='Testgroup')
    iam.delete_server_certificate(ServerCertificateName='TestserverCertificate')
    iam.delete_policy(PolicyArn=iam_techs['policy']['Arn'])

    # If an event arrives on a resource that is deleted, then it should skip and wait until the Deletion event arrives.
    # And add them back (no polling this time)
    for tech in IAM_TYPES:
        if tech == 'serverCertificate':
            event_name = 'UploadServerCertificate'
        else:
            event_name = f"Create{capitalize_tech(tech)}"

        # The ARN will be lowercase 'arn' in the CWE:
        response_elements = json.loads(json.dumps(iam_techs[tech], default=serialize).replace('Arn', 'arn'))

        delete_event = CloudwatchEventFactory(
            detail=DetailFactory(
                requestParameters={f"{tech}Name": f'Test{tech}'},
                responseElements={tech: response_elements},
                eventSource='iam.amazonaws.com',
                eventName=event_name,
                eventTime=now
            )
        )
        data = json.dumps(delete_event, default=serialize)
        data = RecordsFactory(records=[SQSDataFactory(body=data)])
        data = json.dumps(data, default=serialize)
        data = json.loads(data)
        handler(data, mock_lambda_environment)


    create_event = CloudwatchEventFactory(
        detail=DetailFactory(
            requestParameters={
                "bucketName": "not-a-bucket"
            },
            eventSource="aws.s3",
            eventName="PutBucketPolicy",
        )
    )
    create_event_data = json.dumps(create_event, default=serialize)
    data = RecordsFactory(records=[SQSDataFactory(body=create_event_data)])
    data = json.dumps(data, default=serialize)
    data = json.loads(data)

    handler(data, mock_lambda_environment)
    assert CurrentS3Model.count() == 0


# # pylint: disable=W0613,R0914,R0915
# def test_differ(current_s3_table, durable_s3_table, mock_lambda_environment):
#     """Test the Differ"""
#     from historical.s3.models import DurableS3Model
#     from historical.s3.differ import handler
#     from historical.models import TTL_EXPIRY
#
#     ttl = int(time.time() + TTL_EXPIRY)
#     new_bucket = S3_BUCKET.copy()
#     new_bucket['eventTime'] = datetime(year=2017, month=5, day=12, hour=10, minute=30, second=0).isoformat() + 'Z'
#     new_bucket["ttl"] = ttl
#     ddb_record = json.dumps(
#         DynamoDBRecordFactory(
#             dynamodb=DynamoDBDataFactory(NewImage=new_bucket, Keys={'arn': new_bucket['arn']}),
#             eventName='INSERT'
#         ),
#         default=serialize
#     )
#
#     new_item = RecordsFactory(records=[SQSDataFactory(body=json.dumps(ddb_record, default=serialize))])
#     data = json.loads(json.dumps(new_item, default=serialize))
#     handler(data, mock_lambda_environment)
#     assert DurableS3Model.count() == 1
#
#     # Test duplicates don't change anything:
#     data = json.loads(json.dumps(new_item, default=serialize))
#     handler(data, mock_lambda_environment)
#     assert DurableS3Model.count() == 1
#
#     # Add an update:
#     new_changes = S3_BUCKET.copy()
#     new_date = datetime(year=2017, month=5, day=12, hour=11, minute=30, second=0).isoformat() + 'Z'
#     new_changes["eventTime"] = new_date
#     new_changes["Tags"] = {"ANew": "Tag"}
#     new_changes["configuration"]["Tags"] = {"ANew": "Tag"}
#     new_changes["ttl"] = ttl
#     data = json.dumps(
#         DynamoDBRecordFactory(
#             dynamodb=DynamoDBDataFactory(NewImage=new_changes, Keys={'arn': new_changes['arn']}),
#             eventName='MODIFY'),
#         default=serialize)
#     data = RecordsFactory(records=[SQSDataFactory(body=json.dumps(data, default=serialize))])
#     data = json.loads(json.dumps(data, default=serialize))
#     handler(data, mock_lambda_environment)
#     results = list(DurableS3Model.query("arn:aws:s3:::testbucket1"))
#     assert len(results) == 2
#     assert results[1].Tags["ANew"] == results[1].configuration.attribute_values["Tags"]["ANew"] == "Tag"
#     assert results[1].eventTime == new_date
#
#     # And deletion (ensure new record -- testing TTL): -- And with SNS for testing completion
#     delete_bucket = S3_BUCKET.copy()
#     delete_bucket["eventTime"] = datetime(year=2017, month=5, day=12, hour=12, minute=30, second=0).isoformat() + 'Z'
#     delete_bucket["ttl"] = ttl
#     data = json.dumps(
#         DynamoDBRecordFactory(
#             dynamodb=DynamoDBDataFactory(OldImage=delete_bucket, Keys={'arn': delete_bucket['arn']}),
#             eventName='REMOVE',
#             userIdentity=UserIdentityFactory(type='Service', principalId='dynamodb.amazonaws.com')),
#         default=serialize)
#     data = RecordsFactory(records=[SQSDataFactory(body=json.dumps(SnsDataFactory(Message=data), default=serialize))])
#     data = json.loads(json.dumps(data, default=serialize))
#     handler(data, mock_lambda_environment)
#     assert DurableS3Model.count() == 3
