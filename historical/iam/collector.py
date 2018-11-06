"""
.. module: historical.iam.collector
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import logging

from botocore.exceptions import ClientError
from cloudaux.orchestration.aws.iam.user import get_user
from pynamodb.exceptions import PynamoDBConnectionError
from raven_python_lambda import RavenLambdaWrapper

from cloudaux.orchestration.aws.iam.group import get_group
from cloudaux.orchestration.aws.iam.managed_policy import get_managed_policy
from cloudaux.orchestration.aws.iam.role import get_role
from cloudaux.orchestration.aws.iam.server_certificate import get_server_certificate

from historical.common.cloudwatch import get_historical_base_info, get_event_name, filter_request_parameters, \
    get_account_id
from historical.common.exceptions import UnknownIAMTypeException, UnknownIAMNameException
from historical.common.sqs import group_records_by_type
from historical.constants import HISTORICAL_ROLE, LOGGING_LEVEL
from historical.common.util import deserialize_records, pull_tag_dict
from historical.iam import capitalize_tech
from historical.iam.models import CurrentIAMModel, CurrentIAMNameTypeAccountLookupIndex, VERSION

logging.basicConfig()
LOG = logging.getLogger('historical')
LOG.setLevel(LOGGING_LEVEL)

# Maps the IAM technology to the CloudWatch Event types
EVENT_MAPPER = {
    'group': {
        'AttachGroupPolicy',
        'CreateGroup',
        'DeleteGroup',
        'DeleteGroupPolicy',
        'DetachGroupPolicy',
        'PutGroupPolicy',
        'UpdateGroup'
    },
    'policy': {
        'CreatePolicy',
        'CreatePolicyVersion',
        'DeletePolicy',
        'DeletePolicyVersion',
        'SetDefaultPolicyVersion',
    },
    'role': {
        'AddRoleToInstanceProfile',
        'AttachRolePolicy',
        'CreateRole',
        'CreateServiceLinkedRole',
        'DeleteRole',
        'DeleteRolePermissionsBoundary',
        'DeleteRolePolicy',
        'DeleteServiceLinkedRole',
        'DetachRolePolicy',
        'PutRolePermissionsBoundary',
        'PutRolePolicy',
        'RemoveRoleFromInstanceProfile',
        'UpdateRole',
        'UpdateRoleDescription'
    },
    'serverCertificate': {
        'DeleteServerCertificate',
        'UpdateServerCertificate',
        'UploadServerCertificate'
    },
    'user': {
        'AddUserToGroup',
        'AttachUserPolicy',
        'CreateAccessKey',
        'CreateUser',
        'DeleteAccessKey',
        'DeleteSSHPublicKey',
        'DeleteSigningCertificate',
        'DeleteUser',
        'DeleteUserPermissionsBoundary',
        'DeleteUserPolicy',
        'DetachUserPolicy',
        'PutUserPermissionsBoundary',
        'PutUserPolicy',
        'RemoveUserFromGroup',
        'UpdateAccessKey',
        'UpdateSSHPublicKey',
        'UpdateSigningCertificate',
        'UpdateUser',
        'UploadSSHPublicKey',
        'UploadSigningCertificate'
    }
}

DELETE_EVENTS = [
    'DeleteGroup',
    'DeletePolicy',
    'DeleteRole',
    'DeleteServerCertificate',
    'DeleteServiceLinkedRole',
    'DeleteUser'
]


def _update_arn_logic(record: dict, iam_details: dict, iam_data: dict, new_field: str):
    """This will handle the special update API call logic. These calls change the ARN of the resource in question.

    :param record:
    :param iam_details:
    :param iam_data:
    :param new_field: The name of the new name field in the Update API parameter
    :return:
    """
    # This COULD be a no-op -- that is if no new certificate name or path is provided.
    new_name = filter_request_parameters(new_field, record)
    new_path = filter_request_parameters('newPath', record)

    if not new_name and not new_path:
        # No-op event.
        return None

    # This can either be renamed or re-path'd. In any case, this changes the ARN.
    # This will result in a new item being created -- and a deletion being set for the old item.
    else:
        delete_model = _create_delete_model(iam_details, iam_data)

        if not delete_model:
            # If for whatever reason we can't find the old item... We'll still try to fetch the new item's details.
            logging.error(f"[?] Received an update event for {iam_details['type']}, but can't find the old record. "
                          f'The received event is: {record}.')

        else:
            _delete_item_from_dynamo(delete_model, record)

    # Return the new name of the item to fetch:
    return new_name or iam_details['name']


def _group_logic(record: dict, iam_details: dict, iam_data: dict, api_kwargs: dict):
    """All IAM Group logic is handled here. This handles the nuances of IAM Groups."""
    if get_event_name(record) in ['UpdateGroup']:
        new_name = _update_arn_logic(record, iam_data, iam_details, 'newGroupName')

        # At this point, we are going to fetch the latest and greatest item like normal
        # (but with the new name provided):
        iam_details['name'] = new_name or iam_details['name']

    return get_group({'GroupName': iam_details['name']}, **api_kwargs)


def _role_logic(record: dict, iam_details: dict, iam_data: dict, api_kwargs: dict):
    """All IAM Role logic is handled here. No real nuances with IAM roles at this point."""
    return get_role({'RoleName': iam_details['name']}, **api_kwargs)


def _policy_logic(record: dict, iam_details: dict, iam_data: dict, api_kwargs: dict):
    """All IAM Managed Policy logic is handled here. This handles the nuances of Manged Policies."""
    # If this is updating the MP -- we only care if the default version is being updated. Otherwise, drop
    # the event as a no-op.
    if get_event_name(record) in ['CreatePolicyVersion', 'DeletePolicyVersion']:
        if not filter_request_parameters('setAsDefault', record):
            return None

    return get_managed_policy({'Arn': iam_details['arn']}, **api_kwargs)


def _server_certificate_logic(record: dict, iam_details: dict, iam_data: dict, api_kwargs: dict):
    """All Server Certificate logic is handled here. This needs to handle the nuances of Server Certificates."""
    if get_event_name(record) in ['UpdateServerCertificate']:
        new_name = _update_arn_logic(record, iam_data, iam_details, 'newServerCertificateName')

        # At this point, we are going to fetch the latest and greatest item like normal
        # (but with the new name provided):
        iam_details['name'] = new_name or iam_details['name']

    return get_server_certificate({'ServerCertificateName': iam_details['name']}, **api_kwargs)


def _user_logic(record: dict, iam_details: dict, iam_data: dict, api_kwargs: dict):
    """All IAM User logic is handled here. This handles the nuances of IAM Users."""
    if get_event_name(record) in ['UpdateUser']:
        new_name = _update_arn_logic(record, iam_data, iam_details, 'newUserName')

        # At this point, we are going to fetch the latest and greatest item like normal
        # (but with the new name provided):
        iam_details['name'] = new_name or iam_details['name']

    return get_user({'UserName': iam_details['name']}, **api_kwargs)


# Maps the IAM technology to the function to fetch the details from it.
FUNC_MAPPER = {
    'group': _group_logic,
    'policy': _policy_logic,
    'role': _role_logic,
    'serverCertificate': _server_certificate_logic,
    'user': _user_logic
}


def _get_policy_arn(event_name: str, record: dict):
    """Attempts to fetch a Managed Policy's ARN -- which for the use case of Historical is also the 'name' field."""
    # If this is a managed policy creation, then fetch the 'Arn' field:
    if event_name == 'CreatePolicy':
        # Look in the nested response:
        return record['detail']['responseElements']['policy']['arn']

    # Check for 'PolicyArn'
    return filter_request_parameters('policyArn', record)


def _get_iam_details(record: dict):
    """Gets the IAM type and name from the record.

    :returns: Tuple of the IAM Type and the corresponding tech.
    """
    # Is this a poller event?
    if get_event_name(record) == 'PollIAM':
        return record['detail']['requestParameters']

    iam_details = {}

    # Cross-reference the type with the event:
    for iam_type, apis in EVENT_MAPPER.items():
        if get_event_name(record) in apis:
            iam_details['type'] = iam_type
            break

    if not iam_details.get('type'):
        raise UnknownIAMTypeException("[X] Was not able to determine the IAM tech of this event. "
                                      f"Here is the event: {record}.")

    # For Managed Policies, we only care about the ARN (which is what we query the API for):
    if iam_details['type'] == 'policy':
        name = _get_policy_arn(get_event_name(record), record)

        if not name:
            raise UnknownIAMNameException('[X] Was not able to obtain the name of the IAM item '
                                          f'from this event: {record}.')

        iam_details['name'] = name
        iam_details['arn'] = name

    else:
        # Fetch the name:
        iam_details['name'] = filter_request_parameters(f"{iam_details['type']}Name", record)

        if not iam_details['name']:
            raise UnknownIAMNameException('[X] Was not able to obtain the name of the IAM item '
                                          f'from this event: {record}.')

        # Fetch the ARN -- which is not always present in the event:
        arn = None
        if record['detail'].get('responseElements'):
            arn = (record['detail']['responseElements'].get(iam_details['type']) or {}).get('arn')

        if not arn:
            arn = _query_arn(iam_details['name'], iam_details['type'], get_account_id(record))

        # The ARN could still be missing at this point -- this is OK if we haven't seen this item before:
        iam_details['arn'] = arn

    return iam_details


# def get_arn(iam_type, iam_name, account_id, path):
#     """Creates the proper IAM type ARN."""
#     path = path or '/'
#
#     if iam_type == 'serverCertificate':
#         iam_type = 'server-certificate'
#
#     return f"arn:aws:iam::{account_id}:{iam_type}{path}{iam_name}"


def _query_arn(name: str, iam_type: str, account_id: str):
    """Queries the GSI to obtain the ARN of an existing item.

    This is necessary because ARNs with IAM are not easily predictable due to the Path being customized (and not
    passed in the CWE).

    :param name: The name of the IAM item.
    :param iam_type: The type of the item.
    :param account_id: The account id of the item.
     """
    result = list(CurrentIAMNameTypeAccountLookupIndex.query(f'{name}+{iam_type}+{account_id}'))

    if not result:
        return None

    else:
        return result[0].arn


def _create_delete_model(iam_details: dict, iam_data: dict):
    """Create an IAM model from a record."""
    # For some reason it wasn't found...
    if not iam_details.get('arn'):
        return None

    iam_data.update({'configuration': {}})

    items = list(CurrentIAMModel.query(iam_details['arn'], limit=1))

    if items:
        model_dict = items[0].__dict__['attribute_values'].copy()
        model_dict.update(iam_data)
        model = CurrentIAMModel(**model_dict)
        return model

    return None


def _delete_item_from_dynamo(model: CurrentIAMModel, record: dict):
    """Performs the DynamoDB deletion from the Current Table."""
    LOG.debug(f'[-] Deleting Dynamodb Records. Hash Key: {model.arn}')
    try:
        # Need to check if the event is NEWER than the previous event in case
        # events are out of order. This could *possibly* happen if something
        # was deleted, and then quickly re-created. It could be *possible* for the
        # deletion event to arrive after the creation event. Thus, this will check
        # if the current event timestamp is newer and will only delete if the deletion
        # event is newer.
        model.save(condition=(CurrentIAMModel.eventTime <= record['detail']['eventTime']))
        model.delete()
    except PynamoDBConnectionError as pdce:
        LOG.warning(f'[X] Unable to IAM object. It does not exist, or this deletion event is stale. '
                    f'Record: {record}. The specific exception is: {pdce}.')


def capture_delete_records(records: list):
    """Writes all of our delete events to DynamoDB."""
    for rec in records:
        iam_details = _get_iam_details(rec)
        iam_data = get_historical_base_info(rec)

        model = _create_delete_model(iam_details, iam_data)
        if model:
            _delete_item_from_dynamo(model, rec)
        else:
            LOG.warning(f'[?] Unable to delete IAM object. It does not exist. Record: {rec}')


def capture_update_records(records: list):
    """Process the requests for IAM update requests"""
    for rec in records:
        iam_details = _get_iam_details(rec)
        iam_data = get_historical_base_info(rec)

        # Describe the tech in question:
        item = None
        try:
            api_kwargs = {
                'account_number': rec['account'],
                'assume_role': HISTORICAL_ROLE,
                'region': rec['region'],
            }
            item = FUNC_MAPPER[iam_details['type']](rec, iam_details, iam_data, api_kwargs)

        except ClientError as exc:
            if exc.response['Error']['Code'] == 'NoSuchEntity':
                continue

        if not item:
            continue

        item_name = item.pop(f"{capitalize_tech(iam_details['type'])}Name")
        iam_data.update({
            'arn': item.pop('Arn'),
            'Name': item_name,
            'Type': iam_details['type'],
            'Tags': pull_tag_dict(item),
            'configuration': item,
            'NameTypeAccount': f'{item_name}+{iam_details["type"]}+{rec["account"]}',
            'version': VERSION
        })

        # Clean Up:
        if get_event_name(rec) == 'PollIAM':
            iam_data.pop('requestParameters', None)

        LOG.debug(f'[+] Writing Dynamodb Record. Records: {iam_data}')
        current_revision = CurrentIAMModel(**iam_data)
        current_revision.save()


@RavenLambdaWrapper()
def handler(event: dict, context: object):  # pylint: disable=W0613
    """
    Historical IAM event collector.

    This collector is responsible for processing CloudWatch events and polling events.
    """
    records = deserialize_records(event['Records'])

    # Split records into two groups, update and delete.
    # We don't want to query for deleted records.
    update_records, delete_records = group_records_by_type(records, DELETE_EVENTS)
    capture_delete_records(delete_records)

    # Filter out error events
    update_records = [e for e in update_records if not e['detail'].get('errorCode')]

    LOG.debug('[@] Processing update records...')
    capture_update_records(update_records)
    LOG.debug('[@] Completed processing of update records.')

    LOG.debug('[@] Successfully updated current Historical table')
