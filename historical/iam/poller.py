"""
.. module: historical.iam.poller
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import os
import logging

from botocore.exceptions import ClientError
from cloudaux import sts_conn
from cloudaux.aws.decorators import rate_limited

from raven_python_lambda import RavenLambdaWrapper

from historical.common.sqs import get_queue_url, produce_events
from historical.common.util import deserialize_records
from historical.constants import CURRENT_REGION, HISTORICAL_ROLE, LOGGING_LEVEL, RANDOMIZE_POLLER
from historical.models import HistoricalPollerTaskEventModel
from historical.iam.models import IAM_POLLING_SCHEMA, IAM_TYPES
from historical.common.accounts import get_historical_accounts

logging.basicConfig()
LOG = logging.getLogger("historical")
LOG.setLevel(LOGGING_LEVEL)


@sts_conn('iam')
@rate_limited()
def list_groups(**kwargs):
    """Lists all groups in a given account. Not using CloudAux's due to manual pagination need."""
    return kwargs.pop('client').list_groups(**kwargs)


@sts_conn('iam')
@rate_limited()
def list_roles(**kwargs):
    """Lists all roles in a given account. Not using CloudAux's due to manual pagination need."""
    return kwargs.pop('client').list_roles(**kwargs)


@sts_conn('iam')
@rate_limited()
def list_users(**kwargs):
    """Lists all users in a given account. Not using CloudAux's due to manual pagination need."""
    return kwargs.pop('client').list_users(**kwargs)


@sts_conn('iam')
@rate_limited()
def list_managed_policies(**kwargs):
    """Lists all managed policies in a given account. Not using CloudAux's due to manual pagination need."""
    return kwargs.pop('client').list_policies(**kwargs)


@sts_conn('iam')
@rate_limited()
def list_server_certificates(**kwargs):
    """Lists all IAM SSL certificates in a given account. Not using CloudAux's due to manual pagination need."""
    return kwargs.pop('client').list_server_certificates(**kwargs)


# Mapping of the:
# 1. (func) Function that will list the IAM type.
# 2. (result_key) The key that contains the list of items from the list result.
# 3. (request_field) The field within the result that contains the value the Collector needs to fetch
#    This value in the response will be the capitalized version. What will be sent over to CWE will have the first
#    character lowercased.
TECH_MAPPER = {
    'group':
        {'func': list_groups, 'result_key': 'Groups'},
    'policy':
        {'func': list_managed_policies, 'result_key': 'Policies'},
    'role':
        {'func': list_roles, 'result_key': 'Roles'},
    'serverCertificate':
        {'func': list_server_certificates, 'result_key': 'ServerCertificateMetadataList'},
    'user':
        {'func': list_users, 'result_key': 'Users'}
}


@RavenLambdaWrapper()
def poller_tasker_handler(event: dict, context: object):  # pylint: disable=W0613
    """
    Historical IAM Poller Tasker.

    The Poller is run at a set interval in order to ensure that changes do not go undetected by Historical.

    Historical pollers generate `polling events` which simulate changes. These polling events contain configuration
    data such as the account/region defining where the collector should attempt to gather data from.

    This is the entry point. This will task subsequent Poller lambdas to list all of a given resource in a select few
    AWS accounts.
    """
    LOG.debug('[@] Running Poller Tasker...')

    queue_url = get_queue_url(os.environ.get('POLLER_TASKER_QUEUE_NAME', 'HistoricalIAMPollerTasker'))
    poller_task_schema = HistoricalPollerTaskEventModel()

    events = []
    for account in get_historical_accounts():
        for tech in IAM_TYPES:
            events.append(poller_task_schema.serialize_me(account['id'], CURRENT_REGION, extra={'iam_type': tech}))

    # Lastly append the global AWS IAM Manged Policies (just use the last account):
    events.append(poller_task_schema.serialize_me(account['id'], CURRENT_REGION,
                                                  extra={'iam_type': 'policy', 'scope': 'aws'}))

    try:
        produce_events(events, queue_url, randomize_delay=RANDOMIZE_POLLER)
    except ClientError as exc:
        LOG.error(f'[X] Unable to generate poller tasker events! Reason: {exc}')

    LOG.debug('[@] Finished tasking the pollers.')


@RavenLambdaWrapper()
def poller_processor_handler(event: dict, context: object):  # pylint: disable=W0613
    """
    Historical IAM Poller Processor.

    This will receive events from the Poller Tasker, and will list all objects of a given technology for an
    account/region pair. This will generate `polling events` which simulate changes. These polling events contain
    configuration data such as the account/region defining where the collector should attempt to gather data from.
    """
    LOG.debug('[@] Running Poller...')

    collector_poller_queue_url = get_queue_url(os.environ.get('POLLER_QUEUE_NAME', 'HistoricalIAMPoller'))
    takser_queue_url = get_queue_url(os.environ.get('POLLER_TASKER_QUEUE_NAME', 'HistoricalIAMPollerTasker'))

    poller_task_schema = HistoricalPollerTaskEventModel()
    records = deserialize_records(event['Records'])

    for record in records:
        # Skip accounts that have role assumption errors:
        try:
            # API call kwargs:
            api_kwargs = {
                'account_number': record['account_id'],
                'assume_role': HISTORICAL_ROLE,
                'region': record['region'],
                'MaxItems': 200
            }

            # Are we paginating?
            if record.get('Marker'):
                api_kwargs['Marker'] = record['Marker']

            # Managed policies require additional filtering:
            if record['Extra']['iam_type'] == 'policy':
                # Is this the call to list all the global AWS managed policies?
                if not record['Extra'].get('scope'):
                    api_kwargs['Scope'] = 'Local'

                else:
                    api_kwargs['Scope'] = 'AWS'

            # Describe the given IAM tech that is requested for the given account:
            iam_results = TECH_MAPPER[record['Extra']['iam_type']]['func'](**api_kwargs)

            # FIRST THINGS FIRST: Did we get a `Marker`? If so, we need to enqueue that ASAP because
            # 'Marker`s expire in 60 seconds!
            if iam_results.get('Marker'):
                logging.debug(f"[-->] Pagination required Type: {record['Extra']['iam_type']}, Token:"
                              f"{iam_results['Marker']}. Tasking continuation.")
                produce_events(
                    [poller_task_schema.serialize_me(record['account_id'], record['region'],
                                                     next_token=iam_results['Marker'], extra=record['Extra'])],
                    takser_queue_url)

            # Task the collector to perform all the DDB logic:
            events = []
            for i in iam_results.get(TECH_MAPPER[record['Extra']['iam_type']]['result_key'], []):
                # If an ARN is provided in the results, then we need to shove that in.
                payload = {
                    'name': i[f"{record['Extra']['iam_type'][0].upper() + record['Extra']['iam_type'][1:]}Name"],
                    'type': record['Extra']['iam_type'],
                    'arn': i['Arn']
                }

                events.append(IAM_POLLING_SCHEMA.serialize_me(record['account_id'], payload))

            LOG.debug(f"[@] Finished generating IAM {record['Extra']['iam_type']} polling events for account: "
                      f"{record['account_id']}. Events Created: {len(record['account_id'])}")
            produce_events(events, collector_poller_queue_url, randomize_delay=RANDOMIZE_POLLER)

        except ClientError as exc:
            LOG.error(f"[X] Unable to generate IAM {record['Extra']['iam_type']} events for account. "
                      f"Account Id: {record['account_id']} Reason: {exc}")
