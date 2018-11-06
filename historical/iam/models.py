"""
.. module: historical.iam.models
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
from marshmallow import fields, post_dump
from pynamodb.attributes import UnicodeAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection, IncludeProjection, KeysOnlyProjection

from historical.constants import CURRENT_REGION, GSI_READ_CAP, GSI_WRITE_CAP
from historical.models import AWSHistoricalMixin, CurrentHistoricalModel, DurableHistoricalModel, \
    HistoricalPollingBaseModel, HistoricalPollingEventDetail


# The schema version
VERSION = 1
POLLING_VERSION = 1
IAM_TYPES = {'group', 'policy', 'role', 'serverCertificate', 'user'}


def default_region():
    """Returns the default region for IAM, which is global."""
    return 'global'


class IAMModel:
    """IAM specific fields for DynamoDB."""

    Name = UnicodeAttribute()
    Type = UnicodeAttribute()
    Region = UnicodeAttribute(default=default_region)


class DurableIAMModel(DurableHistoricalModel, AWSHistoricalMixin, IAMModel):
    """The Durable Table model for IAM."""

    class Meta:
        """Table Details"""

        table_name = 'HistoricalIAMDurableTable'
        region = CURRENT_REGION
        tech = 'iam'


class CurrentIAMNameTypeAccountLookupIndex(GlobalSecondaryIndex):
    """The current Table's GSI for looking up resources based on the name and type."""

    class Meta:
        """Table Details"""

        index_name = 'HistoricalIAMCurrentNameTypeAccount'
        projection = KeysOnlyProjection()
        read_capacity_units = GSI_READ_CAP
        write_capacity_units = GSI_WRITE_CAP

    NameTypeAccount = UnicodeAttribute(hash_key=True)


class CurrentIAMModel(CurrentHistoricalModel, AWSHistoricalMixin, IAMModel):
    """The Current Table model for IAM."""

    class Meta:
        """Table Details"""

        table_name = 'HistoricalIAMCurrentTable'
        region = CURRENT_REGION
        tech = 'iam'

    # This is the partition key for the Global Secondary Index, which will be:
    # the "Name+Type+AccountID"
    NameTypeAccount = UnicodeAttribute()
    name_type_index = CurrentIAMNameTypeAccountLookupIndex()


class IAMPollingEventDetail(HistoricalPollingEventDetail):
    """Schema that provides the required fields for mimicking the CloudWatch Event for Polling."""

    request_parameters = fields.Dict(dump_to="requestParameters", load_from="requestParameters", required=True)
    event_source = fields.Str(load_only=True, load_from="eventSource", required=True)
    event_name = fields.Str(load_only=True, load_from="eventName", required=True)

    @post_dump
    def add_required_iam_polling_data(self, data):
        """Adds the required data to the JSON.

        :param data:
        :return:
        """
        data["eventSource"] = "historical.iam.poller"
        data["eventName"] = "PollIAM"

        return data


class IAMPollingEventModel(HistoricalPollingBaseModel):
    """This is the Marshmallow schema for a Polling event. This is made to look like a CloudWatch Event."""

    detail = fields.Nested(IAMPollingEventDetail, required=True)
    version = fields.Str(load_only=True, required=True)

    @post_dump()
    def dump_iam_polling_event_data(self, data):
        """Adds the required data to the JSON.

        :param data:
        :return:
        """
        data["version"] = f'{POLLING_VERSION}'

        return data

    def serialize_me(self, account, iam_details):
        """Serializes the JSON for the Polling Event Model.

        :param account:
        :param iam_details: This is a dictionary of the type, along with the required parameter name for the collector
                            to make the necessary describe call for the item in question.
        :return:
        """
        return self.dumps({
            "account": account,
            "detail": {
                "request_parameters": iam_details
            }
        }).data


IAM_POLLING_SCHEMA = IAMPollingEventModel(strict=True)
