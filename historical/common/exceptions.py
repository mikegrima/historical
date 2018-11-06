"""
.. module: historical.common.exceptions
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""


class DurableItemIsMissingException(Exception):
    """Exception for if a Durable Item is missing but should be found."""

    pass


class MissingProxyConfigurationException(Exception):
    """Exception if the Proxy is missing the proper configuration on how to operate."""

    pass


class UnknownIAMTypeException(Exception):
    """Exception raised if an IAM event arrived, but the type of technology for the event is unknown or not handled
    by Historical."""

    pass


class UnknownIAMNameException(Exception):
    """Exception raised if an IAM event arrived, but the name of item for the event is unknown."""

    pass
