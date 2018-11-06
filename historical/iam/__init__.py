"""
.. module: historical.iam
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""


def capitalize_tech(tech: str):
    """Utility function to capitalize the IAM tech name.

    This is required instead of using .capitalize() because of the Server Certificates. capitalize() will
    transform "serverCertificates" to "Servercertificates" -- we need "ServerCertificates".

    :param tech:
    :returns: The properly capitalized tech name.
    """
    return tech[0].upper() + tech[1:]
