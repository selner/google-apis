from googleapiclient.discovery import build
import httplib2
from oauth2client.client import SignedJwtAssertionCredentials
# -*- coding: utf-8 -*-
__author__ = 'bryan'

import logging
import os
script_path = os.path.dirname(os.path.abspath( __file__ ))

#
# Enable Google API Logging level
# Docs:  https://developers.google.com/api-client-library/python/guide/logging
#
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
httplib2.debuglevel = 0  # switch to 4 for lots of output

class GoogleAPIBase:
    pathSecretsFile = None
    emailAPIClient = None
    pathAPISecretKey = None

    def __init__(self, service, version, scope, emailAPIClient=None, pathAPISecretKey=None):
        self.emailAPIClient = emailAPIClient
        self.pathAPISecretKey = pathAPISecretKey
        self.service = self._initialize_service(service, version, scope)

    def _prepare_credentials(self, scope):
            # Declare constants and set configuration values
            # A file to store the access token
            CLIENT_SECRETS = 'client_secrets.json'

            # Retrieve existing credendials
            # storage = Storage(TOKEN_FILE_NAME)
            # credentials = storage.get()

            # If existing credentials are invalid and Run Auth flow
            # the run method will store any new credentials
            # if credentials is None or credentials.invalid:

            with open(self.pathAPISecretKey) as f:
                private_key = f.read()
            ## notasecret
            credentials = SignedJwtAssertionCredentials(self.emailAPIClient, private_key, scope)

            return credentials

    def _getTokenFromCreds(self, credentials):
        return credentials.get_access_token()

    def _initialize_service(self, service_name, service_version, scope):
        # 1. Create an http object
        http = httplib2.Http()

        # 2. Authorize the http object
        # In this tutorial we first try to retrieve stored credentials. If
        # none are found then run the Auth Flow. This is handled by the
        # _prepare_credentials() function defined earlier in the tutorial

        credentials = self._prepare_credentials(scope)

        http = credentials.authorize(http)  # authorize the http object

        # 3. Build the Analytics Service Object with the authorized http object
        return build(service_name, service_version, http=http)


class GABase(GoogleAPIBase):
    service = None
    def __init__(self, emailAPIClient=None, pathAPISecretKey=None):
        GoogleAPIBase.__init__(self, 'analytics', 'v3', 'https://www.googleapis.com/auth/analytics.readonly', emailAPIClient, pathAPISecretKey)

