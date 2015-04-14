#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import httplib2
import pprint

from apiclient.discovery import build

from oauth2client.client import SignedJwtAssertionCredentials

#  Fill in the following values based upon the previous steps
SERVICE_ACCOUNT_EMAIL = '<your_service_account_email>'
SERVICE_ACCOUNT_PKCS12_FILE_PATH = '<your_p12_key_file>'
USER_EMAIL = '<email_from_a_domain_admin>'


# List the scopes your app requires. These must match the scopes
# registered in the Admin console for your Google Apps domain.
SCOPES = ['https://www.googleapis.com/auth/admin.directory.user.readonly']

def authenticate():
    print 'Authenticate the domain for %s' % USER_EMAIL

    f = open(SERVICE_ACCOUNT_PKCS12_FILE_PATH, 'rb')
    key = f.read()
    f.close()

    # Setting the sub field with USER_EMAIL allows you to make API calls using the
    # special keyword 'me' in place of a user id for that user.
    credentials = SignedJwtAssertionCredentials(SERVICE_ACCOUNT_EMAIL, key,
            scope=SCOPES, sub=USER_EMAIL)
    http = httplib2.Http()
    http = credentials.authorize(http)

    # Create and return the authorized API client
    return build('plusDomains', 'v1', http=http)

def read_posts(user_id):
    service = authenticate()
    activities_service = service.activities()
    request = activities_service.list(
            userId=user_id,
            collection='user',
            maxResults='2')

    while request is not None:
        activities_document = request.execute()
        if activities_document.get('items') is not None:
            print 'got page with %d' %
            len(activities_document.get('items'))
            for activity in activities_document.get('items'):
                print activity.get('id'),activity.get('object').get('content')

    request = activities_service.list_next(request, activities_document)

if __name__ == '__main__':
    read_posts('me') 

