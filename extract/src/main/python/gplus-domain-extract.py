#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys
import httplib2
import pprint
import json
import csv
from random import randint
from time import sleep
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
import HTMLParser
import re
import io

#  Fill in the following values based upon the previous steps
SERVICE_ACCOUNT_EMAIL = '86057209538-rlvi2ellefe6aq7isc1o62jpghbtd5vr@developer.gserviceaccount.com'
SERVICE_ACCOUNT_PKCS12_FILE_PATH = '/home/gilmarj/labs/google-bigdata-training/i-ciandt-dev-key.p12'
USER_EMAIL = 'gilmarj@ciandt.com'

PEOPLE_FILE = '/home/gilmarj/labs/google-bigdata-training/google-bigdata-participants.csv'
JSON_OUTPUT_FILE = '/home/gilmarj/labs/google-bigdata-training/gplus-developers-posts.json'
TXT_OUTPUT_FILE = '/home/gilmarj/labs/google-bigdata-training/gplus-developers-posts.txt'


# List the scopes your app requires. These must match the scopes
# registered in the Admin console for your Google Apps domain.
SCOPES = ['https://www.googleapis.com/auth/plus.me',
            'https://www.googleapis.com/auth/plus.stream.read',
            'https://www.googleapis.com/auth/admin.directory.user.readonly']

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

def remove_tags(text):
    html_parser = HTMLParser.HTMLParser()
    unescaped = html_parser.unescape(text)
    return re.compile(r'<[^>]+>').sub('', unescaped)

def read_posts(user_id, json_out_file, txt_out_file):
    service = authenticate()
    activities_service = service.activities()
    request = activities_service.list(
            userId=user_id,
            collection='user',
            maxResults='10')

    while request is not None:
        activities_document = request.execute()
        if activities_document.get('items') is not None:
            print 'got page with %d' % len(activities_document.get('items'))
            for activity in activities_document.get('items'):
                desc = ''
                if activity.get('access').get('description') is not None:
                    desc =  activity.get('access').get('description')
                if 'CI&T Developers' in desc:
                    print json.dumps(activity,sort_keys=True) 
                    json_out_file.write(unicode(json.dumps(activity,sort_keys=True))+'\n')
                    content = remove_tags(activity.get('object').get('content'))
                    txt_out_file.write(unicode(content+'\n').replace(u'\ufeff', '')) 
                    print activity.get('id'), activity.get('access').get('description'), activity.get('object').get('content')

        request = activities_service.list_next(request, activities_document)

if __name__ == '__main__':
    json_out_file = io.open(JSON_OUTPUT_FILE, 'w', encoding='utf8')
    txt_out_file = io.open(TXT_OUTPUT_FILE, 'w', encoding='utf8')
    with open(PEOPLE_FILE, 'rb') as csvfile:
        people  = csv.reader(csvfile, delimiter=',')
        for person in people:
            sleep(randint(0,1))
            #try:
            read_posts(person[1],json_out_file,txt_out_file)
            #except:
            #    print "Unexpected error:", sys.exc_info()[0] 
    json_out_file.close()
    txt_out_file.close()
