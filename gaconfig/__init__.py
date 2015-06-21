# -*- coding: utf-8 -*-
# __author__ = 'bryan'

from lib.gabaseapi import GABase
from googleapiclient.errors import HttpError
import pprint
import json
import time
from lib import helpers
pp = pprint.PrettyPrinter(indent=4)

class GAManagement(GABase):
    accountId = None
    config = {}

    profiles = None

    def __init__(self, accountId, emailAPIClient=None, pathAPISecretKey=None):
        GABase.__init__(self, emailAPIClient, pathAPISecretKey)
        self.accountId = accountId


    def export_results(self):
        if self.config:
            filename = "googleanalytics_export_account_" + str(self.accountId) + ".json"
            print "Exporting config to " + filename
            with open(filename, 'w') as f:
                json.dump(self.config, f, sort_keys=False, indent=2)
                f.close()

    def _getWebPropertiesConfigs(self):
        for propertyid in self.config['webproperties']:
            property = self.config['webproperties'][propertyid]
            print ('<<<<< Getting configuration for web property "%s" (%s)' % (property['name'], str(propertyid)))

            prop_webPropertyAdWordsLinks = self.service.management().webPropertyAdWordsLinks().list(accountId=self.accountId,
                                                                           webPropertyId=propertyid).execute()
            property['webPropertyAdWordsLinks'] = self._getResultItemsAsDict(prop_webPropertyAdWordsLinks)

            try:
                prop_webpropertyUserLinks = self.service.management().webpropertyUserLinks().list(accountId=self.accountId,
                                                                               webPropertyId=propertyid).execute()
                property['webpropertyUserLinks'] = self._getResultItemsAsDict(prop_webpropertyUserLinks)
            except HttpError:
                # print('WARNING:  Could not get webpropertyUserLinks" for web property "%s" (%s).  Reason: %s' % (self.config['webproperties'][property]['name'], str(property), str(HttpError.message)))
                print('\tWARNING:  Could not get webpropertyUserLinks" for web property "%s" (%s). ' % (property['name'], str(propertyid)))

            prop_datasources = self.service.management().customDataSources().list(accountId=self.accountId,
                                                                           webPropertyId=propertyid).execute()
            property['customDataSources'] = self._getResultItemsAsDict(prop_datasources)

            prop_custdim = self.service.management().customDimensions().list(accountId=self.accountId,
                                                                           webPropertyId=propertyid).execute()
            property['customDimensions'] = self._getResultItemsAsDict(prop_custdim)

            prop_custdim = self.service.management().customMetrics().list(accountId=self.accountId,
                                                                           webPropertyId=propertyid).execute()
            property['customMetrics'] = self._getResultItemsAsDict(prop_custdim)

            self.config['webproperties'][propertyid] = property

            self._setProfilesData(propertyid)
            print ('>>>>> Completed web property "%s" (%s)\n\n' % (property['name'], str(propertyid)))

    def _getAccount(self):
        print ('Getting account "%s"' % str(self.accountId))
        accounts = self.service.management().accounts().list().execute()

        if accounts and accounts.get('items'):
            for account in accounts.get('items'):
                if self.accountId == account.get('id'):
                    self.config['account'] = {}
                    self.config['account'] = account


    def _setProfilesData(self, webpropertyId):

        print ('\tgetting profiles for web property "%s" (%s)' % (self.config['webproperties'][webpropertyId]['name'], str(webpropertyId)))

        webPropProfiles = self._getResultItemsAsDict(self.service.management().profiles().list(accountId=self.accountId, webPropertyId=webpropertyId).execute())
        if webPropProfiles:

            if not self.config['webproperties'].has_key(webpropertyId):
                self.config['webproperties'][webpropertyId] = {}

            if not self.config['webproperties'][webpropertyId].has_key('profiles'):
                self.config['webproperties'][webpropertyId]['profiles'] = {}

            for profileid in webPropProfiles:
                profile = webPropProfiles[profileid]

                print ('\tgetting profile "%s" (%s) configuration.' % (profile['name'], str(profileid)))


                # Google's rate limit is 10 queries per second so lets pause for 1 second
                time.sleep(1)

                goals = self.service.management().goals().list(
                    accountId=self.accountId,
                    webPropertyId=webpropertyId,
                    profileId=profileid).execute()

                profile['goals'] = self._getResultItemsAsDict(goals)

                prof_filters = self.service.management().profileFilterLinks().list(accountId=self.accountId,
                                                                              webPropertyId=webpropertyId,
                                                                              profileId=profileid).execute()
                profile['filters'] = self._getResultItemsAsDict(prof_filters)

                prof_experiments = self.service.management().experiments().list(accountId=self.accountId,
                                                                              webPropertyId=webpropertyId,
                                                                              profileId=profileid).execute()
                profile['experiments'] = self._getResultItemsAsDict(prof_experiments)

                prof_unsampledReports = self.service.management().unsampledReports().list(accountId=self.accountId,
                                                                              webPropertyId=webpropertyId,
                                                                              profileId=profileid).execute()
                profile['unsampledReports'] = self._getResultItemsAsDict(prof_unsampledReports)

                prof_uploads = self.service.management().unsampledReports().list(accountId=self.accountId,
                                                                              webPropertyId=webpropertyId,
                                                                              profileId=profileid).execute()
                profile['uploads'] = self._getResultItemsAsDict(prof_uploads)

                self.config['webproperties'][webpropertyId]['profiles'][profileid] = profile


    def export_hierarchy(self):

        if not self.config.has_key('account'):
            self._getAccountLevelConfig()


        self.export_results()



    def _getResultItemsAsDict(self, result):
        ret = {}
        for item in result.get('items'):
            itemid = item.get('id')
            ret[itemid] = item
        return ret

    def _setResultItems(self, key, result):
        dictResult = self._getResultItemsAsDict(result)
        if not self.config.has_key(key):
            self.config[key] = {}
        self.config[key] = dictResult.copy()

    def _getAccountLevelConfig(self):
        self._getAccount()
        self._setResultItems('segments', self.service.management().segments().list().execute())
        self._setResultItems('filters', self.service.management().filters().list(accountId=self.accountId).execute())
        self._setResultItems('webproperties', self.service.management().webproperties().list(accountId=self.accountId).execute())
        self._getWebPropertiesConfigs()

