# -*- coding: utf-8 -*-
__author__ = 'bryan'
from lib.gabaseapi import GABase
from googleapiclient.errors import HttpError
import pprint
import json
from lib import helpers
pp = pprint.PrettyPrinter(indent=4)
import urlparse




class GAQuery(GABase):
    name = None
    report_params = {}
    outputfolder = None
    outFilePath = None
    dailydata = None

    def __init__(self, reportParameters=None, emailAPIClient=None, pathAPISecretKey=None):
        GABase.__init__(self)
        self._setFromParam(reportParameters)
        self.dailyresults = []
        self.strQueryParamDetails = []
        self.outFilePath = helpers.getAvailOutputFileName(self.outputfolder, self.name)

        # Step 1. Get an analytics service object.

    def getData(self, params=None, startdate=None, enddate=None, account=None, propertyname=None, profile=None,
                          metrics=None, dimensions=None, filters=None, segment=None):

        resraw = self.getGAQueryRaw(params, startdate, enddate, account, propertyname, profile, metrics, dimensions,
                               filters, segment)

        resHeaders = resraw.get('columnHeaders')
        headers = []
        for item in resHeaders:
            headers.append(item['name'])

        resultRows = {}
        if not resraw.has_key('rows') and (resraw.has_key('totalResults') and resraw['totalResults'] == 0):
            print("WARNING!! No results returned by query.   This is most likely an error in the query format.")
        else:
            for v in resraw['rows']:
                row = dict(zip(headers, v))
                strKey = self._getRowKey(params, headers, row)
                resultRows[strKey] = row

        return resultRows

    def getGAQueryRaw(self,  params=None, startdate=None, enddate=None, account=None, propertyname=None,
                   profile=None, metrics=None, dimensions=None, filters=None, segment=None):

        if params is None:
            params = self.report_params

        init_query_params = self._getParamsDictCopy(params, startdate, enddate, account, propertyname, profile, metrics,
                                                    dimensions, filters, segment)
        query_params = self._prependgaToFields(init_query_params)

        try:
            res = self.service.data().ga().get(
                ids='ga:' + query_params['profileid'],
                metrics=query_params['metrics'],
                dimensions=query_params['dimensions'],
                filters=query_params['filters'],
                segment=query_params['segment'],
                start_date=helpers.fmtDateForGAPI(query_params['startdate']),
                end_date=helpers.fmtDateForGAPI(query_params['enddate']),
                prettyPrint=True).execute()

            return res

        except TypeError, error:
            # Handle errors in constructing a query.
            print ('There was an error in constructing your query : %s' % error)
            raise error

        except HttpError, error:
            # Handle API errors.
            print ('Arg, there was an API error : %s : %s' %
                   (error.resp.status, error._get_reason()))
            raise error

    def getReportForEachDayBetween(self, start_date=None, end_date=None):
        daybydayResults = {}

        if self.report_params is None:
            raise ReferenceError("Error:  No parameters set for report.")

        retDates = self.getDateValsStartEnd(start_date, end_date)

        self.addQueryParamsToOutput(self._getParamsDictCopy(self.report_params, startdate=retDates['startdate'], enddate=retDates['enddate']))


        for single_date in helpers.daterange(retDates['startdate'], retDates['enddate']):
            single_date_fmt = helpers.fmtDateForGA(single_date)
            daykey = single_date_fmt

            print('Getting data for dates ' + single_date_fmt + ' - ' + single_date_fmt)

            ret = self.getGAQueryRaw(startdate=single_date, enddate=single_date)
            daybydayResults[daykey] = ret

        return daybydayResults

    def getDateValsStartEnd(self, startdate=None, enddate=None):

        if not startdate:
            startdate = self.report_params['startdate']

        cleanstart = helpers.convertDate(startdate)
        if not cleanstart:
            raise ValueError("No valid start date was found for requested query.")

        if not enddate:
            enddate = self.report_params['enddate']

        cleanend = helpers.convertDate(enddate)
        if not cleanend:
            cleanend = helpers.getYesterday()

        datevalStart = helpers.getDateFromGADate(cleanstart)
        datevalEnd = helpers.getDateFromGADate(cleanend)

        if datevalEnd < datevalStart:
            datevalEnd = datevalStart

        return {'startdate':  datevalStart  , 'enddate' : datevalEnd }

    def addQueryParamsToOutput(self, params=None, label=None):
        if params is None:
            params = self.report_params

        if label is None:
            label = self.name
        else:
            label = ": " + label

        outStr = "name:\t" + label + "\n"
        for paramk in params:
            outStr += paramk + ":\t" + str(params[paramk]) + "\n"

        self.strQueryParamDetails.append(outStr)

    def writeQueryParamsToFile(self, filep):

        self.writeprint(filep, "\n\n****** Google Analytics Query Parameters ******\n")
        if self.strQueryParamDetails is None or not len(self.strQueryParamDetails) > 0:
            self.writeprint(filep, "Error.  No query params specified")
        else:
            for p in self.strQueryParamDetails:
                self.writeprint(filep, p + " \n")

        self.writeprint(filep, "\n\n***********************************************\n")

    def writeprint(self, filep, strval):
        if filep is None:
            raise SystemError("Error:  no file was opened to write into.")

        print strval
        filep.write(strval)

    def _parseAPICallToParams(self, apicall):
        retParams = {}
        if apicall is not None and len(apicall) > 0:
            o = urlparse.urlparse(apicall)
            if not (o.query is None and len(o.query) == 0):
                dictQuery = urlparse.parse_qs(o.query, True)
                #                print "API Call arguments: "
                #                pp.pprint(dictQuery)
                for k in dictQuery.keys():
                    if k in ['start-date', 'end-date']:
                        kNew = k.replace("-", "")
                    else:
                        kNew = k
                    retParams[kNew] = ",".join(dictQuery[k])

        return retParams


    def _getRawQueryForDates(self, startdate=None, enddate=None):

        self.dailyresults = self.getReportForEachDayBetween(startdate, enddate)
        jsonresults = json.dumps(self.dailyresults, indent=4)
        jsonfilepath = helpers.getAvailOutputFileName(self.outFilePath, ext="json")
        with open(jsonfilepath, 'wb') as jsonfile:
            jsonfile.writelines(jsonresults)
            jsonfile.close()

    def _setFromParam(self, params):
        testname = helpers.cleanParamString(params, 'testname')
        reportname = helpers.cleanParamString(params, 'reportname')
        if reportname:
            self.name = reportname
        elif testname:
            self.name = testname

        self.outputfolder = helpers.cleanParamString(params, 'outputfolder')
        self.outputfolder = helpers.setupOutputFolder(self.outputfolder)

        self.report_params['apicall'] = helpers.cleanParamString(params, 'apicall')
        if not (self.report_params['apicall'] is None and len(str(params['apicall'])) == 0):
            apiParams = self._parseAPICallToParams(self.report_params['apicall'])
            newParams = apiParams.copy()
            for p in params:
                if params[p] is not None:
                    newParams[p] = params[p]
            params = newParams.copy()

        self.report_params['metrics'] = helpers.cleanParamList(params, 'metrics')
        self.report_params['dimensions'] = helpers.cleanParamList(params, 'dimensions')
        self.report_params['filters'] = helpers.cleanParamString(params, 'filters')
        self.report_params['segment'] = helpers.cleanParamString(params, 'segment')
        self.report_params['sort'] = helpers.cleanParamString(params, 'sort')

        self.report_params['profileid'] = helpers.cleanParamString(params, 'ids')
        self.report_params['profileid'] = helpers.cleanParamString(params, 'profileid')
        if self.report_params['profileid'] is None or len(str(params['profileid'])) == 0:
            self.report_params['profileid'] = self.DEFAULT_GAPROFILEID
        else:
            self.report_params['profileid'] = helpers.cleanParamString(params, 'profileid')

        self.report_params['account'] = helpers.cleanParamString(params, 'account')
        if self.report_params['account'] is None or len(str(params['account'])) == 0:
            self.report_params['account'] = self.DEFAULT_GAACCOUNT
        else:
            self.report_params['account'] = helpers.cleanParamString(params, 'account')

        self.report_params['property_name'] = helpers.cleanParamString(params, 'property_name')
        if self.report_params['property_name'] is None or len(str(params['property_name'])) == 0:
            self.report_params['property_name'] = self.DEFAULT_GAPROPERTY
        else:
            self.report_params['property_name'] = helpers.cleanParamString(params, 'property_name')

        self.report_params['startdate'] = helpers.cleanParamDate(params, 'startdate')
        self.report_params['enddate'] = helpers.cleanParamDate(params, 'enddate')

        helpers.pp.pprint("User set the following report parameters:\n")
        print("name: " + self.name)
        print("output folder: " + self.outputfolder)
        helpers.pp.pprint(self.report_params)

    def _prependgaToFields(self, params):
        fields_to_update = ['filters', 'metrics', 'dimensions', 'sort']

        for fieldkey in params:
            try:
                if fieldkey in fields_to_update:
                    fieldval = params[fieldkey]
                    if isinstance(fieldval, list):
                        lnewval = []
                        for item in fieldval:
                            if not item.startswith("ga:"):
                                lnewval.append("ga:" + str(item))
                            else:
                                lnewval.append(item)
                        params[fieldkey] = ",".join(lnewval)
                    elif isinstance(fieldval, basestring):
                        parts = fieldval.split(";")
                        if parts is not None and len(parts) > 1:
                            lnewval = []
                            for item in parts:
                                if not item.startswith("ga:"):
                                    lnewval.append("ga:" + str(item))
                                else:
                                    lnewval.append(item)
                            params[fieldkey] = ";".join(lnewval)
                        elif not fieldval.startswith("ga:"):
                            params[fieldkey] = "ga:" + fieldval
            except TypeError:
                print fieldkey, 'is not iterable'

        params['startdate'] = helpers.convertDate(params['startdate'], '-')
        params['enddate'] = helpers.convertDate(params['enddate'], '-')

        return params

    def _getRowKey(self, query_params, headers, row):
        strKey = ""
        lMetrics = query_params['metrics']
        if isinstance(lMetrics, basestring):
            lMetrics.split(",")

        if row.has_key('ga:date'):
            strKey = row['ga:date'] + "_"

        for h in headers:
            if not h.startswith("ga:date"):
                if h not in lMetrics:
                    strKey += "_" + row[h]
        return strKey

    def _getParamsDictCopy(self, params, startdate=None, enddate=None, account=None, propertyname=None, profileid=None,
                           metrics=None, dimensions=None, filters=None, segment=None, sort=None):
        retParams = {}
        if isinstance(params, dict):
            retParams = params.copy()

        if startdate is not None:
            retParams['startdate'] = startdate

        if enddate is not None:
            retParams['enddate'] = enddate

        if account is not None:
            retParams['account'] = account

        if property is not None:
            retParams['property'] = propertyname

        if profileid is not None:
            retParams['profileid'] = profileid

        if metrics is not None:
            retParams['metrics'] = metrics

        if dimensions is not None:
            retParams['dimensions'] = dimensions

        if filters is not None:
            retParams['filters'] = filters

        if segment is not None:
            retParams['segment'] = segment

        if sort is not None:
            retParams['sort'] = sort

        return retParams

