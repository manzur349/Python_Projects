import re, qz.core.amps_servers, qzsix, AMPS, datetime, sandra
from qz.amps.types import NVFIXMessage
from qz.amps.qzamps import AMPSUtils
from qz.lib.mail import sendmail
from credit.utils.realm.realm_data_service import RealmDataService

class IDR2Watcher(object):
    #This class is used to monitor IDR2 processes that have been kicked. Instances of this will pull data from Realm and AMPS to report on number of groups
    #completed, number of trades processed, and pv for a given path interval. The class needs to be informed if an optimized or non-optimized run is being
    #monitored. Reports are then emailed to stakeholders.
    def __init__(self, myDataset, myDizzle, myEnv, myScenario, myLane = '', optimized = True):
        self.myDate = myDizzle
        self.sandra_db = 'credit_qa'
        self.sandra_email_path = '/Credit/RegSim/IDR2/Watcher/email_recepients'
        self.myEnv = ''
        self.myLane = ''
        self.completed = False
        if myEnv.strip().lower() in ('u', 'uat'):
            self.myEnv = 'uat'
        if myEnv.strip().lower() in ('q', 'qa'):
            self.myEnv = 'q'
        if myEnv.strip().lower() in ('d', 'dev'):
            self.myEnv = 'dev'
        if myEnv.strip().lower() in ('p', 'prod'):
            self.myEnv = 'prod'
        if not myLane:
            self.myLane = self.myEnv
        else:
            self.myLane = myLane.strip().lower()
        self.optimized = optimized
        self.dataset = myDataset
        self.ampsResult = {'processed' : {}, 'initialized' : '', 'failed' : ''}
        self.myMessage = ''
        self.realmResults = {'result' : {}}

    def _generate_email_message(self):
        if self.optimized:
            if 'error' not in self.ampsResults and 'error' not in self.realmResults and 'message' in self.ampsResults:
                self.myMessage += self.ampsResults['message'] + self._get_formatted_path_report()
            elif 'error' not in self.ampsResults and 'error' in self.realmResults and 'message' in self.ampsResults:
                self.myMessage += self.ampsResults['message'] + '<P>Encountered error whenb running trade count/path report<BR>' + str(self.realmResults['error'])
            elif 'error' in self.ampsResults and 'error' not in self.realmResults:
                self.myMessage = '<P>Encountered error whebn getting completed groups<BR>' + str(self.ampsResults['error']) + '<BR><BR>'
                self.myMessage += self._get_formatted_path_report()
            elif 'error' in self.ampsResults and 'error' in self.realmResults:
                self.myMessage += '<P>Encountered error when retrieving completed groups.<BR>' + str(self.ampsResults['error']) + '<BR><BR>'
                self.myMessage += '<P>Encountered error when retrieving trade count/path report<BR>' + str(self.realmResults['error'])
            else:
                self.myMessage += '<P>Please invoke method to query AMS and Realm before sending email.'
        else:
            if 'error' not in self.ampsResults and 'error' not in self.realmResults:
                mergedResults = self._merge_nonoptimized_group_and_path_results()
                noFailedMessage = 'There are no failed groups for this run.'
                noInitializedMessage = 'There are not initialized groups for this run.'
                if 'initialized' in self.ampsResults and 'failed' in self.ampsResults:
                    self.myMessage += mergedResults + '<BR><BR>' and self.ampsResults['initialized'] + '<BR><BR>' + self.ampsResults['failed']
                elif 'initialized' in self.ampsResults and 'failed' not in self.ampsResults:
                    self.myMessage += mergedResults + '<BR><BR>' + self.ampsMessage['initialized'] + '<BR><BR>' + noFailedMessage
                elif 'initialized' not in self.ampsResults and 'failed' in self.ampsResults:
                    self.myMessage += mergedResults + '<BR><BR>' + noInitializedMessage + '<BR><BR>' + self.ampsResults['failed']
                else:
                    self.myMessage += mergedResults + '<BR><BR>' + noInitializedMessage + '<BR><BR>' + noFailedMessage
            elif 'error' not in self.ampsResults and 'error' in self.realmResults:
                if 'processed' in self.ampsResults:
                    self.myMessage += '<P>Encountered error when retrieving number of trades per path - <BR>'
                    self.myMessage += str(self.realmResults['error']) + '<BR><BR>Number of groups processed/path - <BR>'
                    self.myMessage += self._get_processed_table_from_dictionary()
                if 'initialized' in self.ampsResults:
                    self.myMessage += '<BR><BR>' + self.ampsResults['initialized']
                else:
                    self.myMessage += '<P>There are no initialized groups for this run.'
                if 'failed' in self.ampsResults:
                    self.myMessage += '<BR><BR>' + self.ampsResults['failed']
                else:
                    self.myMessage += '<P>There are no failed groups for this run.'
            elif 'error' in self.ampsResults and 'error' not in self.realmResults:
                self.myMessage += '<P>Encountered error when trying to retrieve groups completed per path.<BR><BR>'
                self.myMessage += self._get_formatted_path_report()
            else:
                self.myMessage += '<P>Encountered error when retrieving completed groups.<BR>' + str(self.ampResults['error']) + '<BR><BR>'
                self.myMessage += '<P>Encountered error when retrieving trade count/path report<BR>' + str(self.realmResults['error'])

    def _get_all_groups_from_result(self, myTopics, failed = False):
        outVal = {}
        for topic in myTopics:
            myTokens = topic.split(' - ')
            if len(myTokens) >= 5:
                try:
                    myPath = myTokens[len(myTokens) - 2].strip()
                    if self.optimized and failed:
                        if myPath.strip() == '(1, 10000)':
                            outVal = self._get_groups_from_tokenized_topics(myPath, outVal, myTokens)
                    elif '(1, 10000)' not in topic:
                        outVal = self._get_groups_from_tokenized_topics(myPath, outVal, myTokens)
                except Exception as e:
                    print('Encountered unrecognized topic type - ')
                    print(topic)
                    print(e)
        return outVal

    def _get_all_topics_with_paths(self, myTopics, myStatus):
        outVal = []
        for stat in myTopics:
            if re.findall(r'(\(\d+, \d+\))', stat['result_topic']) and stat['status'].strip().lower() == myStatus.lower():
                outVal.append(stat['result_topic'].strip())
        return outVal

    def _get_bob_amps(self):
        myServer = ''
        outVal = None
        if self.myEnv.strip().lower() in ('u', 'q', 'uat', 'qa', 'd', 'dev'):
            myServer = 'lvt_credit_prims_dev'
        else:
            myServer = 'nyc_credit_prims_prd'
        outVal = qz.core.amps_servers.lookup(myServer)
        outVal = outVal.client(__name__, messageClass = NVFIXMessage)
        outVal.connect_and_logon()
        return outVal

    def _get_cleaned_group_from_topic(self, myGroup):
        outVal = ''
        myStack = []
        for c in myGroup:
            if c not in ('[', ']', '"', '\''):
                myStack.append(c)
        for c in myStack:
            outVal += c
        if ',' in outVal:
            myTokens = outVal.split(',')
            outVal = []
            for token in myTokens:
                outVal.append(token.strip())
            retrun outVal
        else:
            return outVal

    def _get_formatted_path_report(self):
        outVal = '<P>Number of trades/path<BR><TABLE><TR><TD>Path No.</TD><TD>Trade Count</TD><TD>Path PV</TD></TR>'
        for i in range(0, 10001, 200):
            if str(i) in self.realmResults['result'].keys():
                outVal += '<TR><TD>' + str(i) + '</TD><TD>' + self.realmResults['result'][str(i)]['trade_count'] + '</TD><TD>'
                outVal += self.realmResults['result'][str(i)]['path_pv'] + '</TD></TR>'
        outVal += '</TABLE>'
        return outVal

    def _get_groups_from_tokenized_topics(self, myPath, myPathDict, tokenizedTopic):
        if len(tokenizedTopic) == 5:
            myBook = self._get_cleaned_group_from_topic(tokenizedTopic[len(tokenizedTopic) - 3].strip())
            if myPath.strip() in myPathDict:
                myBooks = myPathDict[myPath.strip()]
                if type(myBook) is str:
                    if myBook not in myBooks:
                        myBooks.append(myBook)
                elif type(myBook) is list:
                    for bk in myBook:
                        if bk.strip() not in myBooks:
                            myBooks.append(bk.strip())
                myPathDict[myPath.strip()] = myBooks
            else:
                if type(myBook) is str:
                    myPathDict[myPath.strip()] = [myBook]
                elif type(myBook) is list:
                    myPathDict[myPath.strip()] = myBook
            return myPathDict
        elif len(tokenizedTopic) == 6:
            myBook = self._get_cleaned_group_from_topic(tokenizedTopic[len(tokenizedTopic) - 4].strip())
            if type(myBook) is str:
                myBook += ' ' + self._get_cleaned_group_from_topic(tokenizedTopic[len(tokenizedTopic) - 3].strip())
            elif type(myBook) is list:
                bookType = self._get_cleaned_group_from_topic(tokenizedTopic[len(tokenizedTopic) - 3].strip())
                for i in range(len(myBook)):
                    myBook[i] = myBook[i] + ' ' + bookType
            if myPath.strip() in myPathDict:
                myBooks = myPathDict[myPath.strip()]
                if type(myBook) is str and myBook not in myBooks:
                    myBooks.append(myBook)
                elif type(myBook) is list:
                    for bk in myBook:
                        if bk.strip() not in myBooks:
                            myBooks.append(bk.strip())
                myPathDict[myPath.strip()] = myBooks
            else:
                if type(myBook) is str:
                    myPathDict[myPath.strip()] = [myBook]
                elif type(myBook) is list:
                    myPathDict[myPath.strip()] = myBook
            return myPathDict
        elif len(tokenizedTopic) > 6:
            raise Exception('Cannot derive group from tokenized topic:\n' + str(tokenizedTopic))

    def _get_processed_table_from_dictionary(self):
        outVal = '<TABLE><TD>Path</TD><TD>No. of Completed Groups</TD></TR>'
        mySortedPaths = self._sort_list_of_paths(list(self.ampsResults['processed'].keys()))
        for path in mySortedPaths:
            outVal += '<TR><TD>' + path + '</TD><TD>' + self.ampsResults['processed'][path] + '</TD></TR>'
        outVal += '</TABLE>'
        return outVal

    def _get_status_for_IDR2(self):
        daResult = {'processed' : {}, 'initialized' : '', 'failed' : ''}
        try:
            myProcessedTopics = self._get_all_groups_from_result(self._get_all_topics_with_paths(self._get_status_topic(), 'processed'))
            myInitializedTopics = self._get_all_groups_from_result(self._get_all_topics_with_paths(self._get_status_topic(), 'initialized'))
            myFailedTopics = self._get_all_groups_from_result(self._get_all_topics_with_paths(self._get_status_topic(), 'failed'), True)
            if myProcessedTopics.keys():
                for i in range(0, 10000, 200):
                    computedPath = '(' + str(i + 1) + ', ' + str(i + 200) + ')'
                    if computedPath in list(myProcessedTopics.keys()):
                        daResult['processed'][computedPath] = str(len(myProcessedTopics[computedPath]))
                    else:
                        daResult['processed'][computedPath] = '0'
            if len(myInitializedTopics.keys()):
                listOfPaths = self._sort_list_of_paths(list(myInitializedTopics.keys()))
                for path in listOfPaths:
                    daResult['initialized'] += 'Number of groups initialized for paths ' + path + ': ' + str(len(myInitializedTopics[path])) + '<BR>'
            else:
                daResult['initialized'] += 'Number of paths initialized: 0<BR><BR>'
            if len(myFailedTopics.keys()):
                listOfPaths = self._sort_list_of_paths(list(myFailedTopics.keys()))
                for path in listOfPaths:
                    daResult['failed'] += 'Number of groups failed for paths ' + path + ': ' + str(len(myFailedTopics[path])) + '<BR>'
            else:
                daResult['failed'] += 'Number of paths failed: 0<BR><BR>'
            self._unpackage_amps_results(daResult)
        except Exception as e:
            print('error!')
            daResult['error'] = e
            self._unpackage_amps_results(daResult)

    def _get_status_for_optimized_IDR2(self):
        daResult = {}
        try:
            myProcessedTopics = self._get_all_groups_from_result(self._get_all_topics_with_paths(self._get_status_topic(), 'processed'))
            myInitializedTopics = self._get_all_groups_from_result(self._get_all_topics_with_paths(self._get_status_topic(), 'initialized'))
            myFailedTopics = self._get_all_groups_from_result(self._get_all_topics_with_paths(self._get_status_topic(), 'failed'), True)
            myBreak = '*' * 10
            if len(myProcessedTopics.keys()):
                daResult['processed'] = 'Number of processed groups: ' + str(len(myProcessedTopics[list(myProcessedTopics.keys())[0]])) + '<BR>'
            else:
                daResult['processed'] = 'Number of processed groups: 0 <BR>'
            if len(myInitializedTopics.keys()):
                daResult['initialized'] = 'Number of initialized groups: ' + str(len(myInitializedTopics[list(myInitiailzedTopics.keys())[0]])) + '<BR>'
            else:
                daResult['initialized'] = 'Number of initialized groups: 0 <BR>'
            if len(myFailedTopics.keys()):
                daResult['failed'] = 'Number of failed groups: ' + str(len(myFailedTopics[list(myFailedTopics.keys())[0]])) + '<BR>'
            else:
                daResult['failed'] = 'Number of failed groups: 0 <BR>'
            daResult['message'] = 'Processed Groups<BR>' + self._print_my_optimized_topics(myProcessedTopics) + '<BR><BR>' + myBreak + '<BR><BR><BR><BR>Initialized Groups<BR>'
            daResult['message'] += self._print_my_optimized_topics(myInitializedTopics) + '<BR><BR>' + myBreak + '<BR><BR><BR><BR>Failed Groups<BR><BR>'
            daResult['message'] += self._print_my_optimized_topics(myFailedTopics) + '<BR><BR>'
            self._unpackage_amps_results(daResult)
        except Exception as e:
            print('error')
            daResult['error'] = e
            self._unpackage_amps_results(daResult)

    def _get_status_topic(self, lookingForComplete = False):
        outVal = []
        try:
            if self.myEnv.strip().lower() in ('u', 'q', 'uat', 'qa', 'd', 'dev'):
                if not self.myLane:
                    self.myLane = self.myEnv.strip()
            amps = self._get_bob_amps()
            myTopic = ''
            if not lookingForComplete:
                if self.myEnv.strip().lower() in ('d', 'dev'):
                    myTopic = '/DEV/RISKBATCH/SERVICE/STATUS'
                elif self.myEnv.strip().lower() in ('q', 'qa'):
                    myTopic = '/QA/RISKBATCH/SERVICE/STATUS'
                elif self.myEnv.strip().lower() in ('u', 'uat'):
                    myTopic = '/UAT/RISKBATCH/SERVICE/STATUS'
                else:
                    myTopic = '/PROD/RISKBATCH/SERVICE/STATUS'
            else:
                if self.myEnv.strip().lower() in ('d', 'dev'):
                    myTopic = '/DEV/RISKBATCH/SERVICE/REGSIM/JOB'
                elif self.myEnv.strip().lower() in ('q', 'qa'):
                    myTopic = '/QA/RISKBATCH/SERVICE/REGSIM/JOB'
                elif self.myEnv.strip().lower() in ('u', 'uat'):
                    myTopic = '/UAT/RISKBATCH/SERVICE/REGSIM/JOB'
                else:
                    myTopic = '/PROD/RISKBATCH/SERVICE/STATUS'
            sow = amps.sow(myTopic, batch_size = 10000, timeout = 500000)
            for message in sow:
                if message.get_command() == AMPS.Message.Command.SOW:
                    jobDetails = AMPSUtils.fixToDict(message.get_data())
                    if len(self.myLane) != 0 and jobDetails['lane'].lower().strip() == self.myLane.lower().strip():
                        outVal.append(jobDetails)
            return qzsix.filter(None, outVal)
        finally:
            amps.close()
            return outVal

    def _getTableStyle(self):
        style = '<STYLE>'
        style += 'table {padding:5px;font-size:11px; font-family:"segoe ui", sans-serif}'
        style += 'th{text-align: center; border: 1px solid black;border-collapse:collapse;background-color:#3498DB ;}'
        style += 'td.mainHeader{background-color:#cce6fc;text-align:right;}'
        style += 'td.mainHeaderLeft{background-color:#cce6fc;text-align:left;}'
        style += 'td.bgGrayRight{background-color:#d3d3d3;text-align:Right;}'
        style += 'td.bgGrayLeft{background-color:#d3d3d3;text-align:left;}'
        style += 'td.highLightYellow{background-color:#FFFF00;}'
        style += 'td.alignLeft{text-align:left;}'
        style += 'td.productHighLightYellow{background-color:#FFFF00;text-align:left;}'
        style += 'td{text-align: left;border: 1px solid black;border-collapse:collapse;white-space: nowrap;}'
        style += 'p {text-indent: 50px;}'
        style += '</STYLE>'
        return style

    def _merge_nonoptimized_group_and_path_results(self):
        outVal = ''
        if 'error' not in self.ampsResults and 'error' not in self.realmResults:
            outVal = self._merge_nonoptimized_group_and_path_results_with_no_errors()
        elif 'error' in self.ampsResults and 'error' not in self.realmResults:
            header = '<TD>Path</TD><TD>No. of Completed Groups</TD><TD>Trade Count</TD><TD>Path PV</TD>
            outVal = 'Processed Results - <BR><TABLE><TR>' + header + '</TR><TR>'
            for i in range(0, 10000, 200):
                cPath = '(' + str(i + 1) + ', ' + str(i + 200) + ')'
                if str(i + 200) in self.realmResults['result']:
                    outVal += '<TR><TD>' + cPath + '</TD><TD>Error</TD><TD>' + self.realmResults['result'][str(i + 200)]['trade_count'] + '</TD><TD>'
                    outVal += self.realmResults['result'][str(i + 200)]['path_pv'] + '</TD></TR>'
                else:
                    outVal ++ '<TR><TD>' + cPath + '</TD><TD>N/A</TD><TD>N/A</TD><TD>N/A</TD></TR>'
            outVal += '</TABLE>
        elif 'error' not in self.ampsResults and 'processed' in self.ampsResults and 'error' in self.realmResults:
            header = '<TD>Path</TD><TD>No. of Completed Groups</TD><TD>Trade Count</TD><TD>Path PV</TD>
            outVal = 'Processed Results - <BR><TABLE><TR>' + header + '</TR><TR>'
            for i in range(0, 10000, 200):
                cPath = '(' + str(i + 1) + ', ' + str(i + 200) + ')'
                if ((i + 200) % 1000):
                    if cPath in self.ampsResults['processed']:
                        outVal += '<TR><TD>' + cPath + '</TD><TD>' + self.ampsResults['processed'][cPath] + '</TD><TD>N/A</TD><TD>N/A</TD></TR>'
                else:
                    if cPath in self.ampsResults['processed']:
                        outVal += '<TR><TD>' + cPath + '</TD><TD>' + self.ampsResults['processed'][cPath] + '</TD><TD>Error</TD><TD>Error</TD></TR>'
                    else:
                        outVal += '<TR><TD>' + cPath + '</TD><TD>N/A</TD><TD>Error</TD><TD>Error</TD></TR>'
            outVal += '</TABLE>'
        else:
            outVal = 'Error encountered when trying to retrieve both processed groups for a given path and number of trades per path.'
        return outVal

    def _merge_nonoptimized_group_and_path_results_with_no_errors(self):
        header = '<TD>Path</TD><TD>No. of Completed Groups</TD><TD>Trade Count</TD><TD>Path PV</TD>
        outVal = 'Processed Results - <BR><TABLE><TR>' + header + '</TR><TR>'
        if 'result' in self.realmResults and '0' in self.realmResults['result']:
            outVal += '<TD>0</TD><TD>N/A</TD><TD>' + self.realmResults['result']['0']['trade_count'] + '</TD><TD>' + self.realmResults['result']['0']['path_pv'] + '</TD></TR>
        else:
            outVal += '<TD>0</TD><TD>N/A</TD><TD>N/A</TD><TD>N/A</TD></TR>'
        if 'processed' in self.ampsResults and 'result' in self.realmResults:
            for i in range(0, 10000, 200):
                cPath = '(' + str(i + 1) + ', ' + str(i + 200) + ')'
                if str(i + 200) in self.realmResults['result'] and cPath in self.ampsResults['processed']:
                    outVal += '<TR><TD>' + cPath + '</TD><TD>' + self.ampsResults['processed'][cPath] + '</TD>;
                    outVal += '<TD>' + self.realmResults['result'][str(i + 200)]['trade_count'] + '</TD><TD>'
                    outVal += self.realmResults['result'][str(i + 200)]['path_pv'] + '</TD><TR>'
                elif str(i + 200) not in self.realmResults['result'] and cPath in self.ampsResults['processed']:
                    outVal += '<TR><TD>' + cPath + '</TD><TD>' + self.ampsResults['processed'][cPath] + '</TD><TD>N/A<TD><TD>N/A</TD></TR>
                elif str(i + 200) in self.realmResults['result'] and cPath not in self.ampsResults['processed']:
                    outVal += '<TR><TD>' + cPath + '</TD><TD>N/A</TD><TD>' + self.realmResults['result'][str(i + 200)]['trade_count'] + '</TD>'
                    outVal += '<TD>' + elf.realmResults['result'][str(i + 200)]['path_pv'] + '</TD></TR>'
                else:
                    outVal += '<TR><TD>' + cPath + '</TD><TD>N/A</TD><TD>N/A</TD><TD>N/A</TD></TR>'
        elif 'processed' not in self.ampsResults and 'result' in self.realmResults:
            for i in range(0, 10000, 200):
                cPath = '(' + str(i + 1) + ', ' + str(i + 200) + ')'
                if str(i + 200) in self.realmResults['result']:
                    outVal += '<TR><TD>' + cPath + '</TD><TD>N/A</TD><TD>' + self.realmResults['result'][str(i + 200)]['trade_count'] + '</TD><TD>'
                    outVal += self.realmResults['result'][str(i + 200)]['path_pv'] + '</TD></TR>'
                else:
                    outVal += '<TR><TD>' + cPath + '</TD><TD>N/A</TD>TD>N/A</TD>TD>N/A</TD></TR>'
        elif 'processed' in self.ampsResults and 'result' not in self.realmResults:
            for i in range(0 10000, 200):
                cPath = '(' + str(i + 1) + ', ' + str(i + 200) + ')'
                if cPath in self.ampsResults['processed']:
                    outVal += '<TR><TD>' + cPath + '</TD><TD>' + self.ampsResults['processed'][cPath] + '</TD><TD>N/A</TD><TD>N/A</TD></TR>'
                else:
                    outVal += '<TR><TD>' + cPath + '</TD><TD>N/A</TD><TD>N/A</TD><TD>N/A</TD></TR>'
        outVal += '</TABLE>'
        return outVal

    def _print_my_optimized_topics(self, myTopics):
        outVal = '<TABLE><TR><TD>Path Interval</TD><TD>Groups</TD></TR>'
        for path in myTopics:
            outVal += '<TR><TD>' + path + '</TD><TD>'
            for grp in myTopics[path]:
                outVal += grp + '<BR>'
            outVal += '</TD></TR>'
        outVal += '</TABLE>'
        return outVal

    def _sort_list_of_paths(self, listOfPaths):
        outVal = []
        listOfTuples = []
        for path in listOfPaths:
            tk = path[1:len(path) - 1].split(',')
            if len(tk) == 2:
                myTuple = (int(tk[0].strip()), int(tk[1].strip()))
                listOfTuples.append(myTuple)
        listOfTuples = sorted(listOfTuples, key = lambda k: (k[0], k[1]))
        for t in listOfTuples:
            outVal.append('(' + str(t[0]) + ', ' + str(t[1]) + ')')
        return outVal

    def _unpackage_amps_results(self, myResults):
        if 'processed' in myResults:
            self.ampsResults['processed'] = myResults['processed']
        else:
            self.ampsResults.pop('processed')
        self.ampsResults['initialized'] = myResults['initialized']
        self.ampsResults['failed'] = myResults['failed']
        if 'error' in myResults:
            self.ampsResults['error'] = myResults['error']
        if 'message' in myResults:
            self.ampsResults['message'] = myResults['message']

    def clear_email_message(self):
        self.myMessage = ''

    def get_IDR2_status_from_amps(self):
        if self.optimized:
            self.get_status_for_optimized_IDR2()
        else:
            self.get_status_for_IDR2()

    def get_IDR2_trade_count_per_path(self):
        try:
            rds = RealmDataService()
            if self.myEnv.upper() in ('Q', 'QA'):
                rds.setCurrentEnvironment('UAT')
            else:
                rds.setCurrentEnvironment(self.myEnv)
            myPaths = []
            for i in range(200, 10001, 200):
                myPath.append(i)
            if type(self.myDate) == datetime.date:
                date = self.myDate.strftime('%d-%b-%Y')
                reportName = 'Exa Trade Count By Path'
                paramDict = {   'Reval Date'    : [date],
                                'Path Data Set' : [self.dataset],
                                'Path Scenario' : [self.scenario],
                                'Path Number'   : myPaths
                            }
                realmTable = rds.runReport(reportName, paramDict)
                if realmTable and len(realmTable):
                    for r in realmTable:
                        if len(r) == 3:
                            self.realmResults['result'][r[0].strip() = {'trade_count' : r[1].strip(), 'path_pv' : r[2].strip()}
        except Exception as e:
            self.realmResults['error'] = e

    def has_completed_all_topic_result(self):
        myAmpsResult = self._get_status_topic(True)
        for rec in myAmpsResult:
            if 'run_progress' in rec and rec['run_progress'].strip().lower() == 'completed all.' and rec['lane'].strip().lower == self.myLane and rec['dataset_name'].lower() == self.dataset.lower():
                self.completed = True
            if self.completed:
                break
        return self.completed

    def print_amps_results(self):
        print(self.ampsResults)

    def print_realm_results(self):
        print(self.realmResults)

    def send_email(self):
        self._generate_email_message()
        batchDate = self.myDate.strftime('%Y-%m-%d')
        mailSubject = ''
        if not len(self.myLane) and not self.completed:
            mailSubject = '[%s] [%s] [%s]' % (self.myEnv.upper(), batchDate, 'IDR2 Progress')
        elif not len(self.myLane) and self.completed:
            mailSubject = '[%s] [%s] [%s]' % (self.myEnv.upper(), batchDate, 'UDR2 Progress - Completed!')

        message = 'Below is the current status for ' + batchDate + ' IDR2 run in progress in '
        if not len(self.myLane):
            message += self.myEnv + ' environment usng dataset ' + self.dataset + ' and scenario ' + self.scenario + ': <BR>'
        else:
            message += self.myEnv + '/' self.myLane + ' environment usng dataset ' + self.dataset + ' and scenario ' + self.scenario + ': <BR>'
        message += self.myMessage
        attachMsg = ''

        mailBody = '''%s%s<BR><BR>%s''' % (self._getTableStyle(), message, attachMsg)
        myDB = sandra.connect(self.sandra_db)
        addrs = myDB.read(self.sandra_email_path).contents
        qz.lib.mail.sendmail(sender = 'Realm Report Scheduler',
                             addrs = addrs,
                             subject = mailSubject,
                             body = mailBody,
                             format = 'html',
                             useOutlook = True)
