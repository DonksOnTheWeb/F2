import math
import datetime
import time
import json

from _loghandler import logger
from _prophet import doHPT, doForecast

from _readers import readFromGeneric
from _writers import writeHourly, writeParams, writeForecast, printHourlyAccuracy, printDailyAccuracy


def autoTune():
    failSleepTime = 60
    ordersDict = buildDictionaries()
    writeHourly(ordersDict['MFCs_Hourly'], 'H Crve MFC')
    writeHourly(ordersDict['Regions_Hourly'], 'H Crve Rgn')
    # Now convert the TS into a nice json and send data to the prophet module
    itr = 1
    accStartRow = {'D': 2, 'H': 2}
    outRowByCountryD = {}
    outRowByCountryHD = {}
    outRowByCountryW = {}
    printOffset = 0

    MFCList = []
    for entry in ordersDict['MFCList']:
        if entry not in MFCList:
            MFCList.append(entry)
    MFCList.sort()

    for MFC in MFCList:
        result = False
        while not result:
            try:
                rows = fullMFCprocess(ordersDict, MFC, accStartRow, outRowByCountryD, outRowByCountryHD,
                                      outRowByCountryW, itr, printOffset)
                accStartRow = rows['AccStart']
                outRowByCountryD = rows['D']
                outRowByCountryHD = rows['HD']
                outRowByCountryW = rows['W']
                itr = rows['I']
                result = True
            except Exception as e:
                logger('W', '')
                logger('W', repr(e))
                logger('W', MFC + ' Failed.  Sleeping ' + str(failSleepTime) + '...')
                time.sleep(failSleepTime)
                logger('I', 'Trying ' + MFC + ' again')


def fullMFCprocess(ordersDict, MFC, accStartRow, outRowByCountryD, outRowByCountryHD, outRowByCountryW, itr,printOffset):
    ts = []
    ts_lw = []
    earliest = None
    latest = None
    ctry = (ordersDict['Regions'][MFC]).split('_')[0]
    for entry in ordersDict['DailyOrders'][MFC]:
        dte = entry
        dte = datetime.datetime.strptime(dte, '%Y-%m-%d')
        if earliest is None:
            earliest = dte
        else:
            if dte < earliest:
                earliest = dte

        if latest is None:
            latest = dte
        else:
            if dte > latest:
                latest = dte

    # latest should be yesterday at most
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday = datetime.datetime.strptime(yesterday, '%Y-%m-%d')
    if latest > yesterday:
        latest = yesterday

    lastWeek = latest - datetime.timedelta(days=6)  # remember we have already gone one day back to yesterday.
    logger("I",
           MFC + ": Earliest date:" + str(earliest) + " Latest date:" + str(latest) + " Last week:" + str(lastWeek))

    delta = datetime.timedelta(days=1)
    while earliest <= latest:
        dte = earliest.strftime('%Y-%m-%d')
        if dte in ordersDict['DailyOrders'][MFC]:
            Orders = ordersDict['DailyOrders'][MFC][dte]
            ts.append({"ds": dte, "y": Orders})
            if earliest <= lastWeek:
                ts_lw.append({"ds": dte, "y": Orders})

        earliest += delta

    timestamp = datetime.datetime.now().strftime('%d-%b-%Y, %H:%M:%S')
    logger("I", "Running " + MFC + " - number " + str(itr) + " at " + timestamp)
    prophetParams = doHPT(MFC, ts, ctry)
    timestamp = datetime.datetime.now().strftime('%d-%b-%Y, %H:%M:%S')
    logger("I", "Completed " + MFC + " - number " + str(itr) + " at " + timestamp)
    logger("I", "")
    writeParams(prophetParams['MFC'], prophetParams['Best'], prophetParams['Ctry'], itr)

    # Now do forecast with those params
    forecast_ts = doForecast(MFC, latest, ts, prophetParams['Ctry'], prophetParams['Best'])
    # ...and the accuracy check forecast
    lastWeek = latest - datetime.timedelta(days=7)
    accuracy_ts = doForecast(MFC, lastWeek, ts_lw, prophetParams['Ctry'], prophetParams['Best'])

    # Print out the accuracy
    acc_ts = createNiceTS(MFC, accuracy_ts, ordersDict)
    hourlyDailyList = acc_ts['HD']  # forecast by HD
    dailyList = acc_ts['D']  # forecast by D
    hourlyOrders = ordersDict['HourlyOrders'][MFC]  # orders by H.  We will use 7 days worth
    dailyOrders = ordersDict['DailyOrders'][MFC]  # forecast by D.  We will use 7 days worth

    accStartRow['H'] = printHourlyAccuracy(MFC, ctry, hourlyDailyList, hourlyOrders, accStartRow['H'], yesterday)
    accStartRow['D'] = printDailyAccuracy(MFC, ctry, dailyList, dailyOrders, accStartRow['D'], yesterday)

    # exportAccuracy(MFC, ctry, itr, hourlyDailyList, hourlyOrders, latest, accStartRow)
    itr = itr + 1

    # Create 2 timeseries for display in gSheets -  HourlyDaily, Daily
    out_ts = createNiceTS(MFC, forecast_ts, ordersDict)

    hourlyDailyList = out_ts['HD']
    dailyList = out_ts['D']
    weeklyList = out_ts['W']

    # Now write these to the forecast tabs
    if ctry in outRowByCountryD:
        outRow = outRowByCountryD[ctry]
    else:
        outRow = 2
    writeForecast(MFC, dailyList, ctry + " Fcast D", outRow, False)
    outRow = outRow + len(dailyList) + printOffset
    outRowByCountryD[ctry] = outRow

    if ctry in outRowByCountryHD:
        outRow = outRowByCountryHD[ctry]
    else:
        outRow = 2
    writeForecast(MFC, hourlyDailyList, ctry + " Fcast HD", outRow, False)
    outRow = outRow + len(hourlyDailyList) + printOffset
    outRowByCountryHD[ctry] = outRow

    if ctry in outRowByCountryW:
        outRow = outRowByCountryW[ctry]
    else:
        outRow = 2
    writeForecast(MFC, weeklyList, ctry + " Fcast W", outRow, True)
    outRow = outRow + len(weeklyList) + printOffset
    outRowByCountryW[ctry] = outRow

    rows = {'AccStart': accStartRow, 'D': outRowByCountryD, 'HD': outRowByCountryHD,
            'W': outRowByCountryW, 'I': itr}
    return rows


def createNiceTS(MFC, ts, ordersDict):
    # use this to build out the TS for the forecast and accuracy forecast
    dCount = 0
    dailyList = []
    hourlyDailyList = []
    weeklyList = []
    weekTotal = 0
    for entry in ts['Forecast']:
        dt_dte = datetime.datetime.strptime(entry, '%Y-%m-%d')
        dte = dt_dte.strftime('%Y-%m-%d')
        orders = math.ceil(ts['Forecast'][entry])

        # Check if beyond 8 weeks - if so only do weekly here
        if dCount < 56:
            dailyList.append({'Date': dte, 'Hour': '10', 'Forecast': orders})
            if dCount < 21:
                day = dt_dte.strftime('%a')
                if MFC in ordersDict['MFCs_Hourly']:
                    curve = ordersDict['MFCs_Hourly'][MFC][day]
                else:
                    proxy = ordersDict['Regions'][MFC]
                    curve = ordersDict['Regions_Hourly'][proxy][day]
                    logger("I", MFC + " not in hourly dictionary - using " + proxy + " as proxy")
                for hr in range(0, 24):
                    hr = str(hr)
                    if hr in curve:
                        oph = math.ceil(curve[hr] * orders)
                    else:
                        oph = 0
                    hourlyDailyList.append({'Date': dte, 'Hour': hr, 'Forecast': oph})
            else:
                hourlyDailyList.append({'Date': dte, 'Hour': '10', 'Forecast': orders})
        dCount = dCount + 1
        weekTotal = weekTotal + orders
        if (dCount % 7) == 0:  # We have done a week
            commencing = (dt_dte - datetime.timedelta(days=dt_dte.weekday())).strftime('%Y-%m-%d')
            weeklyList.append({'Date': commencing, 'Forecast': weekTotal})
            weekTotal = 0

    retVal = {'HD': hourlyDailyList, 'D': dailyList, 'W': weeklyList}
    return retVal


def buildDictionaries():
    howFarBack = 4

    # We first create a dict of regions and MFCs and opening times
    logger("I", "Getting MFC definitions")
    liveMFCs = readFromGeneric('Live', '1GmOojxN2v0vjJKT_g3SfSv6EgLJ4eMLoKo08LlKTXFk', '!A:J')
    logger("I", "Done")
    GeoTags = {}
    OpeningCol = {}
    Opening = {}
    ActiveMFCs = []
    becomes = {}
    line = 0
    dayNames = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    for entry in liveMFCs:
        MFC = entry[0]
        GeoTag = entry[1]
        was = entry[2]
        ActiveMFCs.append(MFC)
        if line == 0:
            itr = 0
            for col in entry:
                if col in dayNames:
                    OpeningCol[col] = itr
                itr = itr + 1
            line = line + 1
        else:
            GeoTags[MFC] = GeoTag
            openingHours = {}
            for dn in dayNames:
                openingHours[dn] = entry[OpeningCol[dn]]
            Opening[MFC] = openingHours
            if len(was) > 0:
                becomes[was] = MFC

    # We then read all the Actual Data as a list of [Dte, MFC, Orders]
    actualOrders = readFromGeneric('Daily Order Dump (FR + UK) Runs Daily.csv',
                                   '1_l2aPXa7Huql-3u5MXfbqLq2HpTQd0HBRM8w4b09igo', '!A:C')
    # Iterating this data, we create a dictionary of dates and orders for each MFC
    Orders = {}
    actualOrders = actualOrders[1:]
    for entry in actualOrders:
        dte = entry[0]
        MFC = entry[1]
        actual = entry[2].replace(',', '').replace('.', '')
        if MFC in becomes:
            MFC = becomes[MFC]
        if len(MFC) > 0:
            if MFC in GeoTags:
                if MFC in ActiveMFCs:
                    if MFC in Orders:
                        Orders[MFC][dte] = actual
                    else:
                        Orders[MFC] = {dte: actual}

    # Finally read the hourly data as a list of [Date, MFC, Hour, Orders]
    hourlyOrders = readFromGeneric('Hourly Dump (FR + UK).csv',
                                   '1TjJpezxYxcet7VTWWFm-irtugOIfe7mjymUG7XId7rM', '!A:D')
    # Iterating this data to create an average hourly curve for each MFC for each Day
    hourly = {}
    hourly_G = {}
    hourlyOrders = hourlyOrders[1:]
    hourlyActuals = {}
    earliest = None
    latest = None
    for entry in hourlyOrders:
        dte = entry[0]
        dte = datetime.datetime.strptime(dte, '%Y-%m-%d')
        if earliest is None:
            earliest = dte
        else:
            if dte < earliest:
                earliest = dte

        if latest is None:
            latest = dte
        else:
            if dte > latest:
                latest = dte

        yyyymmdd = dte.strftime('%Y-%m-%d')
        dte = dte.strftime('%a')  # In format Mon, Tue, Wed etc...
        MFC = entry[1]
        hr = entry[2]
        actual = entry[3].replace(',', '').replace('.', '')

        if MFC in becomes:
            MFC = becomes[MFC]
        if MFC in ActiveMFCs:
            GeoTag = GeoTags[MFC]
            # hourly actuals.  This is the true data as opposed to average data.
            if MFC in hourlyActuals:
                if yyyymmdd in hourlyActuals[MFC]:
                    hourlyActuals[MFC][yyyymmdd][hr] = int(actual)
                else:
                    hourlyActuals[MFC][yyyymmdd] = {hr: int(actual)}
            else:
                hourlyActuals[MFC] = {yyyymmdd: {hr: int(actual)}}

            # populate the hourly json.  This is the average curve
            hourly = hourBuilder(MFC, hourly, dte, hr, actual)
            hourly_G = hourBuilder(GeoTag, hourly_G, dte, hr, actual)

    # Now normalise the curves
    normHourly = hourNormalise(hourly)
    normHourly_G = hourNormalise(hourly_G)

    # Now finally replace any entries without howFarBack entries with their geotag equiv average.
    howFarBack = math.floor(((latest - earliest).days + 1) / 7)
    logger('I', 'Earliest: ' + earliest.strftime('%d-%b-%Y'))
    logger('I', 'Latest: ' + latest.strftime('%d-%b-%Y'))
    logger('I', 'Delta (Inclusive): ' + str((latest - earliest).days + 1))
    logger('I', 'I am going back ' + str(howFarBack) + ' weeks')

    replacements = {}
    for MFC in hourly:
        if MFC in ActiveMFCs:
            for dte in hourly[MFC]:
                for entry in hourly[MFC][dte]:
                    if 'Count' in entry:
                        if hourly[MFC][dte][entry] < howFarBack:
                            replacements[MFC + ' ' + dte + ' - using some region values'] = 1
                            # pull missing from GeoTag
                            hr = entry.replace('Count', '')
                            GeoTag = GeoTags[MFC]
                            normHourly[MFC][dte][hr] = normHourly_G[GeoTag][dte][hr]

    for r in replacements:
        logger('I', r)

    'Now shift the opening hours for each MFC'

    retVal = {'MFCs_Hourly': normHourly,
              'Regions_Hourly': normHourly_G,
              'HourlyOrders': hourlyActuals,
              'DailyOrders': Orders,
              'Regions': GeoTags,
              'MFCList': ActiveMFCs}

    return retVal


def hourBuilder(key, p_json, dte, hr, actual):
    if key in p_json:
        if dte in p_json[key]:
            p_json[key][dte]['Tot'] = p_json[key][dte]['Tot'] + int(actual)
            if hr in p_json[key][dte]:
                p_json[key][dte][hr] = p_json[key][dte][hr] + int(actual)
                p_json[key][dte][str(hr) + 'Count'] = p_json[key][dte][str(hr) + 'Count'] + 1
            else:
                p_json[key][dte][hr] = int(actual)
                p_json[key][dte][str(hr) + 'Count'] = 1
        else:
            p_json[key][dte] = {hr: int(actual), 'Tot': int(actual), str(hr) + 'Count': 1}
    else:
        p_json[key] = {dte: {hr: int(actual), 'Tot': int(actual), str(hr) + 'Count': 1}}

    return p_json


def hourNormalise(p_json):
    retJson = {}
    for itr in p_json:
        for dte in p_json[itr]:
            for entry in p_json[itr][dte]:
                if 'Tot' not in entry and 'Count' not in entry:
                    normsVal = p_json[itr][dte][entry] / p_json[itr][dte]['Tot']
                    if itr in retJson:
                        if dte in retJson[itr]:
                            retJson[itr][dte][entry] = normsVal
                        else:
                            retJson[itr][dte] = {entry: normsVal}
                    else:
                        retJson[itr] = {dte: {entry: normsVal}}

    return retJson
