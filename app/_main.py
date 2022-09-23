import math
import datetime
import time
import json

from _loghandler import logger
from _prophet import doHPT, doForecast

from _readers import readFromGeneric
from _writers import writeHourly, writeParams, writeForecast


def weeklyForecastRoutine():

    failSleepTime = 30
    printOffset = 0

    # First read all live MFCs
    logger("I", "Reading MFC definitions")
    definitions = readMFCDefinitions()

    # Now determine maximum allowed data date.  This SUNDAY!!

    prevSunday = prior_Sunday()
    lastWeek = prevSunday - datetime.timedelta(days=7)
    logger("I", "Last Sunday is :" + str(prevSunday))
    logger("I", "Last Week is :" + str(lastWeek))

    # Read raw data
    logger("I", "Reading Raw Actual Data")
    hourlyRaw = readActuals(prevSunday, 'Hourly Dump (FR + UK).csv', '1TjJpezxYxcet7VTWWFm-irtugOIfe7mjymUG7XId7rM',
                            '!A:D')
    dailyRaw = readActuals(prevSunday, 'Daily Order Dump (FR + UK) Runs Daily.csv',
                           '1_l2aPXa7Huql-3u5MXfbqLq2HpTQd0HBRM8w4b09igo', '!A:C', False)
    dailyRaw_Historic = readActuals(lastWeek, 'Daily Order Dump (FR + UK) Runs Daily.csv',
                                    '1_l2aPXa7Huql-3u5MXfbqLq2HpTQd0HBRM8w4b09igo', '!A:C', False)

    activeMFCs = definitions['ActiveMFCs']
    geoTags = definitions['GeoTags']
    opensAt = definitions['OpensAt']
    closesAt = definitions['ClosesAt']
    becomes = definitions['Becomes']

    # Hour Curves
    logger("I", "Building Hourly Curves")
    cleanHours = {}
    for MFC in activeMFCs:
        succession = {}
        if MFC in becomes:
            succession = becomes[MFC]

        parsedData = parseForForecast(MFC, succession, hourlyRaw)
        for bad in parsedData['bad']:
            activeMFCs.remove(bad)

        cleanHours[MFC] = {'Hours': parsedData['json'], 'GeoTag': geoTags[MFC]}

    hourlyCurves = buildHourlyCurves(cleanHours)
    # Align with opening hours
    hourlyCurves = openingAlign(hourlyCurves, opensAt, closesAt)

    writeHourly(hourlyCurves['MFCs_Hourly'], 'H Crve MFC')
    writeHourly(hourlyCurves['Regions_Hourly'], 'H Crve Rgn')

    # Now tha actual calculation loop
    logger("I", "Main Loop")
    itr = 1
    outRowByCountryD = {}
    outRowByCountryHD = {}
    outRowByCountryW = {}
    rows = {'D': outRowByCountryD, 'HD': outRowByCountryHD, 'W': outRowByCountryW, 'I': itr, 'P': printOffset}
    for MFC in activeMFCs:
        result = False
        while not result:
            try:
                rows = mainLoop(MFC, geoTags, becomes, hourlyRaw, dailyRaw, dailyRaw_Historic, prevSunday, lastWeek,
                                hourlyCurves, rows)
                result = True
            except Exception as e:
                logger('W', '')
                logger('W', repr(e))
                logger('W', MFC + ' Failed.  Sleeping ' + str(failSleepTime) + '...')
                time.sleep(failSleepTime)
                logger('I', 'Trying ' + MFC + ' again')

        itr = itr + 1
        rows['I'] = itr


def mainLoop(MFC, geoTags, becomes, hourlyRaw, dailyRaw, dailyRaw_Historic, yesterday, lastWeek, hourlyCurves, rows):
    outRowByCountryD = rows['D']
    outRowByCountryHD = rows['HD']
    outRowByCountryW = rows['W']
    itr = rows['I']
    printOffset = rows['P']

    succession = {}
    ctry = geoTags[MFC].split('_')[0]
    if MFC in becomes:
        succession = becomes[MFC]

    parsed = parseForForecast(MFC, succession, dailyRaw)
    ts = parsed['ts']
    logger("I", "Calculating Parameters for " + MFC)
    params = calcParams(MFC, ts, ctry, itr)
    logger("I", "Running forecasts for " + MFC)
    forecast_ts = doForecast(MFC, yesterday, ts, ctry, params['Best'])

    logger("I", "Exporting Results.")

    # Now multiply out the forecast_ts with the day curve.  Always round up and floats.
    outputs = createOutputsByHourDayWeek(MFC, forecast_ts, hourlyCurves)
    hourlyDailyList = outputs['HD']
    dailyList = outputs['D']
    weeklyList = outputs['W']

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

    rows = {'D': outRowByCountryD, 'HD': outRowByCountryHD,
            'W': outRowByCountryW, 'I': itr, 'P': printOffset}
    return rows


def dteBookEnds(earliest, latest, dte):
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
    return {'Earliest': earliest, 'Latest': latest}


def readMFCDefinitions():
    logger("I", "Getting MFC definitions")
    liveMFCs = readFromGeneric('Live', '1GmOojxN2v0vjJKT_g3SfSv6EgLJ4eMLoKo08LlKTXFk', '!A:L')
    logger("I", "Done")
    GeoTags = {}
    OpeningCol = {}
    Opening = {}
    Closing = {}
    ActiveMFCs = []
    becomes = {}
    line = 0
    dayNames = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    for entry in liveMFCs:
        MFC = entry[0]
        GeoTag = entry[1]
        was = entry[2]
        frac = entry[3]
        if line == 0:
            itr = 0
            for col in entry:
                if col in dayNames:
                    OpeningCol[col] = itr
                itr = itr + 1
            line = line + 1
        else:
            ActiveMFCs.append(MFC)
            GeoTags[MFC] = GeoTag
            openingHours = {}
            closingHours = {}
            for dn in dayNames:
                OpenClose = str(entry[OpeningCol[dn]])
                ClosesAt = '00'
                OpensAt = OpenClose.split('-')[0]
                if '-' in OpenClose:
                    ClosesAt = OpenClose.split('-')[1]
                openingHours[dn] = OpensAt
                closingHours[dn] = ClosesAt
            Opening[MFC] = openingHours
            Closing[MFC] = closingHours
            if len(was) > 0:
                becomes[MFC] = {'MFC': was, 'Fraction': frac}

    ActiveMFCs.sort()
    return {'GeoTags': GeoTags, 'ActiveMFCs': ActiveMFCs, 'OpensAt': Opening, 'ClosesAt': Closing, 'Becomes': becomes}


def readActuals(yesterday, sheet_Name, sheet_ID, sheet_Columns, containsHours=True):
    # hourlyOrders = readFromGeneric('Hourly Dump (FR + UK).csv','1TjJpezxYxcet7VTWWFm-irtugOIfe7mjymUG7XId7rM', '!A:D')
    actuals = readFromGeneric(sheet_Name, sheet_ID, sheet_Columns)
    actuals = actuals[1:]
    retJson = {}
    earliest = None
    latest = None
    for entry in actuals:
        dte = entry[0]
        dte = datetime.datetime.strptime(dte, '%Y-%m-%d')
        yyyymmdd = dte.strftime('%Y-%m-%d')
        dates = dteBookEnds(earliest, latest, dte)
        earliest = dates['Earliest']
        latest = dates['Latest']

        MFC = entry[1]

        if containsHours:
            hr = entry[2]
            actual = entry[3].replace(',', '').replace('.', '')
        else:
            hr = '10'
            actual = entry[2].replace(',', '').replace('.', '')

        if dte <= yesterday:
            if MFC in retJson:
                if yyyymmdd in retJson[MFC]:
                    retJson[MFC][yyyymmdd][hr] = int(actual)
                else:
                    retJson[MFC][yyyymmdd] = {hr: int(actual)}
            else:
                retJson[MFC] = {yyyymmdd: {hr: int(actual)}}

    return retJson


def calcParams(MFC, ts, ctry, itr):
    logger("I", "Running " + MFC + " parameter tuning - " + datetime.datetime.now().strftime('%d-%b-%Y, %H:%M:%S'))
    prophetParams = doHPT(MFC, ts, ctry)
    logger("I", "Finished " + MFC + " parameter tuning - " + datetime.datetime.now().strftime('%d-%b-%Y, %H:%M:%S'))
    writeParams(prophetParams['MFC'], prophetParams['Best'], prophetParams['Ctry'], itr)
    return prophetParams


def parseForForecast(MFC, succession, ts):
    tsJson = {}
    earliest = None
    latest = None
    if len(succession) > 0:
        frac = succession['Fraction']
        was = succession['MFC']
        if was in ts:
            loc_ts = ts[was]
            frac = float(frac.replace('%', '')) / 100
            for dte in loc_ts:
                dates = dteBookEnds(earliest, latest, dte)
                earliest = dates['Earliest']
                latest = dates['Latest']
                for hr in loc_ts[dte]:
                    historic = int(loc_ts[dte][hr]) * frac
                    if dte in tsJson:
                        if hr in tsJson[dte]:
                            tsJson[dte][hr] = tsJson[dte][hr] + historic
                        else:
                            tsJson[dte][hr] = historic
                    else:
                        tsJson[dte] = {hr: historic}

    badMFC = []
    if MFC in ts:
        for dte in ts[MFC]:
            dates = dteBookEnds(earliest, latest, dte)
            earliest = dates['Earliest']
            latest = dates['Latest']
            for hr in ts[MFC][dte]:
                historic = int(ts[MFC][dte][hr])
                if dte in tsJson:
                    if hr in tsJson[dte]:
                        tsJson[dte][hr] = tsJson[dte][hr] + historic
                    else:
                        tsJson[dte][hr] = historic
                else:
                    tsJson[dte] = {hr: historic}
    else:
        logger("I", MFC + " not in raw TS data.  Needs to be removed.")
        badMFC.append(MFC)

    earliest = datetime.datetime.strptime(earliest, '%Y-%m-%d')
    latest = datetime.datetime.strptime(latest, '%Y-%m-%d')

    out_ts = []
    out_json = {}
    delta = datetime.timedelta(days=1)
    while earliest <= latest:
        dte = earliest.strftime('%Y-%m-%d')
        if dte in tsJson:
            for hr in range(0, 24):
                actual = 0
                hr = str(hr)
                if hr in tsJson[dte]:
                    actual = tsJson[dte][hr]
                    # out_ts.append({"ds": dte, "hr": hr, "y": actual})
                    out_ts.append({"ds": dte, "y": actual})
                if dte in out_json:
                    out_json[dte][hr] = actual
                else:
                    out_json[dte] = {hr: actual}

        earliest += delta
    return {'ts': out_ts, 'json': out_json, 'bad': badMFC}


def buildHourlyCurves(bigJson):
    GeoTags = {}
    hourly = {}
    hourly_G = {}
    earliest = None
    latest = None
    for MFC in bigJson:
        for dte in bigJson[MFC]['Hours']:
            dte = datetime.datetime.strptime(dte, '%Y-%m-%d')
            str_dte = dte.strftime('%Y-%m-%d')
            dayname_dte = dte.strftime('%a')
            dates = dteBookEnds(earliest, latest, dte)
            earliest = dates['Earliest']
            latest = dates['Latest']
            GeoTag = bigJson[MFC]['GeoTag']
            GeoTags[MFC] = GeoTag
            for hr in bigJson[MFC]['Hours'][str_dte]:
                actual = bigJson[MFC]['Hours'][str_dte][hr]
                # populate the hourly json.  This will be the average curve
                hourly = hourBuilder(MFC, hourly, dayname_dte, hr, actual)
                hourly_G = hourBuilder(GeoTag, hourly_G, dayname_dte, hr, actual)

    # Now normalise the curves
    normHourly = hourNormalise(hourly)
    normHourly_G = hourNormalise(hourly_G)

    # Now finally replace any entries without howFarBack entries with their geotag equiv average.
    howFarBack = math.floor(((latest - earliest).days + 1) / 7)
    logger('I', 'Hourly Curve Earliest: ' + earliest.strftime('%d-%b-%Y'))
    logger('I', 'Hourly Curve Latest: ' + latest.strftime('%d-%b-%Y'))
    logger('I', 'Hourly Curve Delta (Inclusive): ' + str((latest - earliest).days + 1))
    logger('I', 'Hourly Curve - I am going back ' + str(howFarBack) + ' weeks')

    for MFC in hourly:
        for dte in hourly[MFC]:
            for entry in hourly[MFC][dte]:
                if 'Count' in entry:
                    if hourly[MFC][dte][entry] < howFarBack:
                        # pull missing from GeoTag
                        hr = entry.replace('Count', '')
                        GeoTag = GeoTags[MFC]
                        normHourly[MFC][dte][hr] = normHourly_G[GeoTag][dte][hr]

    'Now shift the opening hours for each MFC'

    retVal = {'MFCs_Hourly': normHourly,
              'Regions_Hourly': normHourly_G,
              'Regions': GeoTags}

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


def createOutputsByHourDayWeek(MFC, ts, hourlyCurves):
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
                if MFC in hourlyCurves['MFCs_Hourly']:
                    curve = hourlyCurves['MFCs_Hourly'][MFC][day]
                else:
                    proxy = hourlyCurves['Regions'][MFC]
                    curve = hourlyCurves['Regions_Hourly'][proxy][day]
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


def prior_Sunday():
    sundayStr = (
                datetime.datetime.now() - datetime.timedelta(days=(datetime.datetime.now().isoweekday() % 7))).strftime(
        '%Y-%m-%d')
    return datetime.datetime.strptime(sundayStr, '%Y-%m-%d')


def openingAlign(hourlyCurves, opensAt, closesAt):
    hourlies = hourlyCurves['MFCs_Hourly']

    for MFC in hourlies:
        historic = hourlies[MFC]
        opening = opensAt[MFC]
        closing = closesAt[MFC]
        alignedCurve = {}

        for d in historic:
            minSales = 1
            for h in range(0,24):
                h = str(h)
                if historic[d][h] < minSales:
                    if historic[d][h] > 0:
                        minSales = historic[d][h]
            openingHour = int(opening[d])
            closingHour = int(closing[d])
            newCurve = {}
            total = 0
            if openingHour < closingHour:
                for h in range(0, openingHour):
                    h = str(h)
                    newCurve[h] = 0
                for h in range(openingHour, closingHour + 1):
                    h = str(h)
                    newCurve[h] = min(minSales, historic[d][h])
                    total = total + newCurve[h]
                if closingHour < 23:
                    for h in range(closingHour, 24):
                        h = str(h)
                        newCurve[h] = 0
            else:
                for h in range(0, closingHour):
                    h = str(h)
                    newCurve[h] = expectedLoad(minSales, historic[d][h])
                    total = total + newCurve[h]
                for h in range(closingHour, openingHour):
                    h = str(h)
                    newCurve[h] = 0
                for h in range(openingHour, 24):
                    h = str(h)
                    newCurve[h] = expectedLoad(minSales, historic[d][h])
                    total = total + newCurve[h]

            finalCurve={}
            for h in newCurve:
                normed = newCurve[h]/total
                finalCurve[h] = normed
            alignedCurve[d] = finalCurve

        hourlies[MFC] = alignedCurve

    hourlyCurves['MFCs_Hourly'] = hourlies
    return hourlyCurves


def expectedLoad(minSales, recorded):
    retVal = 0
    if recorded == 0:
        retVal = minSales
    else:
        retVal = recorded
    return retVal