import csv
from fileinput import close
from operator import gt
from random import betavariate
from sqlite3 import Timestamp
from statistics import mean
import string
from timeit import repeat
import os
from os import environ
import numpy as np
import time
from datetime import datetime
from collections import Counter
from progress.bar import Bar


# def suppress_qt_warnings():
#     environ["QT_DEVICE_PIXEL_RATIO"] = "0"
#     environ["QT_AUTO_SCREEN_SCALE_FACTOR"] = "1"
#     environ["QT_SCREEN_SCALE_FACTORS"] = "1"
#     environ["QT_SCALE_FACTOR"] = "1"

def read_header(fileName):
    f = open(fileName, 'r', encoding='GBK')
    reader = csv.reader(f)
    header = next(reader)
    idxRec = {}
    for i in range(0, len(header)):
        if header[i] == 'devEUI' or header[i] == 'deveui':
            idxRec['devEUI'] = i
        if header[i] == 'rssi':
            idxRec['rssi'] = i
        if header[i] == 'snr':
            idxRec['snr'] = i
        if header[i] == 'gateway':
            idxRec['gateway'] = i
        if header[i] == 'frequency':
            idxRec['frequency'] = i
        if header[i] == 'time' or header[i] == 'ts':
            idxRec['time'] = i
        if header[i] == 'fCnt' or header[i] == 'fcnt':
            idxRec['fCnt'] = i
        if header[i] == 'repeat':
            idxRec['repeat'] = i
        if header[i] == 'sf':
            idxRec['sf'] = i
        if header[i] == 'companyId' or header[i] == 'companyid':
            idxRec['companyId'] = i
        if header[i] == 'gasNumber' or header[i] == 'gasnumber':
            idxRec['gasNumber'] = i
        if header[i] == 'messageLength' or header[i] == 'messagelength':
            idxRec['messageLength'] = i
        if header[i] == 'messageType' or header[i] == 'messagetype':
            idxRec['messageType'] = i
        if header[i] == 'typeService' or header[i] == 'typeservice':
            idxRec['typeService'] = i
    f.close()
    return idxRec

# ============ read a row to an object =============
def read_row(row, idxRec):
    if "'" in row[idxRec['devEUI']]:
        timeStr = row[idxRec['time']].replace("'", "")
        if "." in timeStr:
            time_obj = datetime.strptime(timeStr, "%Y-%m-%d %H:%M:%S.%f")
        else:
            time_obj = datetime.strptime(timeStr, "%Y-%m-%d %H:%M:%S")
        
        timeStamp = time.mktime(time_obj.timetuple()) + (time_obj.microsecond / 1000000.0)
        record = {"gtwID": row[idxRec['gateway']].replace("'", ""), 
                "devID" : row[idxRec['devEUI']].replace("'", ""), 
                "rssi" : float(row[idxRec['rssi']]),
                "snr" : float(row[idxRec['snr']]),
                "fcnt" : int(row[idxRec['fCnt']]),
                "repeat" : int(row[idxRec['repeat']]),
                "sf" : int(row[idxRec['sf']]),
                "freq" : int(row[idxRec['frequency']].replace("'", "")),
                "time" : row[idxRec['time']].replace("'", ""),
                "timeStamp" : timeStamp,
                "companyID": row[idxRec['companyId']].replace("'", ""),
                "gasNumber": row[idxRec['gasNumber']].replace("'", ""),
                "messageLen": row[idxRec['messageLength']],
                "messageType": row[idxRec['messageType']].replace("'", ""),
                "typeService": row[idxRec['typeService']].replace("'", "")}
    else:
        timeStr = row[idxRec['time']].replace("T", " ").replace("+08:00","")
        if "." in timeStr:
            time_obj = datetime.strptime(timeStr, "%Y-%m-%d %H:%M:%S.%f")
        else:
            time_obj = datetime.strptime(timeStr, "%Y-%m-%d %H:%M:%S")
        timeStamp = time.mktime(time_obj.timetuple()) + (time_obj.microsecond / 1000000.0)
        record = {"gtwID": row[idxRec['gateway']], 
                "devID" : row[idxRec['devEUI']], 
                "rssi" : float(row[idxRec['rssi']]),
                "snr" : float(row[idxRec['snr']]),
                "fcnt" : int(row[idxRec['fCnt']]),
                "repeat" : int(row[idxRec['repeat']]),
                "sf" : int(row[idxRec['sf']]),
                "freq" : int(row[idxRec['frequency']]),
                "time" :  row[idxRec['time']].replace("T", " ").replace("+08:00",""), 
                "timeStamp" : timeStamp,
                "companyID": row[idxRec['companyId']],
                "gasNumber": row[idxRec['gasNumber']],
                "messageLen": row[idxRec['messageLength']],
                "messageType": row[idxRec['messageType']],
                "typeService": row[idxRec['typeService']]}   
    return record


def sort_by_dev(filePath, outFilePath):
    fileList = os.listdir(filePath)
    
    for file in fileList:
        if 'iot_message_info' not in file:
            continue
        print(file)
        devEUIdict = {}
        devService = {}
        devEUIval = []
        idxRec = read_header(filePath + file)

        f = open(filePath + file, 'r', encoding='GBK')
        reader = csv.reader(f)
        next(reader)
        
        for row in reader:
            # if row[repeatIdx] == '1':
            #     continue
            record = read_row(row, idxRec)
            devID = record['devID']
            if devID not in devEUIdict:
                devEUIdict[devID] = devEUIdict.__len__()
                devEUIval.append([])
                devService[devID] = record['typeService']
            devEUIval[devEUIdict[devID]].append(record)

        for devID in devEUIdict:
            serviceType = devService[devID]
            if not os.path.exists(outFilePath + serviceType):
                os.makedirs(outFilePath + serviceType)
            outFileName = outFilePath + serviceType + '/' + devID + ".csv"
            if os.path.exists(outFileName):
                outfile = open(outFileName, 'a', newline="")
                mWriter = csv.writer(outfile, dialect='excel')
            else:
                outfile = open(outFileName, 'w', newline="")
                mWriter = csv.writer(outfile, dialect='excel')
                mWriter.writerow(['deveui', 'gateway', 'rssi', 'snr', 'fcnt', 'repeat', 'sf', 'frequency', 'time', 'timeStamp', 'companyid', 'gasnumber', 'messagelength', 'messagetype', 'typeservice'])

            recList = sorted(devEUIval[devEUIdict[devID]], key=lambda r: r['timeStamp'])
            for record in recList:
                if "." in record['time']:
                    time_obj = datetime.strptime(record['time'], "%Y-%m-%d %H:%M:%S.%f")
                else:
                    time_obj = datetime.strptime(record['time'], "%Y-%m-%d %H:%M:%S")
                base_time = datetime.strptime('2022-10-19 00:00:00', "%Y-%m-%d %H:%M:%S")
                if 'iot_message_info_2022-10-18' in file and time_obj > base_time:
                    continue
                base_time = datetime.strptime('2022-10-21 18:00:00', "%Y-%m-%d %H:%M:%S")
                if 'iot_message_info_2022-10-19' in file and time_obj > base_time:
                    continue

                mWriter.writerow([devID, record['gtwID'], record['rssi'], record['snr'], record['fcnt'], record['repeat'], record['sf'],
                                record['freq'], record['time'], record['timeStamp'], record['companyID'], record['gasNumber'], 
                                record['messageLen'], record['messageType'], record['typeService']])
            outfile.close()

def sort_by_gtw(filePath, outFilePath):
    fileList = os.listdir(filePath)
    for file in fileList:
        if 'iot_message_info' not in file:
            continue
        print(file)
        gtwDict = {}
        gtwVal = []
        idxRec = read_header(filePath + file)

        f = open(filePath + file, 'r', encoding='GBK')
        reader = csv.reader(f)
        next(reader)
        
        for row in reader:
            # if row[repeatIdx] == '1':
            #     continue
            record = read_row(row, idxRec)
            gtwID = record['gtwID']     

            if "." in record['time']:
                time_obj = datetime.strptime(record['time'], "%Y-%m-%d %H:%M:%S.%f")
            else:
                time_obj = datetime.strptime(record['time'], "%Y-%m-%d %H:%M:%S")
            base_time = datetime.strptime('2022-10-19 00:00:00', "%Y-%m-%d %H:%M:%S")
            if 'iot_message_info_2022-10-18' in file and time_obj > base_time:
                continue
            base_time = datetime.strptime('2022-10-21 18:00:00', "%Y-%m-%d %H:%M:%S")
            if 'iot_message_info_2022-10-19' in file and time_obj > base_time:
                continue   

            if gtwID not in gtwDict:
                gtwDict[gtwID] = gtwDict.__len__()
                gtwVal.append([])
            gtwVal[gtwDict[gtwID]].append(record)

        for gtwID in gtwDict:
            outFileName = outFilePath + gtwID + ".csv"
            if os.path.exists(outFileName):
                outfile = open(outFileName, 'a', newline="")
                mWriter = csv.writer(outfile, dialect='excel')
            else:
                outfile = open(outFileName, 'w', newline="")
                mWriter = csv.writer(outfile, dialect='excel')
                mWriter.writerow(['deveui', 'gateway', 'rssi', 'snr', 'fcnt', 'repeat', 'sf', 'frequency', 'time', 'timeStamp', 'companyid', 'gasnumber', 'messagelength', 'messagetype', 'typeservice'])

            recList = sorted(gtwVal[gtwDict[gtwID]], key=lambda r: r['timeStamp'])
            for record in recList:
                mWriter.writerow([record['devID'], record['gtwID'], record['rssi'], record['snr'], record['fcnt'], record['repeat'], record['sf'],
                                record['freq'], record['time'], record['timeStamp'], record['companyID'], record['gasNumber'], 
                                record['messageLen'], record['messageType'], record['typeService']])
            outfile.close()

def read_gen_file(fileName, ignoreRepeat):
    # print(fileName)
    idxRec = read_header(fileName)
    f = open(fileName, 'r', encoding='GBK')
    reader = csv.reader(f)
    next(reader)

    transRec = []
    for row in reader:
        if row.__len__() == 0 or (ignoreRepeat and row[idxRec['repeat']] == '1'):
            continue
        record = read_row(row, idxRec)
        transRec.append(record)
    return transRec
        

def pkt_loss(inFilePath, outFilePath):
    outf = open(outFilePath, 'w', newline="")
    writer = csv.writer(outf, dialect='excel')
    writer.writerow(['devEUI','packet number','packet loss','loss rate'])
    
    fileList = os.listdir(inFilePath)
    ignoreRepeat = True
    for file in fileList:
        transList = read_gen_file(inFilePath + file, ignoreRepeat)
        devID = file.replace(".csv", "")
        pktLoss = 0
        recList = sorted(transList, key=lambda r: r['timeStamp'])  
        prevCnt = 1e5
        for record in recList:
            currCnt = record.get("fcnt")
            if currCnt - prevCnt > 1:
                pktLoss += currCnt - prevCnt
            prevCnt = currCnt
        if recList.__len__() > 0:
            lossRate =  pktLoss/(pktLoss + recList.__len__())
        else:
            lossRate = 1
        writer.writerow([devID, recList.__len__(), pktLoss, lossRate])
    outf.close()

def trans_anal(inFilePath, outFilePath):
    outf = open(outFilePath, 'w', newline="")
    writer = csv.writer(outf, dialect='excel')
    writer.writerow(['devEUI','packet number','work days','max packet/day', 'longest period'])

    fileList = os.listdir(inFilePath)
    bar = Bar('Processing', max=fileList.__len__())
    for file in fileList:
        transList = read_gen_file(inFilePath + file, True)
        bar.next()
        devID = file.replace(".csv", "")

        lgstPrd = 0
        maxNum = 0
        tmpMaxNum = 0
        workDays = 0
        recList = sorted(transList, key=lambda r: r['timeStamp'])
        for record in recList:
            if "." in record.get("time"):
                time_obj = datetime.strptime(record.get("time"), "%Y-%m-%d %H:%M:%S.%f")
            else:
                time_obj = datetime.strptime(record.get("time"), "%Y-%m-%d %H:%M:%S")
            dt = datetime(time_obj.year, time_obj.month, time_obj.day)
            # print(dt)
            if 'prevDt' not in dir():
                prevDt = dt
                maxNum = 1
                tmpMaxNum = 1
                lgstPrd = 1
                workDays = 1
            else:
                if dt == prevDt:
                    # print('Same day')
                    tmpMaxNum += 1
                    if tmpMaxNum > maxNum:
                        maxNum = tmpMaxNum
                else:
                    # print('New day with gap = ' + str((dt - prevDt).days))
                    if (dt - prevDt).days > lgstPrd:
                        lgstPrd = (dt - prevDt).days
                    tmpMaxNum = 1
                    workDays += 1
                prevDt = dt
        if 'prevDt' in dir():
            del prevDt
        writer.writerow([devID, recList.__len__(), workDays, maxNum, lgstPrd])
    outf.close()
    bar.finish()

def snr_anal_dev(inFilePath, transPath, devPath):
    outf = open(devPath, 'w', newline="")
    writer = csv.writer(outf, dialect='excel')
    writer.writerow(['devEUI','numPkt','maxSNR','minSNR','maxRSSI','minRSSI','SNR Fluctuation', 'RSSI Fluctuation', 'good rate', 'mid rate', 'bad rate'])
    snrALL = []
    rssiALL = []

    fileList = os.listdir(inFilePath)
    for file in fileList:
        devID = file.replace(".csv", "")
        transList = read_gen_file(inFilePath + file)
        snrSet = []
        rssiSet = []
        recList = sorted(transList, key=lambda r: r['timeStamp'])
        for record in recList:
            snrSet.append(record.get("snr"))
            rssiSet.append(record.get("rssi"))

        goodNum = sum(itm > 0 for itm in snrSet)
        midNum = sum(itm > -10 and itm <= 0 for itm in snrSet)
        badNum = sum(itm < -10 for itm in snrSet)

        if recList.__len__() > 0:
            writer.writerow([devID, recList.__len__(), max(snrSet), min(snrSet), max(rssiSet), min(rssiSet), 
                            max(snrSet) - min(snrSet), max(rssiSet) - min(rssiSet),
                            goodNum/recList.__len__(), midNum/recList.__len__(), badNum/recList.__len__()])
        # else:
        #     writer.writerow([devID, recList.__len__()])

        rssiALL.extend(rssiSet)
        snrALL.extend(snrSet)

    outf.close() 
    outf = open(transPath, 'w', newline="")
    writer = csv.writer(outf, dialect='excel')
    writer.writerow(snrALL)
    writer.writerow(rssiALL)
    outf.close()

def dev_gtw_all(inFilePath, transOutFile, devOutFile):
    outf = open(transOutFile, 'w', newline="")
    writer = csv.writer(outf, dialect='excel')
    writer.writerow(['devEUI','transID','time', 'selectGTW', 'nGTW', 'maxSNR', 'maxRSSI', 'snrGTW', 'rssiGTW', 'good rate', 'mid rate', 'bad rate'])
    outf2 = open(devOutFile, 'w', newline="")
    writerDev = csv.writer(outf2, dialect='excel')
    writerDev.writerow(['devEUI', 'nTrans', 'nGTW Avg', 'nGTW Max', 'nGTW Min', 'Sel GTWs', '1st rate', '2nd rate', '3rd rate', '1st GTW', '2nd GTW', '3rd GTW'])

    fileList = os.listdir(inFilePath)
    for file in fileList:
        devID = file.replace(".csv", "")
        transList = read_gen_file(inFilePath + file)
        recList = sorted(transList, key=lambda r: r['timeStamp'])
        bstGtwList = []
        selGtwList = []
        numGtwList = []
        recGtwEachTrans = []
        for record in recList:
            if 'prevTime' not in dir():
                prevTime = record.get('timeStamp')
                snrSet = [record['snr']]
                nTrans = 1
                mTime = record['time']
                bestGTW = {'maxSNR':record.get('snr'), 'maxRSSI':record.get('rssi'), 'snrGTW':record.get('gtwID'), 'rssiGTW':record.get('gtwID')}
                bstGtwList.append(record['gtwID'])
                tmpGtwList = {record['gtwID']:record['snr']}
                selGtw = None
                if record['repeat'] == 0:
                    selGtw = record['gtwID']
                continue
            currTime = record.get('timeStamp')
            if abs(currTime - prevTime) < 1:
                snrSet.append(record['snr'])
                if bestGTW['maxSNR'] < record['snr']:
                    bestGTW['maxSNR'] = record['snr']
                    bestGTW['snrGTW'] = record['gtwID']
                if bestGTW['maxRSSI'] < record['rssi']:
                    bestGTW['maxRSSI'] = record['rssi']
                    bestGTW['rssiGTW'] = record['gtwID']
                if record['repeat'] == 0:
                    selGtw = record['gtwID']
                tmpGtwList[record['gtwID']] = record['snr']
            else:
                goodNum = sum(itm > 0 for itm in snrSet)
                midNum = sum(itm > -10 and itm <= 0 for itm in snrSet)
                badNum = sum(itm < -10 for itm in snrSet)
                writer.writerow([devID, nTrans, mTime, selGtw, snrSet.__len__(),  
                                bestGTW['maxSNR'], bestGTW['maxRSSI'], bestGTW['snrGTW'], bestGTW['rssiGTW'],
                                goodNum/snrSet.__len__(), midNum/snrSet.__len__(), badNum/snrSet.__len__()])
                bstGtwList.pop()
                bstGtwList.append(bestGTW['snrGTW'])
                selGtwList.append(selGtw)
                numGtwList.append(snrSet.__len__())
                recGtwEachTrans.append(sorted(tmpGtwList.items(), key=lambda x: x[1], reverse = True))

                snrSet = [record['snr']]
                nTrans += 1
                mTime = record['time']
                bestGTW = {'maxSNR':record.get('snr'), 'maxRSSI':record.get('rssi'), 'snrGTW':record.get('gtwID'), 'rssiGTW':record.get('gtwID')}
                bstGtwList.append(record['gtwID'])
                tmpGtwList = {record['gtwID']:record['snr']}
                selGtw = None
                if record['repeat'] == 0:
                    selGtw = record['gtwID']
            prevTime = currTime

        goodNum = sum(itm > 0 for itm in snrSet)
        midNum = sum(itm > -10 and itm <= 0 for itm in snrSet)
        badNum = sum(itm < -10 for itm in snrSet)
        writer.writerow([devID, nTrans, mTime, selGtw, snrSet.__len__(),  
                bestGTW['maxSNR'], bestGTW['maxRSSI'], bestGTW['snrGTW'], bestGTW['rssiGTW'],
                goodNum/snrSet.__len__(), midNum/snrSet.__len__(), badNum/snrSet.__len__()])
        if selGtw != None:
            selGtwList.append(selGtw)
        numGtwList.append(snrSet.__len__())

        recGtwEachTrans.append(sorted(tmpGtwList.items(), key=lambda x: x[1], reverse = True))

        # print('selGtwList:' + str(selGtwList))
        diffSleGtws = set(selGtwList).__len__()
        FirstList = []
        SecondList = []
        ThirdList = []
        for item in recGtwEachTrans:
            gtws = []
            for i in item:
                gtws.append(i[0])
            if len(gtws) > 2:
                FirstList.append(gtws[0])
                SecondList.extend(gtws[0:2])
                ThirdList.extend(gtws[0:3])
            elif len(gtws) > 1:
                FirstList.append(gtws[0])
                SecondList.extend(gtws[0:2])
                ThirdList.extend(gtws[0:2])
            elif len(gtws) > 0:
                FirstList.append(gtws[0])
                SecondList.append(gtws[0])
                ThirdList.append(gtws[0])
        # print("FirstList:" + str(FirstList) + "  " + str(dict(Counter(FirstList))))
        # print("SecondList:" + str(SecondList))
        # print("ThirdList:" + str(SecondList))
        
        gtwTop1 = sorted(dict(Counter(FirstList)).items(), key=lambda x: x[1], reverse = True)
        # print("TOP1: " + str(gtwTop1))
        rate1 = gtwTop1[0][1] / nTrans
        gtwTop2 = sorted(dict(Counter(SecondList)).items(), key=lambda x: x[1], reverse = True)
        # print("TOP2: " + str(gtwTop2))
        rate2 = gtwTop2[0][1] / nTrans
        gtwTop3 = sorted(dict(Counter(ThirdList)).items(), key=lambda x: x[1], reverse = True)
        # print("TOP3: " + str(gtwTop3))
        rate3 = gtwTop3[0][1] / nTrans

        writerDev.writerow([devID, nTrans, mean(numGtwList), max(numGtwList), min(numGtwList), diffSleGtws, rate1, rate2, rate3,
                            gtwTop1[0][0], gtwTop2[0][0], gtwTop3[0][0]])

        del prevTime
        # print(key)
        # print(bstGtwList)
    outf.close()
    outf2.close()


def gtw_all(inFilePath, outFilePath):
    outf = open(outFilePath, 'w', newline="")
    writer = csv.writer(outf, dialect='excel')
    writer.writerow(['gtwID','nSelect','nAll', 'good rate', 'mid rate', 'bad rate', 'good rate (ALL)', 'mid rate (ALL)', 'bad rate (ALL)'])

    fileList = os.listdir(inFilePath)
    bar = Bar('Processing', max = fileList.__len__())
    for file in fileList:
        gtwID = file.replace(".csv", "")
        transList = read_gen_file(inFilePath + file)
        recList = sorted(transList, key=lambda r: r['timeStamp'])
        bar.next()

        devList = set()
        selDevList = set()
        snrListAll = []
        snrListSel = []
        for record in recList:
            devID = record['devID']
            snr = record['snr']
            devList.add(devID)
            snrListAll.append(snr)
            if record['repeat'] == 0:
                selDevList.add(devID)
                snrListSel.append(snr)               

        goodNum = sum(itm > 0 for itm in snrListSel)
        midNum = sum(itm > -10 and itm <= 0 for itm in snrListSel)
        badNum = sum(itm < -10 for itm in snrListSel)
        goodNumAll = sum(itm > 0 for itm in snrListAll)
        midNumAll = sum(itm > -10 and itm <= 0 for itm in snrListAll)
        badNumAll = sum(itm < -10 for itm in snrListAll)
        if snrListAll.__len__() > 0:
            writer.writerow([gtwID, len(selDevList), len(devList),
                    goodNum/snrListSel.__len__(), midNum/snrListSel.__len__(), badNum/snrListSel.__len__(),
                    goodNumAll/snrListAll.__len__(), midNumAll/snrListAll.__len__(), badNumAll/snrListAll.__len__()])
    outf.close()
    bar.finish()

def gtw2dev(gtwRecPath, devPath, outPath):
    if not os.path.exists(outPath):
        os.makedirs(outPath)
    devTotalTrans = {}
    f = open(devPath, 'r', encoding='GBK')
    reader = csv.reader(f)
    next(reader) # skip header
    for row in reader:
        if row.__len__() > 0:
            devID = row[0]
            devTotalTrans[devID] = int(row[1])
    f.close()

    fileList = os.listdir(gtwRecPath)
    bar = Bar('Processing', max = fileList.__len__())
    for file in fileList:
        gtwID = file.replace(".csv", "")
        transList = read_gen_file(gtwRecPath + file, False)
        recList = sorted(transList, key=lambda r: r['timeStamp'])
        bar.next()

        devTransNum = {}
        for record in recList:
            if record['typeService'] != 'ENN燃气表':
                continue
            devID = record['devID']
            snr = record['snr']
            if devID not in devTransNum:
                devTransNum[devID] = [snr]
            else:
                devTransNum[devID].append(snr)

        # write out transmission number for each gateway
        outf = open(outPath+gtwID+'.csv', 'w', newline="")
        writer = csv.writer(outf, dialect='excel')
        writer.writerow(['gtwID','devID','trans_number', 'dev_total_number', 'gtw_rx_ratio', 'low_SNR_trans', 'middle_SNR_trans', 'high_SNR_trans'])
        for key in devTransNum:
            nTrans = devTransNum[key].__len__()
            nHigh = sum(i >= 0 for i in devTransNum[key])
            nLow = sum(i < -10 for i in devTransNum[key])
            nMiddle = nTrans - nHigh - nLow
            assert key in devTotalTrans
            if devTotalTrans[key] == 0:
                writer.writerow([gtwID, key, nTrans, devTotalTrans[key], 1, nLow, nMiddle, nHigh])
            else:
                writer.writerow([gtwID, key, nTrans, devTotalTrans[key], nTrans/devTotalTrans[key], nLow, nMiddle, nHigh])
        outf.close()        

def redundant_gtw(minPRR, gtwRecPath, devPath):
    devTotalTrans = {}
    f = open(devPath, 'r', encoding='GBK')
    reader = csv.reader(f)
    next(reader) # skip header
    for row in reader:
        if row.__len__() > 0 and int(row[1]) > 10 and float(row[3]) < 1:
            devID = row[0]
            devTotalTrans[devID] = int(row[1])
    f.close()

    fileList = os.listdir(gtwRecPath)
    gtw2devRec = {}
    gtw2devNum = {}
    for file in fileList:
        gtwID = file.replace(".csv", "")
        matchDev = set()

        f = open(gtwRecPath+file, 'r', encoding='GBK')
        reader = csv.reader(f)
        next(reader) # skip header
        for row in reader:
            devID = row[1]
            if devID in devTotalTrans and float(row[4]) > minPRR:
                matchDev.add(devID)
        f.close()
        gtw2devRec[gtwID] = matchDev
        gtw2devNum[gtwID] = matchDev.__len__()

    devs = set(devTotalTrans.keys())
    keepGtw = {}
    while len(devs) > 0 and len(gtw2devNum) > 0:
        sortedGtw = sorted(gtw2devNum.items(), key=lambda d:d[1], reverse=True)
        bestGtw = sortedGtw[0][0]
        saveDevs = gtw2devRec[bestGtw]
        gtw2devRec.pop(bestGtw)
        gtw2devNum.pop(bestGtw)
        keepGtw[bestGtw] = saveDevs
        for key in gtw2devRec:
            newDevs = gtw2devRec[key] # a set
            for item in saveDevs:
                newDevs.discard(item)
            gtw2devRec[key] = newDevs
            gtw2devNum[key] = len(newDevs)
        for item in saveDevs:
            devs.discard(item)
        print('Saved devices:' + str(len(saveDevs)) +'\t Remaining devs:'+ str(len(devs))+'\n')
    print(len(devs))




# if __name__ == "__main__":
#     # pkt_loss(homePath+'sort_by_service/ENNGas/', homePath+'ENN_Result/packet_loss.csv')
#     # trans_anal(homePath+'sort_by_service/ENNGas/', homePath+'ENN_Result/trans_analysis.csv')
#     # snr_anal_dev(homePath+'sort_by_service/ENNGas/', homePath+'ENN_Result/trans_snr.csv', homePath+'ENN_Result/dev_snr.csv')
#     # dev_gtw_all(homePath+'sort_by_service/ENNGas/', homePath+'ENN_Result/trans_gtw_all.csv', homePath+'ENN_Result/dev_gtw_all.csv')
#     # gtw_all(homePath+'sort_by_gtw/', homePath+'gtw_all.csv')
#     # gtw2dev(homePath+'sort_by_gateway/', homePath+'ENN_Result/packet_loss.csv', homePath+'ENN_Result/GTW2DEV/')
#     redundant_gtw(0.8, homePath+'ENN_Result/GTW2DEV/', homePath+'ENN_Result/packet_loss.csv')

