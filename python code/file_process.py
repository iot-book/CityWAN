import csv
import collections
import time
import os
from datetime import datetime
import class_record
from progress.bar import Bar
import rich.progress
from class_record import *

home_path = 'F:/Home/'

ParaIndex = collections.namedtuple('ParaIndex', ['ts','deveui','messagetype','companyid','frequency',
    'sf','repeat','gasnumber','fcnt','rssi','snr','messagelength','typeservice','gateway', 'devaddr'])

def read_header(row_header) -> ParaIndex:
    """Read the header of a csv file.

    This function take a row of header as the input.
    It extracting the index of each parameter.

    Args:
        row_header: First row of header from a csv file.

    Returns:
        index of each parameter

    Raises:
        None
    """
    param_index = ParaIndex(*[val for val in range(15)]) 
    for i in range(0, len(row_header)):
        if row_header[i] == 'devEUI' or row_header[i] == 'deveui':
            param_index = param_index._replace(deveui = i)
        if row_header[i] == 'rssi':
            param_index = param_index._replace(rssi = i)
        if row_header[i] == 'snr':
            param_index = param_index._replace(snr = i)
        if row_header[i] == 'gateway':
            param_index = param_index._replace(gateway = i)
        if row_header[i] == 'frequency':
            param_index = param_index._replace(frequency = i)
        if row_header[i] == 'time' or row_header[i] == 'ts':
            param_index = param_index._replace(ts = i)
        if row_header[i] == 'fCnt' or row_header[i] == 'fcnt':
            param_index = param_index._replace(fcnt = i)
        if row_header[i] == 'repeat':
            param_index = param_index._replace(repeat = i)
        if row_header[i] == 'sf':
            param_index = param_index._replace(sf = i)
        if row_header[i] == 'companyId' or row_header[i] == 'companyid':
            param_index = param_index._replace(companyid = i)
        if row_header[i] == 'gasNumber' or row_header[i] == 'gasnumber':
            param_index = param_index._replace(gasnumber = i)
        if row_header[i] == 'messageLength' or row_header[i] == 'messagelength':
            param_index = param_index._replace(messagelength = i)
        if row_header[i] == 'messageType' or row_header[i] == 'messagetype':
            param_index = param_index._replace(messagetype = i)
        if row_header[i] == 'typeService' or row_header[i] == 'typeservice':
            param_index = param_index._replace(typeservice = i)
        if row_header[i] == 'devAddr' or row_header[i] == 'devaddr':
            param_index = param_index._replace(devaddr = i)
    return param_index

def row_to_record(row, param_index: ParaIndex) -> Record:
    """Transform a row to a record object.

    This function take a row of the csv file as the input.
    It extracting the record information based on the index of each parameter.

    Args:
        row: One row from the csv file.
        param_index: Index of each parameter from read_header()

    Returns:
        An Record object

    Raises:
        None
    """
    if "'" in row[param_index.deveui]:
        time_str = row[param_index.ts].replace("'", "")
        if "." in time_str:
            time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
        else:
            time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        time_stamp = time.mktime(time_obj.timetuple()) + (time_obj.microsecond / 1000000.0)
        if row[param_index.fcnt] == '' or row[param_index.fcnt] == 'NULL':
            fcnt_rec = -1
        else:
            fcnt_rec = int(row[param_index.fcnt])
        record = Record(row[param_index.gateway].replace("'", ""),
                        row[param_index.deveui].replace("'", ""),
                        row[param_index.rssi],
                        row[param_index.snr],
                        fcnt_rec,
                        row[param_index.repeat],
                        row[param_index.sf],
                        row[param_index.frequency].replace("'", ""),
                        row[param_index.ts].replace("'", ""),
                        time_stamp,
                        row[param_index.companyid].replace("'", ""),
                        row[param_index.gasnumber].replace("'", ""),
                        row[param_index.messagelength].replace("'", ""),
                        row[param_index.messagetype].replace("'", ""),
                        row[param_index.typeservice].replace("'", ""))
    else:
        time_str = row[param_index.ts].replace("T", " ").replace("+08:00","")
        if "." in time_str:
            time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
        else:
            time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        time_stamp = time.mktime(time_obj.timetuple()) + (time_obj.microsecond / 1000000.0)
        if row[param_index.fcnt] == '' or row[param_index.fcnt] == 'NULL':
            fcnt_rec = -1
        else:
            fcnt_rec = int(row[param_index.fcnt])
        record = Record(row[param_index.gateway],
                        row[param_index.deveui],
                        row[param_index.rssi],
                        row[param_index.snr],
                        fcnt_rec,
                        row[param_index.repeat],
                        row[param_index.sf],
                        row[param_index.frequency],
                        row[param_index.ts].replace("T", " ").replace("+08:00",""),
                        time_stamp,
                        row[param_index.companyid],
                        row[param_index.gasnumber],
                        row[param_index.messagelength],
                        row[param_index.messagetype],
                        row[param_index.typeservice])
    return record

def read_records(in_file_path):
    '''Read all packet transfer records in a file
    '''
    device_records = RecordBatch()
    with open(in_file_path, 'r', encoding='GBK') as f:
        reader = csv.reader(f)
        param_index = read_header(next(reader))
        for row in reader:
            record = row_to_record(row, param_index)
            device_records.append(record)
    return device_records

def sort_by_dev(in_file_path, out_file_path):
    """Sort records based on device id and service type.

    This function reads raw records and writes records of different
    end devices and services to isolated files.

    Args:
        in_file_path: Path of original recodes files.
        out_file_path: Path for write out isolated files for each device

    Returns:
        None

    Raises:
        None
    """
    if not os.path.exists(out_file_path):
        os.makedirs(out_file_path)

    for file in os.listdir(in_file_path):
        if 'iot_message_info' not in file: continue
        print(file)
        device_records = BatchDevice()
        # read records
        with rich.progress.open(in_file_path + file, 'r', encoding='GBK') as f:
            reader = csv.reader(f)
            param_index = read_header(next(reader))
            for row in reader:
                record = row_to_record(row, param_index)
                if record.device == '':
                    record.device = row[param_index.devaddr]
                if repeated_item(file, record.time):
                    continue          
                device_records.append(record)
        
        # append records to the 'dev file'
        for record_batch in device_records:
            sorted_records = sorted(record_batch, key = lambda x:x.time_stamp)
            deveui = sorted_records[0].device # At least one record
            # service = sorted_records[0].type_service
            # if not os.path.exists(out_file_path + service):
            #     os.makedirs(out_file_path + service)
            out_file = out_file_path + '/' + deveui + ".csv"
            if not os.path.exists(out_file): 
                with open(out_file, 'w', newline="") as outf:
                    file_writer = csv.writer(outf, dialect='excel')
                    file_writer.writerow(['deveui', 'gateway', 'rssi', 'snr', 'fcnt', 'repeat', 'sf', 
                            'frequency', 'time', 'timeStamp', 'companyid', 'gasnumber', 'messagelength', 'messagetype', 'typeservice'])
            with open(out_file, 'a', newline="") as outf:
                file_writer = csv.writer(outf, dialect='excel')
                for record in sorted_records:
                    file_writer.writerow([record.device, record.gateway, record.rssi, record.snr, 
                            record.fcnt, record.repeat, record.sf, record.freq, record.time,
                            record.time_stamp, record.company_id, record.gas_number, record.message_len,
                            record.message_type, record.type_service])

def trans_count(in_path, out_path):
    """Count the number of transfers for each file in the out_path directory

    Args:
        in_file_path: Path of original recodes files.
        out_file_path: Path for write out 

    Returns:
        None

    Raises:
        None
    """
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    with open(out_path + 'trans_per_day.csv', 'w', newline="") as outf:
        file_writer = csv.writer(outf, dialect='excel')
        file_writer.writerow(['date', 'trans', 'records', 'nodes']) 

    for file in os.listdir(in_path):
        print(file)
        # read records
        with rich.progress.open(in_path + file, 'r', encoding='GBK') as f:
            reader = csv.reader(f)
            param_index = read_header(next(reader))
            records_number = 0
            node_set = set()
            trans_number = 0
            for row in reader:
                record = row_to_record(row, param_index)
                records_number += 1
                node_set.add(record.device)
                if record.repeat == '0':
                    trans_number += 1
            nodes_number = len(node_set)
            with open(out_path + 'trans_per_day.csv', 'a', newline="") as outf:
                file_writer = csv.writer(outf, dialect='excel')
                file_writer.writerow([file.replace('.csv', ''), trans_number, records_number, nodes_number]) 


def sort_by_date(in_file_path, out_file_path):
    """Sort records based on their dates.

    This function reads raw records and writes records of different
    dates to isolated files.

    Args:
        in_file_path: Path of original recodes files.
        out_file_path: Path for write out isolated files for each date

    Returns:
        None

    Raises:
        None
    """
    if not os.path.exists(out_file_path):
        os.makedirs(out_file_path)

    for file in os.listdir(in_file_path):
        if 'iot_message_info' not in file: continue
        print(file)
        device_records = RecordBatch()
        # read records
        with rich.progress.open(in_file_path + file, 'r', encoding='GBK') as f:
            reader = csv.reader(f)
            param_index = read_header(next(reader))
            for row in reader:
                record = row_to_record(row, param_index) 
                if repeated_item(file, record.time):
                    continue
                device_records.append(record)

        # append records to the 'date file'
        for record in device_records:
            service = record.type_service
            if not os.path.exists(out_file_path + service):
                os.makedirs(out_file_path + service)
            out_file = out_file_path + service + '/' + record.time.split()[0] + ".csv"
            if not os.path.exists(out_file): 
                with open(out_file, 'w', newline="") as outf:
                    file_writer = csv.writer(outf, dialect='excel')
                    file_writer.writerow(['deveui', 'gateway', 'rssi', 'snr', 'fcnt', 'repeat', 'sf', 
                            'frequency', 'time', 'timeStamp', 'companyid', 'gasnumber', 'messagelength', 'messagetype', 'typeservice'])
            with open(out_file, 'a', newline="") as outf:
                file_writer = csv.writer(outf, dialect='excel')
                file_writer.writerow([record.device, record.gateway, record.rssi, record.snr, 
                        record.fcnt, record.repeat, record.sf, record.freq, record.time,
                        record.time_stamp, record.company_id, record.gas_number, record.message_len,
                        record.message_type, record.type_service])

def sort_by_hour(in_file_path, out_file_path):
    """The number of transmissions per hour in a day is counted
    Args:
        in_file_path: Path of original recodes files.
        out_file_path: Path for write out isolated files for each date

    Returns:
        None

    Raises:
        None
    """
    if not os.path.exists(out_file_path):
        os.makedirs(out_file_path)
    out_file = out_file_path + 'trans_per_hour.csv'
    with open(out_file, 'w', newline="") as outf:
        file_writer = csv.writer(outf, dialect='excel')
        file_writer.writerow(['Date', 'Hour', '# Transmissions', '# Records', '# Nodes'])

    for file in os.listdir(in_file_path):
        # if 'iot_message_info' not in file: continue
        print(file)

        records_dict = {key: 0 for key in range(0, 24)}
        trans_dict = {key: 0 for key in range(0, 24)}
        nodes_dict ={key: set() for key in range(0, 24)}
        # read records
        with rich.progress.open(in_file_path + file, 'r', encoding='GBK') as f:
            reader = csv.reader(f)
            param_index = read_header(next(reader))
            for row in reader:
                record = row_to_record(row, param_index)                            
                hour = int(record.time.split()[1].split(':')[0])
                records_dict[hour] += 1
                nodes_dict[hour].add(record.device)
                if record.repeat == '0':
                    trans_dict[hour] += 1
        with open(out_file, 'a', newline="") as outf:
            for h in range(0,24):
                file_writer = csv.writer(outf, dialect='excel')
                file_writer.writerow([file.split()[0], h, trans_dict[h], records_dict[h], len(nodes_dict[h])])

def sort_by_freq(in_file_path, out_file_path):
    """Count the number of transmissions per frequency in a day
    Args:
        in_file_path: Path of original recodes files.
        out_file_path: Path for write out isolated files for each date

    Returns:
        None

    Raises:
        None
    """
    if not os.path.exists(out_file_path):
        os.makedirs(out_file_path)
    out_file = out_file_path + 'trans_per_channel.csv'
    with open(out_file, 'w', newline="") as outf:
        file_writer = csv.writer(outf, dialect='excel')
        file_writer.writerow(['Date', 'Hour', '# Transmissions', '# Records', '# Nodes'])

    for file in os.listdir(in_file_path):
        # if 'iot_message_info' not in file: continue
        print(file)

        records_dict = dict()
        trans_dict = dict()
        nodes_dict = dict()
        # read records
        with rich.progress.open(in_file_path + file, 'r', encoding='GBK') as f:
            reader = csv.reader(f)
            param_index = read_header(next(reader))
            for row in reader:
                record = row_to_record(row, param_index)                            
                frequency = record.freq
                if frequency in records_dict.keys:
                    records_dict[frequency] += 1
                else:
                    records_dict[frequency] = 1
                if frequency in nodes_dict.keys:
                    nodes_dict[frequency].add(record.device)
                else:
                    nodes_dict[frequency] = {}
                if record.repeat == '0':
                    if frequency in trans_dict.keys:
                        trans_dict[frequency] += 1
                    else:
                        trans_dict[frequency] = 1
        with open(out_file, 'a', newline="") as outf:
            for freq in trans_dict.keys:
                file_writer = csv.writer(outf, dialect='excel')
                file_writer.writerow([file.split()[0], freq, trans_dict[freq], records_dict[freq], len(nodes_dict[freq])])

def time_range(in_file):
    print(in_file)
    # read records
    first_time, last_time = None, None
    with rich.progress.open(in_file, 'r', encoding='GBK') as f:
        reader = csv.reader(f)
        param_index = read_header(next(reader))
        for row in reader:
            record = row_to_record(row, param_index)
            if "." in record.time:
                curr_time = datetime.strptime(record.time, "%Y-%m-%d %H:%M:%S.%f")
            else:
                curr_time = datetime.strptime(record.time, "%Y-%m-%d %H:%M:%S")
            if first_time is None:
                first_time, last_time = curr_time, curr_time
                continue
            if first_time > curr_time:
                first_time = curr_time
            if last_time < curr_time:
                last_time = curr_time
    print('First Record: ' + str(first_time))
    print('Last Record: ' + str(last_time))

def repeated_item(file, time_str) -> bool:
    # 原始数据中不同文件存在部分时间段重叠
    if "." in time_str:
        time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
    else:
        time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

    if (('iot_message_up_2022-09-27' in file and 
        time_obj > datetime.strptime('2022-09-30 08:22:32', "%Y-%m-%d %H:%M:%S"))
        or ('iot_message_up_2022-09-30' in file and \
        time_obj > datetime.strptime('2022-10-03 08:32:00', "%Y-%m-%d %H:%M:%S"))
        or ('iot_message_up_2022-10-06' in file and \
        time_obj > datetime.strptime('2022-10-09 08:08:35', "%Y-%m-%d %H:%M:%S"))
        or ('iot_message_up_2022-10-09' in file and \
        time_obj > datetime.strptime('2022-10-12 08:11:37', "%Y-%m-%d %H:%M:%S"))
        or ('iot_message_up_2022-10-15' in file and \
        time_obj > datetime.strptime('2022-10-18 08:02:27', "%Y-%m-%d %H:%M:%S"))
        or ('iot_message_up_2022-10-18' in file and \
        time_obj > datetime.strptime('2022-10-18 11:36:16', "%Y-%m-%d %H:%M:%S"))
        or ('iot_message_up_2022-10-19' in file and \
        time_obj > datetime.strptime('2022-10-21 16:01:05', "%Y-%m-%d %H:%M:%S"))
        or ('iot_message_up_2022-10-21' in file and \
        time_obj > datetime.strptime('2022-11-04 08:01:36', "%Y-%m-%d %H:%M:%S"))
        or ('iot_message_up_2022-11-07' in file and \
        time_obj > datetime.strptime('2022-11-10 09:44:07', "%Y-%m-%d %H:%M:%S"))):
            return True
    else:
        return False   

if __name__ == "__main__":
    sort_by_dev(home_path + 'dataset/', home_path + 'DATA_UP/')
    # sort_by_date(home_path + 'dataset/', home_path + 'DATA_TIME/')
    # sort_by_hour(home_path + 'DATA_TIME/ENNGas/', home_path + 'Overall/')
    # sort_by_freq(home_path + 'DATA_TIME/ENNGas/', home_path + 'Overall/')
    # trans_count(home_path + 'DATA_TIME/ENNGas/', home_path + 'Overall/')

    # time_range(home_path + 'dataset/iot_message_info_2022-10-21.csv')
    # for file in os.listdir(home_path + 'dataset/'):
    #     if 'iot_message_info' not in file: continue
    #     # print(file)
    #     time_range(home_path + 'dataset/' + file)