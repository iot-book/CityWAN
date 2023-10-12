import os
import csv
import copy
from progress.bar import Bar
from collections import Counter
import file_process
import class_record
from class_record import RecordBatch

# home_path = './dataset/'
home_path = 'F:/Home/'

def device_wo_gateway(device_path, out_path, threshold = 0.1, cal_loss_rate = False):
    """How many end devices are lost without some specific gateway?

    This function reads raw records and writes records of diffirent
    end devices and services to isolated files.

    Args:
        in_file_path: Path of original recodes files.
        out_file_path: Path for write out isolated files for each device

    Returns:
        None

    Raises:
        None
    """
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    # List of gateway ids
    gateway_id_list = []
    with open(home_path + 'device_gateway/gtw_all.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            gateway_id_list.append(row[0])

    # Write out transmission loss rate
    if cal_loss_rate or not os.path.exists(out_path + 'trans_loss_rate.csv'):
        with open(out_path + 'trans_loss_rate.csv', 'w', newline="") as outf:
            file_writer = csv.writer(outf, dialect='excel')
            file_writer.writerow([''] + gateway_id_list)
            bar = Bar('Processing', max = len(os.listdir(device_path)))
            for file in os.listdir(device_path):
                bar.next()    
                device_records = RecordBatch()
                # read records
                with open(device_path + file, 'r', encoding='GBK') as f:
                    reader = csv.reader(f)
                    param_index = file_process.read_header(next(reader))
                    for row in reader:
                        record = file_process.row_to_record(row, param_index)
                        device_records.append(record)
                # Original: Number of transmissions
                trans_number_all = device_records.trans_number()
                if trans_number_all == 0:
                    file_writer.writerow([file.replace(".csv", "")] + [0]*len(gateway_id_list))
                    continue

                # Traversing gateways, how many percent of the node loses traffic after excluding a gateway
                loss_rate = []
                for gateway_id in gateway_id_list:
                    trans_number = device_records.trans_wo_gateway(gateway_id)
                    loss_rate.append((trans_number_all - trans_number) / trans_number_all)
                    # loss_rate.append(str(trans_number_all) + '-' + str(trans_number))
                file_writer.writerow([file.replace(".csv", "")] + loss_rate)
    
    # Number of disconnected devices without a gateway
    with open(out_path + 'trans_loss_rate.csv', 'rt') as csvfile:
        with open(out_path + 'lost_devices.csv', 'w', newline="") as outf:
            file_writer = csv.writer(outf, dialect='excel')
            file_writer.writerow(['gateway', 'lost devices'])
            reader = csv.reader(csvfile)
            # read loss rate --> number of lost devices
            rate_matrix = [row for row in reader]
            for i in range(1, len(rate_matrix[0])):
                gateway = rate_matrix[0][i]
                rates = [val[i] for val in rate_matrix[1:]]
                lost_devices = len([var for var in rates if float(var) > threshold])
                file_writer.writerow([gateway, str(lost_devices)])

def gateway_for_device(device_path, out_path, threshold = 0.9):
    """对每个device 要达到threshold的收包率 至少需要哪些网关组合

    This function looks for gateways to promise an end device having a certain PPR.

    Args:
        device_path: Path of records for each device.
        out_path: Path for write out required gateways for each device
        threshold: Minimal PRR requirement

    Returns:
        None

    Raises:
        None
    """
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    with open(out_path + 'selected-gateway_'+str(threshold)+'.csv', 'w', newline="") as outf:
        file_writer = csv.writer(outf, dialect='excel')
        file_writer.writerow(['Round', 'Selected Gateways', 'Satisfied Devices', 'New Gateways', 'Gateways'])

    # abnormal end devices
    abnormal_devices = []
    with open(device_path + '../abnormal.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            abnormal_devices.append(row[0])
    
    selected_gateways = []
    satisfied_devices = []
    search_round = 1
    while len(satisfied_devices) < len(os.listdir(device_path)) - len(abnormal_devices):
        max_gateway_number = -1 # Find the node with the largest base gateway coverage
        max_minimal_gateway_set = []
        bar = Bar('Gateway Search Round' + str(search_round), max = len(os.listdir(device_path)))
        for file in os.listdir(device_path):
            bar.next()    
            if file.replace('.csv', '') in abnormal_devices or file.replace('.csv', '') in satisfied_devices: # skip
                continue
            # Otherwise, the search continues for the basic gateway overlay set of the current node
            device_records = RecordBatch()
            with open(device_path + file, 'r', encoding='GBK') as f:
                reader = csv.reader(f)
                param_index = file_process.read_header(next(reader))
                for row in reader:
                    record = file_process.row_to_record(row, param_index)
                    device_records.append(record)
            # After excluding the selected gateway, check whether the node meets the threshold requirement
            trans_number_all = device_records.trans_number()
            trans_with_selected = device_records.trans_wo_gateway(selected_gateways, False)
            if trans_with_selected / trans_number_all >= threshold:
                satisfied_devices.append(file.replace('.csv', ''))
                continue
            # If the requirements are not met, continue to search for the basic gateway coverage set
            trans_pass_number = trans_with_selected
            device_records.remove_by_gateway(selected_gateways)
            minimal_gateway_set = []
            while trans_pass_number/trans_number_all < threshold and len(device_records) > 0:
                gateways = [val.gateway for val in device_records]
                best_gateway = max(gateways,key = gateways.count)
                trans_pass_number += device_records.trans_wo_gateway(best_gateway, False)
                device_records.remove_by_gateway(best_gateway)
                minimal_gateway_set.append(best_gateway)
            # Recording the number of gateways in the basic gateway coverage set of the current node
            if len(minimal_gateway_set) > max_gateway_number:
                max_gateway_number = len(minimal_gateway_set)
                max_minimal_gateway_set = minimal_gateway_set

        # After traversing all nodes, select the node whose basic gateway covers the largest coverage set 
        # and add all gateways to the selected gateway list
        selected_gateways += max_minimal_gateway_set
        search_round += 1
        print('\nSelected Gateways:' + str(len(selected_gateways)) +' (+' + str(max_gateway_number) + ')')
        print('Satisfied Devices:' + str(len(satisfied_devices)))
        with open(out_path + 'selected-gateway.csv', 'a', newline="") as outf:
            file_writer = csv.writer(outf, dialect='excel')
            file_writer.writerow([search_round-1, len(selected_gateways) - max_gateway_number, len(satisfied_devices), max_gateway_number, selected_gateways])
        # break

def abnormal_device(device_path, out_path):
    bar = Bar('Processing', max = len(os.listdir(device_path)))
    with open(out_path + 'Recs-Trans Unmatch.csv', 'w', newline="") as outf:
        file_writer = csv.writer(outf, dialect='excel')
        file_writer.writerow(['DevID', 'non-Red Recs', 'Total Trans', 'Total Recs'])
        for file in os.listdir(device_path):
            bar.next()    
            # Read records of each device
            device_records = RecordBatch()
            with open(device_path + file, 'r', encoding='GBK') as f:
                reader = csv.reader(f)
                param_index = file_process.read_header(next(reader))
                for row in reader:
                    record = file_process.row_to_record(row, param_index)
                    device_records.append(record)
            repeat = [val.repeat for val in device_records]
            number_not_repeat = repeat.count('0')
            number_transmissions = device_records.trans_number()
            # print('Not repeat:' + str(number_not_repeat) + ', Number Trans:' + str(number_transmissions))
            # if number_not_repeat < 10 or number_transmissions < 10:
            if number_not_repeat != number_transmissions:
                file_writer.writerow([file.replace('.csv',''), str(number_not_repeat), str(number_transmissions), str(len(device_records))])
        
def network_yield(device_path, out_path):
    """Collects statistics on the number of packets generated, packet loss rate, 
    and daily/maximum/minimum number of packets sent by each device

    This function counts the number of packets generated by each device.

    Args:
        device_path: Path of records for each device.
        out_path: Path for write out required gateways for each device

    Returns:
        None

    Raises:
        None
    """
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    bar = Bar('Processing', max = len(os.listdir(device_path)))
    with open(out_path + 'packet_loss.csv', 'w', newline='') as outf:
        file_writer = csv.writer(outf, dialect='excel')
        file_writer.writerow(['DevID', 'Total Recs', 'Total Trans', 'Packet Loss', 'fcnt Reset', 'reTrans', 'max fcnt', 'Trans per Day', 'Max Trans/day', 'Min Trans/day', 'Day of Trans', 'PLR', 'Avg Gtws'])
        for file in os.listdir(device_path):
            bar.next()    
            device_records = RecordBatch()
            with open(device_path + file, 'r', encoding='GBK') as f:
                reader = csv.reader(f)
                param_index = file_process.read_header(next(reader))
                for row in reader:
                    record = file_process.row_to_record(row, param_index)
                    device_records.append(record)
            # How many transmissions? How many packets are lost? How many fcnt resets? How many transfers per day (average, Max, min)?
            number_transmissions = device_records.trans_number()
            packet_loss, fcnt_reset, fcnt_repeat, max_fcnt = device_records.number_packet_loss()
            avg_trans, max_trans, min_trans, trans_days = device_records.trans_per_day()
            file_writer.writerow([file.replace('.csv', ''), len(device_records), number_transmissions, packet_loss, fcnt_reset, fcnt_repeat, max_fcnt, avg_trans, max_trans, min_trans, trans_days, packet_loss/(packet_loss+number_transmissions), len(device_records)/number_transmissions])


if __name__ == "__main__":
    # gateway_for_device(home_path + 'data_out/sort_by_service/ENNGas/', home_path + 'GtwSet/', threshold = 0.9)
    # abnormal_device(home_path + 'data_out/sort_by_service/ENNGas/', home_path + 'data_out/sort_by_service/')
    network_yield(home_path + 'DATA/ENNGas/', home_path + 'Overall/')
