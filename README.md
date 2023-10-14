This repository provides the dataset and corresponding data analysis scripts for a measurement study in a large-scale LoRa network. LoRa, as a representative Low-Power Wide-Area Network (LPWAN) technology, holds tremendous potential for various Internet of Things (IoT) applications. However, as there are few real large-scale deployments, it is unclear whether and how well LoRa can eventually meet its prospects. In this paper, we demystify the real performance of LoRa by deploying LoRa systems in both campus-scale testbeds and citywide applications. We measure a large-scale LoRa network consisting of 100 gateways and 19,821 LoRa end nodes, covering an area of 130 km$^2$ for 12 applications. Our measurement aims to provide insights for large-scale LoRa network deployment and also for future academic research to fully unleash the potential of LoRa. 



# Related Publications

Shuai Tong, Jiliang Wang, Yunhao Liu, Jun Zhang. 2023. Citywide LoRa Network Deployment and Operation: Measurements, Analysis, and Implications. In the 21st ACM Conference on Embedded Networked Sensor Systems (ACM SenSys '23)


# Data Format

We collect information of both uplinks and downlinks at the network server, which gathers transmission records at all LoRa gateways. 

The collected dataset are public available at: [https://tianchi.aliyun.com/notebook-ai/myDataSet?spm=a2c22.12281897.0.0.6b5423b7cO4EYX#datasetLabId=163450](https://tianchi.aliyun.com/dataset/163450)

**The data format of uplink transmissions is as follows:**


deveui|gateway|rssi|snr|fcnt|repeat|sf|frequency|time
----|----|----|----|----|----|----|----|----
3f53012a0000383c|b827ebfffe3e30af|-100.000000000|0.750000000|98765|0|7|475900000|2023-02-02 19:36:13.784
3f53012a0000383c|b827ebfffe81c189|-103.000000000|3.500000000|98765|1|7|475900000|2023-02-02 19:36:13.787
3f53012a0000383c|b827ebfffe86ef7d|-90.000000000|-5.500000000|98765|1|7|475900000|2023-02-02 19:36:13.790
3f53012a0000383c|b827ebfffe55a8a3|-106.000000000|-4.000000000|98765|1|7|475900000|2023-02-02 19:36:13.792

**Description of each item in the dataset is as follows:**

item|description
----|----
deveui|Serial number of the end device which generate the uplink packet
gateway|Serial number the gateway which receives the uplink transmission
rssi|Received Signal Strength Indicator
snr|Signal to Noise Ratio. It is the ratio of LoRa signal power to that of all other electrical signals in the area.
fcnt|Frame counter of uplink packets. The frame counters for each end node should be a monotonically increasing sequence whose value indicates the number of uplink attempts, and numbers skipped in the sequence correspond to lost packets of that device
repeat|0 if the transmission record was selected at the network server; 1 if the transmission record was identified as redundant
sf|Spreading factor of the corresponding uplink transmission
frequency|Carrier frequency of the uplink transmission
time|Time of packet reception, recorded at gateways

We collect downlink records also at LoRa network servers. When the server schedules to start a downlink transmission, it generates the packet and select a suitable gateway for process the downlink transmission.

**The data format of downlink transmissions is as follows:**


ts|devaddr|frequency|sf|messagelength|delay|power|gateway
----|----|----|----|----|----|----|----
"2023-02-02 20:58:54.256"|"null"|"505500000"|11|34|"5s"|"17"|"b827ebfffe888b76"
"2023-02-02 23:38:43.660"|"008153BC"|"505100000"|10|84|"4s"|"17"|"b827ebfffe888b76"
"2023-02-02 23:38:50.663"|"008153BC"|"506500000"|10|24|"4s"|"17"|"b827ebfffe888b76"
"2023-02-02 23:42:01.724"|"008153BC"|"505900000"|10|84|"4s"|"17"|"b827ebfffe888b76"
"2023-02-02 23:42:07.717"|"008153BC"|"505500000"|10|84|"4s"|"17"|"b827ebfffe888b76"


**Description of each item in the dataset is as follows:**

item|description
----|----
ts|Time stamp for downlink transmissions
devaddr|Serial number of the target end device for the downlink transmission
frequency|Carrier frequency of the uplink transmission
sf|Spreading factor of the corresponding uplink transmission
time|Time of packet reception, recorded at gateways
messagelength| length of the downlink message
delay| The maximum gateway delivery delay that the network server can allow

# Scripts Introduction

### Functionalities of primary files 
#### class_record.py
This file implement a class used to maintain a transmission record as

```python
def __init__(self, gateway, device, rssi, snr, fcnt, repeat, sf, freq, time, time_stamp, company_id, gas_number, message_len, message_type, type_service):
```

It also define a class for batch record process as

```python
def __init__(self):
        self._records = []
        self._transmissions = []
        self._trans_number = -1
```

The class of batch records provide functions such as acounting transmission numbers based on frame counters (i.e., fcnt), removing records by gateways, estimating packet loss rates, etc.


#### file_process.py

This script processes files of datasets, including reading file header, formatting data items, and writing out analyzing results.

This file describes the following functions:

1. row_to_record: transforming a row to a record object.

```python
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
```

2. read_records: read all packet transfer records in a file.

```python
def read_records(in_file_path):
```

3. sort_by_dev/sort_by_date/sort_by_hour/sort_by_frequency: these functions sort records based on their corresponding end nodes, transmission dates, transmission hours, and carrier frequencies.

```python
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
```

```python
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
```

```python
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
```

#### data_analysis.py

This file provides function for analyzing network coverage and gateway utilization. 

An **EXAMPLE** to use this script for estimating packet loss is as 

```python
for file in os.listdir(device_path): 
    device_records = RecordBatch()
    with open(device_path + file, 'r', encoding='GBK') as f:
        reader = csv.reader(f)
        param_index = file_process.read_header(next(reader))
        for row in reader:
            record = file_process.row_to_record(row, param_index)
            device_records.append(record)

    number_transmissions = device_records.trans_number()
    packet_loss, fcnt_reset, fcnt_repeat, max_fcnt = device_records.number_packet_loss()
    file_writer.writerow([len(device_records), number_transmissions, packet_loss, fcnt_reset, fcnt_repeat, max_fcnt])
```

This script also provide functions for detecting abnormal end devices, which does not operate regularly in the estimation period.

 ```python
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
if number_not_repeat != number_transmissions:
    file_writer.writerow([file.replace('.csv',''), str(number_not_repeat), str(number_transmissions), str(len(device_records))])
```

*NOTE: All the scripts run on Python 3.11.0.*
