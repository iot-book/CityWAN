from collections import Counter
from numpy import *

class Record:
    '''A class used to maintain a record, the constructor needs to initialize the fields in the record

    Initial
    Record = collections.namedtuple('Record', ['gateway', 'device', 'rssi', 'snr', 'fcnt',
                                           'repeat', 'sf', 'freq', 'time', 'time_stamp',
                                            'company_id', 'gas_number', 'message_len',
                                            'message_type', 'type_service'])
    '''
    def __init__(self, gateway, device, rssi, snr, fcnt, repeat, sf, freq, time, time_stamp,
                    company_id, gas_number, message_len, message_type, type_service):
        (self.gateway, self.device, self.rssi, self.snr, self.fcnt, self.repeat, self.sf, self.freq, self.time, 
            self.time_stamp, self.company_id, self.gas_number, self.message_len, self.message_type, self.type_service) \
        = (gateway, device, rssi, snr, fcnt, repeat, sf, freq, time, time_stamp,
            company_id, gas_number, message_len, message_type, type_service)
        self.trans_id = 0

class RecordBatch:
    """Summary of class here.

    Longer class information....
    Longer class information....

    Attributes:
        likes_spam: A boolean indicating if we like SPAM or not.
        eggs: An integer count of the eggs we have laid.
    """

    def __init__(self):
        self._records = []
        self._transmissions = []
        self._trans_number = -1

    def __len__(self):
        return len(self._records)

    def __getitem__(self, position):
        return self._records[position]

    def append(self, obj: Record):
        self._records.append(obj)

    def trans_number(self) -> int:
        '''After merging the records at the same time, the number of transfers 
        remaining in the current list is marked with the transfer sequence number of the records
        '''
        if self._trans_number == -1:
            if self.__len__() == 0:
                return 0
            sorted(self._records, key = lambda x:x.time_stamp)
            trans_count = 1
            previous_item_time = self._records[0].time_stamp
            self._records[0].trans_id = trans_count
            for item in self._records[1:]:
                if abs(item.time_stamp - previous_item_time) >= 1:
                    trans_count += 1
                item.trans_id = trans_count
                previous_item_time = item.time_stamp
            self._trans_number = trans_count
        return self._trans_number

    def remove_by_gateway(self, gateways):
        ''' Remove all records associated with gateways (a list) from the record list
        '''
        sorted(self._records, key = lambda x:x.time_stamp)
        i = 0
        while i >= 0 and i < len(self._records):
            if self._records[i].gateway in gateways:
                timestamp = self._records[i].time_stamp
                lo, hi = i-1, i
                while lo >= 0:
                    if abs(timestamp - self._records[lo].time_stamp) > 1:
                        lo += 1
                        break
                    lo = lo - 1
                while hi < len(self._records):
                    if abs(timestamp - self._records[hi].time_stamp) > 1:
                        break
                    hi = hi + 1
                del self._records[max(lo, 0):hi+1]
                i = max(lo-1, 0)
            else:
                i = i + 1

    def trans_wo_gateway(self, gateways, exclude = True) -> int:
        ''' How many transmissions are left if a gateway contained by [gateways] is not considered
        '''
        trans_count = 0
        first = True
        for item in self._records:
            if not exclude ^ (item.gateway in gateways): #异或
                continue
            if first:
                trans_count = 1
                previous_item_time = item.time_stamp
                first = False
            else:
                if abs(item.time_stamp - previous_item_time) >= 1:
                    trans_count += 1
                previous_item_time = item.time_stamp
        return trans_count

    def number_packet_loss(self) -> int:
        ''' Count the number of lost packets in the current record list according to the fcnt field
        '''
        packet_loss = 0 # 丢包数
        fcnt_reset = 0  # fcnt重置为0的次数
        fcnt_down = 0 # fcnt下降的次数（理论上应该不会出现）
        fcnt_repeat = 0 # fcnt重复（表示有重传）
        self.trans_number() # 为每条记录识别trans_id
        trans_prev = 0
        fcnt_list = []
        for item in self._records:
            assert item.trans_id >= trans_prev, 'ERROR: trans_id is a non-decreasing sequence'
            if item.trans_id == trans_prev:
                continue
            trans_prev = item.trans_id
            if item.fcnt == 0:                       # fcnt set to 0
                fcnt_reset += 1
            elif len(fcnt_list) > 0:
                if item.fcnt < fcnt_list[-1]:        # If the fcnt drops abnormally, it indicates that the FCNT is reset and the previous packets are lost
                    fcnt_reset += 1
                    packet_loss += item.fcnt
                elif item.fcnt == fcnt_list[-1]:     # fcnt unchanged
                    fcnt_repeat += 1
                else:                                # fcnt increased
                    packet_loss += item.fcnt -  fcnt_list[-1] - 1
            fcnt_list.append(item.fcnt)
        return packet_loss, fcnt_reset, fcnt_repeat, max(fcnt_list)     

    def trans_per_day(self):
        '''Calculates the daily transmission times of a single node and returns the average daily/maximum/minimum transmission times
        '''   
        self.trans_number() # Identify trans_id for each record
        trans_date = []   
        trans_prev = 0 
        for item in self._records:
            assert item.trans_id >= trans_prev, 'ERROR: trans_id is a non-decreasing sequence'
            if item.trans_id == trans_prev:
                continue
            trans_prev = item.trans_id
            trans_date.append(item.time.split()[0])
        trans_date = dict(Counter(trans_date))
        dict_values = [val for val in trans_date.values()]
        return mean(dict_values), max(dict_values), min(dict_values), len(trans_date.keys())
            
            
            

            
class BatchDevice:
    """This class organizes batch node objects with lists, 
    and each node object contains a list for organizing batch records.

    It is mainly used to classify records by node, 
    facilitating the batch of the same node records to the file

    Attributes:
        likes_spam: A boolean indicating if we like SPAM or not.
        eggs: An integer count of the eggs we have laid.
    """

    def __init__(self):
        self._records = []
        self._deveui_dict = {}

    def __len__(self):
        return len(self._records)

    def __getitem__(self, position):
        return self._records[position]

    def append(self, obj: Record):
        '''Add a new record to the corresponding node object according to the deveui field in the record
        '''
        if obj.device in self._deveui_dict.keys():
            self._records[self._deveui_dict[obj.device]].append(obj)
        else:
            self._deveui_dict[obj.device] = len(self._records)
            records = RecordBatch()
            records.append(obj)
            self._records.append(records)

    def get_device_records(self, deveui):
        if deveui not in self._deveui_dict.keys():
            return None
        return self._records[self._deveui_dict[deveui]]

    def get_all_deveui(self):
        return self._deveui_dict.keys()