"""
Module handles getting prelabels from master disks
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import os
import re
import serializer
import logging
import time as t
import const


class GetPrelabel:

    def __init__(self, args_dict):
        self.prelabel_dict_folder = args_dict['prelabel_dict_folder']
        self.prelabel_dict_file = args_dict['prelabel_dict']
        self.prelabel_dict = {}
        self.logger = logging.getLogger(__name__)

    def save_prelabel_dict(self):
        serializer.save_json(self.prelabel_dict, self.prelabel_dict_folder, self.prelabel_dict_file)

    def load_prelabel_dict(self):
        self.prelabel_dict = serializer.load_json(self.prelabel_dict_folder, self.prelabel_dict_file)

    def get_prelabel(self):

        while True:
            t.sleep(1)
            val = input('q - quit, any other key - next disc: ')
            if val == 'q':
                self.logger.info('QUIT')
                break
            else:
                fnames = []
                d_times = []
                for root, d_names, f_names in os.walk('F:/CARIAD/CAL/DATA_COLLECTION/D130/prelabel'):
                    for d_name in d_names:
                        print(f'Looking through folder {d_name}')
                        if re.match(r'\d{6}$', d_name):
                            d_times.append(int(d_name))
                    for f_name in f_names:
                        print(f'Looking through file {f_name}')
                        if re.match(r'.+\d{6}\.json$', f_name):
                            fnames.append(os.path.join(root, f_name))

                for index, fname in enumerate(fnames):
                    fname = re.sub(r'\\+', '/', fname)
                    fnames[index] = fname

                vin = ''
                date = ''
                time = ''
                gt_id = ''
                road_type = ''
                weather = ''
                daytime = ''
                gt_dict = {}
                for file in fnames:
                    print(F'Searching through file {file}')
                    search = re.search(r'.+prelabel_(\d{8})T(\d{6}).+', file)
                    # search_prelabel_in_wrong_path = re.search(r'.+prelabel_(\d{8})T(\d{6})', file)
                    if search:
                        self.logger.info(F'Found prelabel file {file}')
                        date = search.group(1)
                        time = search.group(2)
                        itime = int(time)
                        if itime not in d_times:
                            if itime-1 in d_times:
                                time = f'{itime - 1:06d}'
                            else:
                                self.logger.error(f'itime-1 not in d_times in file {file}')
                    # elif search_prelabel_in_wrong_path:
                    #     self.logger.warning(F'Prelabel file {file} in wrong path')
                    #     vin = 'WBA7H01000B267498'
                    #     date = search_prelabel_in_wrong_path.group(1)
                    #     time = search_prelabel_in_wrong_path.group(2)
                    #     itime = int(time)
                    #     if itime not in d_times:
                    #         if itime-1 in d_times:
                    #             time = f'{itime - 1:06d}'
                    #         else:
                    #             self.logger.error(f'itime-1 not in d_times in file {file}')
                    else:
                        self.logger.error(f'Cant find VIN, date, time in {file}')
                    with open(file, 'r') as f:
                        self.logger.info(F'Open file {file}')
                        line = f.readline()
                        while line:
                            s0 = re.search(r'.+"vin":"(\w{17})".+', line)
                            if s0:
                                vin = s0.group(1)
                            s1 = re.search(r'"property":\["(00\d\d)"\],"type":"State","typeName":"GT_ID"', line)
                            if s1:
                                gt_id = s1.group(1)
                            s2 = re.search(r'"property":\["(\w+)"\],"type":"State","typeName":"ROAD_TYPE"', line)
                            if s2:
                                road_type = s2.group(1).lower()
                            s3 = re.search(r'"property":\["(\w+)"\],"type":"State","typeName":"WEATHER"', line)
                            if s3:
                                weather = s3.group(1).lower()
                            s4 = re.search(r'"property":\["(\w+)"\],"type":"State","typeName":"DAYTIME"', line)
                            if s4:
                                daytime = s4.group(1).lower()
                            line = f.readline()
                        if not gt_id:
                            print('Cant find GT_ID')
                        if not road_type:
                            print('Cant find ROAD_TYPE')
                        if not weather:
                            print('Cant find WEATHER')
                        if not daytime:
                            print('Cant find DAYTIME')

                        measurement_id = f'ADCAM_{vin}_{date}_{time}'
                        if measurement_id not in gt_dict:
                            gt_dict[measurement_id] = {}
                        gt_dict[measurement_id] = {'GT_ID': [gt_id], 'ROAD_TYPE': [road_type], 'WEATHER': [weather],
                                                   'DAYTIME': [daytime]}
                self.merge_dict(gt_dict)
                self.save_prelabel_dict()

    def merge_dict(self, d1):
        if self.prelabel_dict:
            # Merge two dictionaries
            self.prelabel_dict = {**self.prelabel_dict, **d1}
            self.logger.info('Merged two dictionaries into one dictionary object')
        else:
            self.prelabel_dict = d1.copy()
            self.logger.info('Created dictionary object')


class MakePrelabel:

    def __init__(self, args_dict, measurement_ids, gt_id="0001", road_type="city", weather="clear", daytime="day"):
        self.prelabel_dict_folder = args_dict['prelabel_dict_folder']
        self.measurement_ids = measurement_ids
        self.prelabel_dict = {}
        self.gt_id = gt_id
        self.road_type = road_type
        self.weather = weather
        self.daytime = daytime
        self.logger = logging.getLogger(__name__)

    def make_prelabel(self):
        for measurement_id in self.measurement_ids:
            self.prelabel_dict[measurement_id] = {'GT_ID': [self.gt_id], 'ROAD_TYPE': [self.road_type],
                                                  'WEATHER': [self.weather], 'DAYTIME': [self.daytime]}
        serializer.save_json(self.prelabel_dict, self.prelabel_dict_folder, const.PRELABEL_DICT_FILE)
