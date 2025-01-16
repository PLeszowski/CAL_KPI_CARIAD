"""
Script calculates Average error, Maximum error, Maximum value, Minimum value, Average value
of pitch, yaw, roll and height from static calibration CAPL script output
Patryk Leszowski
APTIV
CARIAD
CALIBRATION
"""
import re
import numpy as np
import time
import datetime

canoe_write_output_file = r'F:\CARIAD\CAL\CTAC\X130\D130_CTAC_T_50x.txt'
pitch_list = []
yaw_list = []
roll_list = []
height_list = []
C_FRONT_MAIN_CAD_PITCH_ASIL = 1
C_FRONT_MAIN_CAD_ROLL_ASIL = 0
C_FRONT_MAIN_CAD_YAW_ASIL = 0
C_cal_ical_fc1_min_yaw = -2
C_cal_ical_fc1_max_yaw = 2
C_cal_ical_fc1_min_pitch = -2
C_cal_ical_fc1_max_pitch = 2
C_cal_ical_fc1_min_roll = -2
C_cal_ical_fc1_max_roll = 2
C_cal_ical_fc1_min_pos_z = 1414
C_cal_ical_fc1_max_pos_z = 1615

def get_max_error(value_list):
    # MAX ERROR
    # 1. Take average of all height values
    # 2. For each value take ABS(difference of value and average)
    # 3. Find MAX value
    max_error = 0
    abs_diff_list = []
    if value_list:
        values = np.array(value_list)
        values_ave = values.mean()
        value_min = np.min(values)
        value_max = np.max(values)
        for value in value_list:
            abs_diff = abs(value - values_ave)
            abs_diff_list.append(abs_diff)
            if abs_diff > max_error:
                max_error = abs_diff
        abs_diffs = np.array(abs_diff_list)
        average_error = abs_diffs.mean()
        values_range = abs(value_max - value_min)
        average_error_str = f'{average_error:.2f}'
        max_error_str = f'{max_error:.2f}'
        value_min_str = f'{value_min:.2f}'
        value_max_str = f'{value_max:.2f}'
        values_ave_str = f'{values_ave:.2f}'
        values_range_str = f'{values_range:.2f}'
        print(f'Average error = {average_error_str}')
        print(f'Maximum error = {max_error_str}')
        print(f'Minimum value = {value_min_str}')
        print(f'Maximum value = {value_max_str}')
        print(f'Average value = {values_ave_str}')
        print(f'Range = {values_range_str}')
        print('excel copy format')
        average_error_str = average_error_str.replace('.', ',')
        max_error_str = max_error_str.replace('.', ',')
        value_min_str = value_min_str.replace('.', ',')
        value_max_str = value_max_str.replace('.', ',')
        values_ave_str = values_ave_str.replace('.', ',')
        values_range_str = values_range_str.replace('.', ',')
        print(f'{average_error_str}')
        print(f'{max_error_str}')
        print(f'{value_min_str}')
        print(f'{value_max_str}')
        print(f'{values_ave_str}')
        print(f'{values_range_str}')
        print('-----------------------------------')
    else:
        print('max_error got empty list')

def main():
    with open(canoe_write_output_file, 'r') as f:
        line = f.readline()
        while line:
            search = re.search(r'.*pitch\s+=\s+(-?\d\.\d\d\d).*', line)
            if search:
                pitch = float(search.group(1))
                if C_FRONT_MAIN_CAD_PITCH_ASIL + C_cal_ical_fc1_min_pitch < pitch < C_FRONT_MAIN_CAD_PITCH_ASIL + C_cal_ical_fc1_max_pitch:
                    pitch_list.append(pitch)
                line = f.readline()
                continue
            search = re.search(r'.*yaw\s+=\s+(-?\d\.\d\d\d).*', line)
            if search:
                yaw = float(search.group(1))
                if C_FRONT_MAIN_CAD_YAW_ASIL + C_cal_ical_fc1_min_yaw < yaw < C_FRONT_MAIN_CAD_YAW_ASIL + C_cal_ical_fc1_max_yaw:
                    yaw_list.append(yaw)
                line = f.readline()
                continue
            search = re.search(r'.*roll\s+=\s+(-?\d\.\d\d\d).*', line)
            if search:
                roll = float(search.group(1))
                if C_FRONT_MAIN_CAD_ROLL_ASIL + C_cal_ical_fc1_min_roll < roll < C_FRONT_MAIN_CAD_ROLL_ASIL + C_cal_ical_fc1_max_roll:
                    roll_list.append(roll)
                line = f.readline()
                continue
            search = re.search(r'.*height\s+=\s+(\d\d\d\d).*', line)
            if search:
                height = int(search.group(1))
                if C_cal_ical_fc1_min_pos_z < height < C_cal_ical_fc1_max_pos_z:
                    height_list.append(height)
                line = f.readline()
                continue
            line = f.readline()

    print('PITCH')
    get_max_error(pitch_list)
    print('YAW')
    get_max_error(yaw_list)
    print('ROLL')
    get_max_error(roll_list)
    print('HEIGHT')
    get_max_error(height_list)

if __name__ == "__main__":
    _start_time = time.time()
    main()
    _script_time = time.time() - _start_time
    print('Total script time: ' + str(datetime.timedelta(seconds=_script_time)))


