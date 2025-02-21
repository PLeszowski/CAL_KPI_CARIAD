"""
Module stores constant values
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
# PROJECT DATA
ADCAM = 0
CARIAD = 1
PROJECT_CONFIG = CARIAD

# RESIM SW ID
SW_PACKAGE_NUM = 'E130'

# Excel sheet with GT data
# GT_DATA_FILE = 'GT_Data.xls'
GT_DATA_FILE = 'GT_Data.xls'
GT_DATA_FOLDER = 'F:/CARIAD/CAL/KPI_DS_INFO/'
# Prelable dictionary
PRELABEL_DICT_FOLDER = 'F:/CARIAD/CAL/KPI_DS_INFO/'
PRELABEL_DICT_FILE = 'prelable_dict.json'
# Reprocessed/extracted pickles made from mdf splits
LOG_PICKLE_FOLDER = f'F:/CARIAD/CAL/REPRO/{SW_PACKAGE_NUM}/pickles/'
LOG_PICKLE_EXT = 'xz'
# CAL dictionary pickles (CAL data from reprocessed/extracted pickles)
CAL_DICT_PICKLE_FOLDER = f'F:/CARIAD/CAL/REPRO/{SW_PACKAGE_NUM}/pickle_dict/'
CAL_PKL_DICT_EXT = 'pickle'
# Output excel test report
TEST_REPORT_FOLDER = f'F:/CARIAD/CAL/REPRO/{SW_PACKAGE_NUM}/test_report/'
TEST_REPORT_FILE_NAME = 'CAL_KPI_REPORT'  # date and time added to name in main
TEST_REPORT_FILE_EXT = 'xls'  # without dot
# Partial results folder (where partial results json will be saved)
PARTIAL_RESULTS_FOLDER = f'F:/CARIAD/CAL/REPRO/{SW_PACKAGE_NUM}/test_report/partial_results/'
# Measurements folder (folder where splits joined into measurements are held)
MEASUREMENT_DFS_FOLDER = f'F:/CARIAD/CAL/REPRO/{SW_PACKAGE_NUM}/measurements/'

# Constants
FUNCTIONALITY = 'CAL'
SPI_CYCLE_TIME = 0.110  # sec
DFROW_GT_ID = 'GT_ID'  # Signal name that holds the GT ID in log
DFROW_LOG_FILE = 'LogName'
DFROW_TIMESTAMP = 'timestamp'
DFROW_TIME_TO_CONV = 'Time to Converge'
DFROW_DIST_TO_CONV = 'Distance to Converge'
DFROW_DISTANCE = 'distance'
DFROW_OOR_DISTANCE = 'OOR Distance'
DFROW_OOR_TIME = 'OOR Time'

VAL_C2W_STATE_CALIBRATED = 64273  # in range
VAL_C2W_STATE_UNVALIDATED = 43923  # calibrating
VAL_C2W_STATE_SUSPECTED = 48290  # suspected
VAL_C2W_STATE_OOR = 2422  # out of range
VAL_C2W_STATE_NOT_READY = 0  # invaid

VAL_DEGRADE_CAUSE_HEIGHT = 1
VAL_DEGRADE_CAUSE_YAW = 2
VAL_DEGRADE_CAUSE_PITCH = 4
VAL_DEGRADE_CAUSE_ROLL = 8

SIG_CLB_C2W_STATE = 'CO_main_safetyState'
SIG_CLB_C2W_STATE_DEGRADE_CAUSE = 'CO_main_safetyState_OOR_or_sus_cause'
SIG_CLB_C2W_PITCH = 'CO_main2road_euler_pitch'
SIG_CLB_C2W_YAW = 'CO_main2road_euler_yaw'
SIG_CLB_C2W_ROLL = 'CO_main2road_euler_roll'
SIG_CLB_C2W_CAM_HEIGHT = 'CO_main_camH'
SIG_VEHICLE_SPEED = 'CO_Vehicle_Speed'
SIG_CRC_COUNTER = 'COM_EyeQ_Frame_ID'

DFROW_VEHICLE = 'Vehicle'
DFROW_BR_PITCH = 'Bracket Pitch'
DFROW_BR_YAW = 'Bracket Yaw'
DFROW_BR_ROLL = 'Bracket Roll'
DFROW_GT_PITCH = 'GT Pitch'
DFROW_GT_YAW = 'GT Yaw'
DFROW_GT_ROLL = 'GT Roll'
DFROW_GT_HEIGHT = 'GT Height'
DFROW_GT_PITCH_DEG = 'GT Pitch deg'
DFROW_GT_YAW_DEG = 'GT Yaw deg'
DFROW_GT_ROLL_DEG = 'GT Roll deg'
DFROW_D_PITCH = 'Delta Pitch'
DFROW_D_YAW = 'Delta Yaw'
DFROW_D_ROLL = 'Delta Roll'
DFROW_D_HEIGHT = 'Delta Height'
DFROW_PITCH_DEG = 'Pitch deg'
DFROW_YAW_DEG = 'Yaw deg'
DFROW_ROLL_DEG = 'Roll deg'
DFROW_SUSPENSION = 'Suspension'
VAL_SUSP_ANY = 'any'
VAL_SUSP_DEFAULT = 'default'
VAL_SUSP_LOW = 'low'
VAL_SUSP_HIGH = 'high'
VAL_SUSP_VARYING = 'varying'
DFROW_ROAD = 'ROAD_TYPE'
VAL_ROAD_ANY = 'any'
VAL_ROAD_HIGHWAY = 'highway'
VAL_ROAD_RURAL = 'rural'
VAL_ROAD_CITY = 'city'
DFROW_WEATHER = 'WEATHER'
VAL_WEATHER_ANY = 'any'
VAL_WEATHER_CLEAR = 'clear'
VAL_WEATHER_RAIN = 'rain'
VAL_WEATHER_SNOW = 'snow'
VAL_WEATHER_FOG = 'fog'
DFROW_DAYTIME = 'DAYTIME'
VAL_DAYTIME_ANY = 'any'
VAL_DAYTIME_DAY = 'day'
VAL_DAYTIME_NIGHT = 'night'
VAL_DAYTIME_DUSK = 'dusk'
DFROW_VEHICLE_SPEED = 'veh_speed'
DFROW_VEHICLE_SPEED_V = 'veh_speed_valid'
VAL_VALID = 1
VAL_INVALID = 0
# GT_ROLL_GROUND_RAD = 0.00635  # ground slope correction: 1.2cm difference on either side of car (car width 1890cm)

# required columns for 'Accuracy' sheet
ACCURACY_INFO = [DFROW_LOG_FILE, DFROW_GT_ID,
                 DFROW_BR_PITCH, DFROW_BR_YAW, DFROW_BR_ROLL, DFROW_SUSPENSION,
                 DFROW_ROAD, DFROW_WEATHER, DFROW_DAYTIME, SIG_CLB_C2W_STATE,
                 DFROW_GT_PITCH, SIG_CLB_C2W_PITCH, DFROW_PITCH_DEG, DFROW_D_PITCH,
                 DFROW_GT_YAW, SIG_CLB_C2W_YAW, DFROW_YAW_DEG, DFROW_D_YAW,
                 DFROW_GT_ROLL, SIG_CLB_C2W_ROLL, DFROW_ROLL_DEG, DFROW_D_ROLL,
                 DFROW_GT_HEIGHT, SIG_CLB_C2W_CAM_HEIGHT, DFROW_D_HEIGHT, SIG_CLB_C2W_STATE_DEGRADE_CAUSE]

# signal parameters for accuracy
S_PARAMS_ACC = {DFROW_D_YAW: 'Yaw ', DFROW_D_PITCH: 'Pitch ', DFROW_D_ROLL: 'Roll ', DFROW_D_HEIGHT: 'Height '}

# required columns for 'Time to AF' and 'Distance to AF' sheets
TIME_DIST_INFO = [DFROW_LOG_FILE, DFROW_GT_ID, DFROW_BR_PITCH, DFROW_BR_YAW, DFROW_BR_ROLL, DFROW_SUSPENSION,
                  DFROW_ROAD, DFROW_WEATHER, DFROW_DAYTIME, SIG_CLB_C2W_STATE, SIG_CLB_C2W_STATE_DEGRADE_CAUSE, DFROW_TIMESTAMP, DFROW_DISTANCE]

# required columns for 'c2w_not_converged' in DFS excel
C2W_NOT_CALIBRATED_INFO = [DFROW_LOG_FILE, DFROW_GT_ID, DFROW_BR_PITCH, DFROW_BR_YAW, DFROW_BR_ROLL, DFROW_SUSPENSION,
                           DFROW_ROAD, DFROW_WEATHER, DFROW_DAYTIME, SIG_CLB_C2W_STATE, DFROW_DISTANCE, SIG_CLB_C2W_STATE_DEGRADE_CAUSE]

# required columns for 'Velocity Distribution' sheet
VELOCITY_INFO = [DFROW_LOG_FILE, DFROW_GT_ID, DFROW_BR_PITCH, DFROW_BR_YAW, DFROW_BR_ROLL, DFROW_SUSPENSION, DFROW_ROAD,
                 DFROW_WEATHER, DFROW_DAYTIME, DFROW_VEHICLE_SPEED]

# signal parameters for velocity distribution
S_PARAMS_VELOCITY = {DFROW_VEHICLE_SPEED: 'Velocity'}

# percentile parameters for time and distance
P_PARAMS_TD = [DFROW_TIME_TO_CONV, DFROW_DIST_TO_CONV]

# signal parameters for run mode time and distance AF (Vision)
S_PARAMS_RUN_MODE_DIST_AF = {SIG_CLB_C2W_STATE: 'AF '}

# required columns for 'Calibrated State Ratio' sheets
CALIB_STATE_RATIO_INFO = [DFROW_GT_ID, DFROW_LOG_FILE, DFROW_BR_PITCH, DFROW_BR_YAW, DFROW_BR_ROLL, DFROW_SUSPENSION, DFROW_ROAD,
                          DFROW_WEATHER, DFROW_DAYTIME, SIG_CLB_C2W_STATE_DEGRADE_CAUSE]

# required columns for 'Degrade Cause Ratio' sheets
DEGRADE_CAUSE_RATIO_INFO = [DFROW_GT_ID, DFROW_LOG_FILE, DFROW_VEHICLE, DFROW_BR_PITCH, DFROW_BR_YAW, DFROW_BR_ROLL, DFROW_SUSPENSION, DFROW_ROAD,
                            DFROW_WEATHER, DFROW_DAYTIME, SIG_CLB_C2W_STATE_DEGRADE_CAUSE]

# required columns for 'Not Converted' sheet
NOT_CONVERGED_INFO = [DFROW_LOG_FILE, DFROW_GT_ID, DFROW_BR_PITCH, DFROW_BR_YAW, DFROW_BR_ROLL, DFROW_SUSPENSION,
                      DFROW_ROAD, DFROW_WEATHER, DFROW_DAYTIME, SIG_CLB_C2W_STATE_DEGRADE_CAUSE]

# signal parameters for calibration state ratios (Pause Reason)
S_PARAMS_PR = {SIG_CLB_C2W_STATE: 'CLB_C2W_State ratios '}

# signal parameters for degradation cause ratios
S_PARAMS_DC = {SIG_CLB_C2W_STATE_DEGRADE_CAUSE: 'CO_main_safetyState_OOR_or_sus_cause ratios '}

# required columns for 'Switch Application' sheets
SWITCH_APP_INFO = [DFROW_LOG_FILE, DFROW_BR_PITCH, DFROW_BR_YAW, DFROW_BR_ROLL, DFROW_SUSPENSION, DFROW_ROAD,
                   DFROW_WEATHER, DFROW_DAYTIME, DFROW_TIME_TO_CONV, DFROW_DIST_TO_CONV]

# signal parameters for Switch Application
S_PARAMS_SA = {DFROW_TIME_TO_CONV: 'Time ', DFROW_DIST_TO_CONV: 'Distance '}

# signal parameters for OOR check
S_PARAMS_OOR = {DFROW_OOR_TIME: 'Time ', DFROW_OOR_DISTANCE: 'Distance '}

# required columns for 'OOR' sheet
OOR_INFO = [DFROW_LOG_FILE, DFROW_GT_ID, DFROW_BR_PITCH, DFROW_BR_YAW, DFROW_BR_ROLL, DFROW_SUSPENSION,
            DFROW_ROAD, DFROW_WEATHER, DFROW_DAYTIME,
            SIG_CLB_C2W_STATE,
            DFROW_GT_PITCH, SIG_CLB_C2W_PITCH, DFROW_D_PITCH,
            DFROW_GT_YAW, SIG_CLB_C2W_YAW, DFROW_D_YAW,
            DFROW_GT_ROLL, SIG_CLB_C2W_ROLL, DFROW_D_ROLL, DFROW_ROLL_DEG,
            DFROW_GT_HEIGHT, SIG_CLB_C2W_CAM_HEIGHT, DFROW_D_HEIGHT,
            DFROW_TIMESTAMP, DFROW_DISTANCE, SIG_CLB_C2W_STATE_DEGRADE_CAUSE]

# Configuration bracket and suspension (Pitch, Yaw, Roll, Suspension)
CAM_POS_CONFIG = (
    (0, 0, 0, VAL_SUSP_DEFAULT),
    (0, 0, 0, VAL_SUSP_HIGH),
    (0, 0, 0, VAL_SUSP_LOW),
    (0, 0, 0, VAL_SUSP_VARYING),
    (3.5, 0, 0, VAL_SUSP_DEFAULT),
    (2.5, 0, 0, VAL_SUSP_DEFAULT),
    (-2.5, 0, 0, VAL_SUSP_DEFAULT),
    (-3.5, 0, 0, VAL_SUSP_DEFAULT),
    (0, 3.5, 0, VAL_SUSP_DEFAULT),
    (0, 2.5, 0, VAL_SUSP_DEFAULT),
    (0, -2.5, 0, VAL_SUSP_DEFAULT),
    (0, -3.5, 0, VAL_SUSP_DEFAULT),
    (0, 0, 3.5, VAL_SUSP_DEFAULT),
    (0, 0, 2.5, VAL_SUSP_DEFAULT),
    (0, 0, -2.5, VAL_SUSP_DEFAULT),
    (0, 0, -3.5, VAL_SUSP_DEFAULT),
    (3.5, 3.5, 3.5, VAL_SUSP_DEFAULT),
    (-3.5, -3.5, -3.5, VAL_SUSP_DEFAULT),
    (4, 0, 0, VAL_SUSP_DEFAULT),
    (-4, 0, 0, VAL_SUSP_DEFAULT),
    (0, 4, 0, VAL_SUSP_DEFAULT),
    (0, -4, 0, VAL_SUSP_DEFAULT),
    (5, 0, 0, VAL_SUSP_DEFAULT),
    (-5, 0, 0, VAL_SUSP_DEFAULT)
)

CARIAD_CONFIG = (
    (VAL_ROAD_ANY, VAL_WEATHER_ANY, VAL_DAYTIME_ANY, VAL_SUSP_ANY),
    (VAL_ROAD_ANY, VAL_WEATHER_CLEAR, VAL_DAYTIME_DAY, VAL_SUSP_LOW),
    (VAL_ROAD_ANY, VAL_WEATHER_CLEAR, VAL_DAYTIME_NIGHT, VAL_SUSP_LOW),
    (VAL_ROAD_ANY, VAL_WEATHER_CLEAR, VAL_DAYTIME_DAY, VAL_SUSP_HIGH),
    (VAL_ROAD_ANY, VAL_WEATHER_CLEAR, VAL_DAYTIME_NIGHT, VAL_SUSP_HIGH),
    (VAL_ROAD_ANY, VAL_WEATHER_RAIN, VAL_DAYTIME_DAY, VAL_SUSP_LOW),
    (VAL_ROAD_ANY, VAL_WEATHER_RAIN, VAL_DAYTIME_NIGHT, VAL_SUSP_LOW),
    (VAL_ROAD_ANY, VAL_WEATHER_RAIN, VAL_DAYTIME_DAY, VAL_SUSP_HIGH),
    (VAL_ROAD_ANY, VAL_WEATHER_RAIN, VAL_DAYTIME_NIGHT, VAL_SUSP_HIGH)
)

GT_HW_POSE_INFO = [DFROW_GT_ID, DFROW_GT_PITCH, DFROW_GT_YAW, DFROW_GT_ROLL, DFROW_GT_HEIGHT, DFROW_GT_PITCH_DEG, DFROW_GT_YAW_DEG, DFROW_GT_ROLL_DEG]
GT_HW_DPARAM_TO_SIG = {DFROW_D_PITCH: SIG_CLB_C2W_PITCH,
                       DFROW_D_YAW: SIG_CLB_C2W_YAW,
                       DFROW_D_ROLL: SIG_CLB_C2W_ROLL,
                       DFROW_D_HEIGHT: SIG_CLB_C2W_CAM_HEIGHT
                       }
GT_HW_DPARAM_TO_PARAM = {DFROW_D_PITCH: DFROW_GT_PITCH,
                         DFROW_D_YAW: DFROW_GT_YAW,
                         DFROW_D_ROLL: DFROW_GT_ROLL,
                         DFROW_D_HEIGHT: DFROW_GT_HEIGHT
                         }
