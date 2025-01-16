"""
Module manages the transformation of dictionaries defined for the calibration function into dataframes
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import pandas as pd
from pandas import errors as pderr
import const
import xlrd
import logging
import math

logger = logging.getLogger(__name__)


def load_df(data_dict, gt_data_df):
    """
    Function calls functions that take the nested dictionary defined for the calibration function and transforms
    it into a dataframe
    :param data_dict: master dictionary
    :param gt_data_df: dataframe containing gt data
    :return: dataframe
    """
    try:
        df = dict_to_full_data_df(data_dict)
    except KeyError as e:
        raise e
    else:
        df = put_gt_data_into_master_df(df, gt_data_df)
        df = put_deltas_into_master_df(df)
        return df


def merge_df(master_df, slave_df):
    """
    Function copies columns from slave_df to master_df.
    :param master_df: master_df: Dataframe, master
    :param slave_df: Dataframe, slave
    :return: dataframe
    """
    # dataframe.shape[0] = number of rows in a dataframe

    # get columns of master and slave
    columns_master = list(master_df.columns.values)
    columns_slave = list(slave_df.columns.values)

    if columns_master and columns_slave:
        # if master has filled columns and slave has empty columns
        if master_df.shape[0] > 0 and slave_df.shape[0] == 0:
            # fill empty slave with nans
            try:
                slave_df = pd.DataFrame(index=master_df.index, columns=slave_df.columns)
            except Exception as e:
                logger.error('Error filling empty row slave with nans')
                raise e
            else:
                logger.debug('Filled empty slave')

        # if master has empty columns and slave has filled columns
        elif master_df.shape[0] == 0 and slave_df.shape[0] > 0:
            # fill empty master with nans
            try:
                master_df = pd.DataFrame(index=slave_df.index, columns=master_df.columns)
            except Exception as e:
                logger.error('Error filling empty row master with nans')
                raise e
            else:
                logger.debug('Filled empty master')

        # if master has filled columns and slave has one row filled columns and if prelabel df
        elif master_df.shape[0] > slave_df.shape[0] == 1 and const.DFROW_GT_ID in slave_df:
            # get number of rows of master
            df_size = master_df.shape[0]
            # fill slave with values from first row
            try:
                slave_df = slave_df.loc[slave_df.index.repeat(df_size)].reset_index(drop=True)
            except Exception as e:
                logger.error('Error filling slave with values from first row')
                raise e
            else:
                logger.debug('Filled one row slave')

        # if master has less row filled, both have timestamps: switch dfs
        if master_df.shape[0] < slave_df.shape[0]:
            master_df_temp = slave_df
            slave_df = master_df
            master_df = master_df_temp
            del master_df_temp

        # if slave has less row filled, both have timestamps
        if master_df.shape[0] > slave_df.shape[0]:
            # check if both have a timestamp
            if 'timestamp' in master_df.columns and 'timestamp' in slave_df.columns:
                # define temporary dataframe
                logger.debug('Merge unequal Dataframes by timestamp')
                # get first timestamp of master
                ts_1st_m = master_df.iloc[0, 0]
                # get first timestamp of slave
                ts_1st_s = slave_df.iloc[0, 0]
                # get last timestamp of master
                ts_last_m = master_df.iloc[-1, 0]
                # get last timestamp of slave
                ts_last_s = slave_df.iloc[-1, 0]
                # get delta between 1st timestamp in master and 1st timestamp slave
                delta_1st = abs(ts_1st_m - ts_1st_s)
                # get delta between last timestamp in master and last timestamp slave
                delta_last = abs(ts_last_m - ts_last_s)
                try:
                    cycle_time = const.SPI_CYCLE_TIME  # cycle_time + cycle_time*0.3  # add 30%
                    # both SPI and ETH transmit through the whole split
                    if delta_1st <= cycle_time >= delta_last:
                        df = pd.merge_asof(master_df.sort_values('timestamp'), slave_df, on='timestamp', direction='nearest')
                    # SPI starts later than ETH
                    elif delta_1st > cycle_time >= delta_last:
                        df = pd.merge_asof(master_df.sort_values('timestamp'), slave_df, on='timestamp', direction='backward')
                    # SPI ends before ETH
                    elif delta_1st <= cycle_time < delta_last:
                        df = pd.merge_asof(master_df.sort_values('timestamp'), slave_df, on='timestamp', direction='forward')
                    elif delta_1st < delta_last:
                        df = pd.merge_asof(master_df.sort_values('timestamp'), slave_df, on='timestamp', direction='forward')
                    elif delta_1st > delta_last:
                        df = pd.merge_asof(master_df.sort_values('timestamp'), slave_df, on='timestamp', direction='backward')
                    else:
                        logger.error("Error merging unequal Dataframes due to timestamp differences")
                        raise pderr.MergeError
                    return df
                except Exception as e:
                    logger.error('Error merging unequal Dataframes by timestamp')
                    raise e
            else:
                logger.error("Dataframe does not have column 'timestamp'")
                raise pderr.MergeError
        # if dataframes have same number of rows
        elif master_df.shape[0] == slave_df.shape[0]:
            logger.debug('Merge same row length dataframes')  # done below (if master_df.shape[0] == slave_df.shape[0]:)
        # else raise error
        else:
            logger.error('Unsupported dataframe shape')
            raise pderr.MergeError

        if master_df.shape[0] == slave_df.shape[0]:
            # declare a list to hold duplicate column names (ex. timestamp)
            columns_to_remove = set(columns_master).intersection(columns_slave)
            # copy the columns from slave to master removing duplicate columns from slave
            try:
                master_df = master_df.merge(slave_df.drop(columns_to_remove, axis=1), left_index=True, right_index=True)
            except Exception as e:
                logger.error('Error merging equal row dataframes')
                raise e
            else:
                logger.debug('Merged Dataframes')
                return master_df
        else:
            logger.error('Unable to merged. Undetermined dataframe shape')
            raise pderr.MergeError

    else:
        if not columns_master:
            logger.error('empty master dataframe')
        if not columns_slave:
            logger.error('empty slave dataframe')
        raise pderr.MergeError


def dict_to_full_data_df(master_dict):
    """
    :param master_dict:
    :return: dataframe
    Function merges dictionaries from log files into one dataframe (master dataframe)
    """
    try:
        # ----------------------------------------------------------------------------
        # Load dictionaries from master dictionary, put in dataframes, and merge the dataframes
        # ------------------------------------
        # Load dictionary 'veh_speed_dict' from master dictionary to dataframes
        veh_speed_dict = pd.DataFrame(master_dict['veh_speed_dict'])
        # Load dictionary 'cal_c2w_dict' from master dictionary to dataframe
        cal_c2w_df = pd.DataFrame(master_dict['cal_c2w_dict'])
        # Merge 'cal_c2w_df' dataframe into master 'full_data_df' dataframe
        full_data_df = merge_df(veh_speed_dict, cal_c2w_df)
        # Delete 'cal_c2w_df' reference
        del cal_c2w_df
        # ------------------------------------
        # Load dictionary 'common_counter_dict' from master dictionary to dataframe
        common_counter_df = pd.DataFrame(master_dict['common_counter_dict'])
        # Merge 'common_counter_df' dataframe into master 'full_data_df' dataframe
        full_data_df = merge_df(full_data_df, common_counter_df)
        # Delete 'common_counter_df' reference
        del common_counter_df
        # ------------------------------------
        # Load dictionary 'prelabel_dict' from master dictionary to dataframe
        prelabel_df = pd.DataFrame(master_dict['prelabel_dict'])
        # Merge 'prelabel_df' dataframe into master 'full_data_df' dataframe
        full_data_df = merge_df(full_data_df, prelabel_df)
        # Delete 'prelabel_df' reference
        del prelabel_df
        # ------------------------------------
        # Delete 'master_dict' reference
        del master_dict
        # ----------------------------------------------------------------------------
        logger.info('dict_to_full_data_df: Converted Dictionaries to Dataframe')
        return full_data_df

    except KeyError as e:
        logger.exception(e)
        pass
        # raise e

def get_gt_data():
    """
    Function reads an n*m excel sheet, first sheet in workbook. Creates nested dictionary where keys of main dictionary
    are A2 to An. Values of main dictionary are dictionaries where keys are B1 to m1, and
    values are B2 to nm. Example:
    {A2: {B1: B2, C1: C2, D1:  D2}, A3: {B1: B3, C1: C3, D1: D3}, A4: {B1: B4, C1: C4, D1: D4}}
    Returns dataframe (from converted dictionary)
    :return: dataframe
    """
    path = const.GT_DATA_FOLDER
    file = const.GT_DATA_FILE
    workbook = xlrd.open_workbook(path + file)
    sheet = workbook.sheet_by_index(0)
    gt_data_dict = {}
    col = 0

    for row in range(1, sheet.nrows):
        key = sheet.cell(row, col).value
        gt_event = {}
        for col in range(1, sheet.ncols):
            gt_event[sheet.cell(0, col).value] = sheet.cell(row, col).value
        gt_data_dict[key] = gt_event
        col = 0
    logger.info('get_gt_data: Got GT Data')
    return pd.DataFrame(gt_data_dict).T


def put_gt_data_into_master_df(full_data_df, gt_data_df):
    """
    :param full_data_df: dataframe
    :param gt_data_df: dataframe
    :return: df: dataframe
    Function merges gt_data_df rows with full_data_df, based on gt_data_df keys
    that match full_data_df GT_ID column values
    """
    df = full_data_df.merge(gt_data_df, how='left', left_on=const.DFROW_GT_ID, right_index=True)
    logger.info('put_gt_data_into_master_df: Put GT data into dataframe')
    return df


def put_deltas_into_master_df(df):
    """
    :param df: dataframe
    Function adds columns with delta value for pitch, yaw, roll, height returns the updated Dataframe, converted to degrees
    :return: df: dataframe
    """

    df_af = df[pd.notnull(df[const.SIG_CLB_C2W_STATE])].copy()
    if df_af.shape[0] > 0:
        df_af[const.DFROW_D_PITCH] = abs(df_af[const.SIG_CLB_C2W_PITCH] - df_af[const.DFROW_GT_PITCH]) / (math.pi / 180)
        df_af[const.DFROW_D_YAW] = abs(df_af[const.SIG_CLB_C2W_YAW] - df_af[const.DFROW_GT_YAW]) / (math.pi / 180)
        df_af[const.DFROW_D_ROLL] = abs(df_af[const.SIG_CLB_C2W_ROLL] - df_af[const.DFROW_GT_ROLL]) / (math.pi / 180)
        df_af[const.DFROW_D_HEIGHT] = (abs(df_af[const.SIG_CLB_C2W_CAM_HEIGHT] - df_af[const.DFROW_GT_HEIGHT]) / df_af[const.DFROW_GT_HEIGHT]) * 100

        df_af[const.DFROW_PITCH_DEG] = df_af[const.SIG_CLB_C2W_PITCH] / (math.pi / 180)
        df_af[const.DFROW_YAW_DEG] = df_af[const.SIG_CLB_C2W_YAW] / (math.pi / 180)
        df_af[const.DFROW_ROLL_DEG] = df_af[const.SIG_CLB_C2W_ROLL] / (math.pi / 180)

        df_af[const.DFROW_GT_PITCH_DEG] = df_af[const.DFROW_GT_PITCH] / (math.pi / 180)
        df_af[const.DFROW_GT_YAW_DEG] = df_af[const.DFROW_GT_YAW] / (math.pi / 180)
        df_af[const.DFROW_GT_ROLL_DEG] = df_af[const.DFROW_GT_ROLL] / (math.pi / 180)

        df_af = df_af.astype({const.DFROW_D_PITCH: float}).round(3)
        df_af = df_af.astype({const.DFROW_D_YAW: float}).round(3)
        df_af = df_af.astype({const.DFROW_D_ROLL: float}).round(3)
        df_af = df_af.astype({const.DFROW_D_HEIGHT: float}).round(3)
        df_af = df_af.astype({const.DFROW_PITCH_DEG: float}).round(3)
        df_af = df_af.astype({const.DFROW_YAW_DEG: float}).round(3)
        df_af = df_af.astype({const.DFROW_ROLL_DEG: float}).round(3)
        df_af = df_af.astype({const.DFROW_GT_PITCH: float}).round(3)
        df_af = df_af.astype({const.DFROW_GT_YAW: float}).round(3)
        df_af = df_af.astype({const.DFROW_GT_ROLL: float}).round(6)
    return df_af
