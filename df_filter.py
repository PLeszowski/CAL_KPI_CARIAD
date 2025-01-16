"""
Module contains functions that filter data in dataframes and return the filtered dataframe
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
import const
import logging

logger = logging.getLogger(__name__)


def copy_rows__col_eq_val_df(dataframe, col_name, param_value):
    """
    :param dataframe: Dataframe
    :param col_name: convergence signal
    :param param_value: convergence signal value
    :return: df: Dataframe
    Function copies to df rows from data_frame where col_name = param_value
    Returns Dataframe df with selected rows
    """
    try:
        # df = dataframe.where(dataframe[calib_param] == param_value)  #  leaves not matched rows empty
        logger.debug(f'getting data where {col_name} = {param_value}')
        df = dataframe[dataframe[col_name] == param_value]
    except KeyError as e:
        logger.exception(e)
        raise e
    except ValueError as e:
        logger.exception(e)
        logger.warning(f'No data where {col_name} = {param_value}')
    else:
        return df


def copy_rows__bracket_df(dataframe, pitch_value, yaw_value, roll_value, suspension_value):
    """
    :param dataframe: Dataframe
    :param pitch_value: bracket pitch value
    :param yaw_value: bracket yaw value
    :param roll_value: bracket roll value
    :param suspension_value: bracket height value
    :return: df: Dataframe
    Function copies to df rows from dataframe where bracket
    pitch = pitch_value, bracket yaw = yaw_value, bracket roll = roll_value
    Returns Dataframe df with selected rows
    """
    try:
        logger.debug(f'getting bracket ({pitch_value}, {yaw_value}, {roll_value}, {suspension_value})')
        df = dataframe[(dataframe[const.DFROW_BR_PITCH] == pitch_value) &
                       (dataframe[const.DFROW_BR_YAW] == yaw_value) &
                       (dataframe[const.DFROW_BR_ROLL] == roll_value) &
                       (dataframe[const.DFROW_SUSPENSION] == suspension_value)]
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return df

def copy_rows__cariad_ds_df(dataframe, road, weather, daytime, suspension):
    """
    :param dataframe: Dataframe
    :param road: road type value
    :param weather: weather condition value
    :param daytime: daytime value
    :param suspension: camera height value
    :return: df: Dataframe
    Function copies to df rows from dataframe where bracket
    ROAD_TYPE = road, WEATHER = weather, DAYTIME = daytime, Suspension = suspension
    Returns Dataframe df with selected rows
    """
    try:
        logger.debug(f'getting df ({road}, {weather}, {daytime}, {suspension})')
        df = dataframe  # .copy()
        if road != const.VAL_ROAD_ANY:
            df = df[(dataframe[const.DFROW_ROAD] == road)]
        if weather != const.VAL_WEATHER_ANY:
            df = df[(dataframe[const.DFROW_WEATHER] == weather)]
        if daytime != const.VAL_DAYTIME_ANY:
            df = df[(dataframe[const.DFROW_DAYTIME] == daytime)]
        if suspension != const.VAL_SUSP_ANY:
            df = df[(dataframe[const.DFROW_SUSPENSION] == suspension)]
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        return df

# NOT USED
def copy_rows__index_boundary_val_df(dataframe):
    """
    :param dataframe: Dataframe
    :return: df: Dataframe
    Function searches data_frame for consecutive indexes with a difference of more than 1
    Copies to df rows where this difference occurs
    Includes first row, last row, and both rows where the difference occurs
    Returns Dataframe df with selected rows
    """
    try:
        col = list(dataframe.index.values)
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        prev_val = col[0]
        index_list = []

        logger.debug('copying rows where indexes have boundary values')
        for index, val in enumerate(col):

            if index == 0:
                index_list.append(val)  # first index
            elif val != prev_val + 1:
                index_list.append(prev_val)  # last consecutive index
                index_list.append(val)  # new index value

            prev_val = val

        index_list.append(prev_val)  # last index value
        df = dataframe.loc[index_list]  # .reset_index(drop=True)
        return df


# NOT USED
def copy_rows__col_boundary_val_df(dataframe, col_name, param_value1, param_value2):
    """
    :param dataframe: Dataframe
    :param col_name: Column to search
    :param param_value1: col_name value 1
    :param param_value2: col_name value 2
    :return: Dataframe
    Function searches data_frame column col_name for value changes between param_value1 and param_value2 and vice-versa
    Copies to df rows where this change occurs (includes rows before change and after change)
    Includes only rows where the difference occurs
    Returns Dataframe df with selected rows
    """

    try:
        dataframe.reset_index(drop=True, inplace=True)
        col = list(dataframe[col_name])
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        prev_index = 0
        index_list = []
        logger.debug(
            f'copying rows pairs where column {col_name} value changes between {param_value1} and {param_value2}')
        for index, val in enumerate(col):
            if index > prev_index and (col[index] == param_value1 and col[prev_index] == param_value2) or (
                    col[index] == param_value2 and col[index] == param_value1):
                index_list.append(prev_index)  # new value
                index_list.append(index)  # new value
            prev_index = index
        df = dataframe.loc[index_list].reset_index(drop=True)
        return df


# NOT USED
def copy_rows__col_changed_val_to_new_val_df(dataframe, col_name, param_value1, param_value2):
    """
    :param dataframe:
    :param col_name:
    :param param_value1:
    :param param_value2:
    :return: Dataframe
    Function searches dataframe column col_name for value changes between param_value1 and param_value2 and vice-versa
    Copies to df row where the change to the new value occurs (includes only rows after change)
    Includes first row, and the rows where the changes to new value occurs
    Returns Dataframe df with selected rows
    """

    try:
        dataframe.reset_index(drop=True, inplace=True)
        col = list(dataframe[col_name])
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        prev_index = 0
        index_list = []
        logger.debug(f'copying rows where change in column {col_name} values {param_value1} and {param_value2} occurs')
        for index, val in enumerate(col):
            if index == prev_index:
                index_list.append(index)  # first value
            elif (col[index] == param_value1 and col[prev_index] == param_value2) or (
                    col[index] == param_value2 and col[prev_index] == param_value1):
                index_list.append(index)  # new value
            prev_index = index
        df = dataframe.loc[index_list].reset_index(drop=True)
        return df


# NOT USED
def copy_rows__col_changed_to_new_val_df(dataframe, col_name):
    """
    :param dataframe:
    :param col_name:
    :return: Dataframe
    Function searches dataframe column col_name for value changes
    Copies to df row where the change to the new value occurs (includes first row and only rows after change)
    Includes first row, and the rows where the changes to new value occurs
    Returns Dataframe df with selected rows
    """
    param_value = -1
    try:
        dataframe.reset_index(drop=True, inplace=True)
        col = list(dataframe[col_name])
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        prev_index = 0
        index_list = []
        for index, val in enumerate(col):
            if index == prev_index:
                index_list.append(index)
                param_value = col[index]
            elif col[index] != param_value:
                index_list.append(index)
                param_value = col[index]
            prev_index = index
        df = dataframe.loc[index_list]#.reset_index(drop=True)
        return df


def copy_rows__col_changed_val_df(dataframe, col_name, param_value, get_first_index=False):
    """
    :param dataframe:
    :param col_name:
    :param param_value
    :param get_first_index: includes the value of the first row in the dataframe
    :return: Dataframe
    Function searches data_frame column col_name for value changes between param_value
    Copies to df row where the change to the new value occurs
    Includes first row, and the rows where the changes to new value occurs
    Returns Dataframe df with selected rows
    """
    try:
        dataframe.reset_index(drop=True, inplace=True)
        col = list(dataframe[col_name])
    except KeyError as e:
        logger.exception(e)
        raise e
    else:
        prev_index = 0
        index_list = []
        for index, val in enumerate(col):
            if index == prev_index and get_first_index is not False:
                index_list.append(index)
            elif (col[index] == param_value and col[prev_index] != param_value) or (
                    col[index] != param_value and col[prev_index] == param_value):
                index_list.append(index)
            prev_index = index
        df = dataframe.loc[index_list].reset_index(drop=True)
        return df


def get_indexes_of_first_occurrence(whole_series, values_series):
    """
    :param whole_series: dataframe row to search in
    :param values_series: dataframe row with values to search for
    :return: indexes: list of indexes of the first occurrences of each value in values_series
    Function searches whole_series for the first occurrences of each value in values_series
    Returns a list of indexes of the first occurrences
    """
    search_list = list(whole_series)
    search_values = list(values_series)
    indexes = []
    for value in search_values:
        for idx, search_list_val in enumerate(search_list):
            if search_list_val == value:
                indexes.append(idx)
                break
    return indexes
