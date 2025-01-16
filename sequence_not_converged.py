"""
Module manages the generation of the Errors sheet in kpi report
Patryk Leszowski
APTIV
ADCAM MID
CALIBRATION
"""
from excel_printer import ExcelPrinter
import const
import re


class NotConvergedSequence:
    """
    Class handles the generation of the Errors in kpi report
    """

    def __init__(self, sequence_dict, path_report, report_file_name):
        self.path_report = path_report
        self.report_file_name = report_file_name
        self.c2w_not_conv_df = sequence_dict['c2w_not_conv_df']
        self.c2w_frame_drops_df = sequence_dict['c2w_frame_drops_df']
        self.c2w_not_conv_dict = {}
        self.c2w_frame_drops_dict = {}
        self.xls = ExcelPrinter(path_report, report_file_name, 'Errors')
        self.current_row = 0

    def get_dictionaries(self):

        # pattern to get measurement name
        pattern = re.compile(r'(.+)_\w{3}_.+')

        if self.c2w_not_conv_df.shape[0] > 0:
            # get vision not converted dataframe, reset index (drop prevents the old index being added as a column)
            c2w_not_conv_df = self.c2w_not_conv_df[const.NOT_CONVERGED_INFO].reset_index(drop=True)
            # start index from 1 (not 0)
            c2w_not_conv_df.index += 1
            # get column with log name
            log_name_series = c2w_not_conv_df['LogName']
            # keep only ADCAM_VIN_DATE_TIME in split name, replace original log name
            c2w_not_conv_df['LogName'] = log_name_series.str.replace(pattern, lambda m: m.group(1))
            # transpose and convert to dictionary
            self.c2w_not_conv_dict = c2w_not_conv_df.T.to_dict()

        if self.c2w_frame_drops_df.shape[0] > 0:
            self.c2w_frame_drops_dict = self.c2w_frame_drops_df.to_dict()

    def export_not_converged(self):
        """
        :return: None
        Method adds new sheet in ExcelSheetGenerator
        """
        current_row = self.current_row
        if self.c2w_not_conv_dict:
            current_row = self.xls.export_to_excel(self.c2w_not_conv_dict, current_row, None, 'No convergence in Vision')
            current_row += 1
        if self.c2w_frame_drops_dict:
            self.xls.export_to_excel(self.c2w_frame_drops_dict, current_row, None, 'Frame drops in Autofix', nested=False)
