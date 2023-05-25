"""
***********************************************************************
Function: ProcessGameState.py handles file ingestion/ETL and is
able to extract different columns from a data file for analysis
----------------------------------------------------------------------
Input: data file path
Output: filtered match data
----------------------------------------------------------------------
Author: Sydney Nguyen
Version: 05/24/2023
**********************************************************************
"""

import pandas as pd


class ProcessGameState:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        self.columns = None
        self.boundaries = {}

    def read_data(self):
        self.data = pd.read_parquet(self.file_path)
        self.columns = self.data.columns

    def load_data(self):
        return self.data

    def set_boundaries(self, column, lower_bound, upper_bound):
        self.boundaries[column] = (lower_bound, upper_bound)

    def check_boundaries(self):
        if self.columns is None:
            raise ValueError("Call read_data() before checking boundaries")

        within_boundary = pd.Series(True, index=self.data.index)

        for column in self.columns:
            if column in self.data.columns and column in self.boundaries:
                column_data = self.data[column]
                bound = self.boundaries[column]

                if pd.api.types.is_numeric_dtype(column_data):
                    lower_bound, upper_bound = bound
                    within_boundary &= column_data.between(lower_bound, upper_bound)
                else:
                    unique_values = bound
                    within_boundary &= column_data.isin(unique_values)

        return within_boundary

    def extract_weapon_classes(self):
        return self.data['inventory']


if __name__ == '__main__':
    file_path = 'game_state_frame_data.parquet'

    process_strategies = ProcessGameState(file_path)

    process_strategies.read_data()

    process_strategies.set_boundaries('tick', 1000, 2000)
    process_strategies.set_boundaries('hp', 0, 50)

    print(process_strategies.check_boundaries())

    print(process_strategies.extract_weapon_classes())