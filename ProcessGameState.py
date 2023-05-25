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

    # ProcessGameState is created with a file path as the parameter
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        self.columns = None
        self.boundaries = {}

    # Reads the file path and sets the columns to self.columns
    def read_data(self):
        self.data = pd.read_parquet(self.file_path)
        self.columns = self.data.columns

    # Outputs the data file
    def load_data(self):
        return self.data

    # Allows users to set a lower and upper bound to filter out data
    # usage: process_game.set_boundaries('health', 0, 75)
    def set_boundaries(self, column, lower_bound, upper_bound):
        self.boundaries[column] = (lower_bound, upper_bound)

    # Checks if each data set is in between user-provided boundaries and returns the data within those bounds
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

    # Extracts the weapon class within the inventory column and returns it
    def extract_weapon_classes(self):
        if 'inventory' not in self.columns:
            raise ValueError("Inventory column does not exist.")

        key = 'weapon_class'

        self.data[key] = self.data['inventory'].apply(lambda x: [item[key] for item in x] if x is not None else None)
        return self.data[key]


if __name__ == '__main__':
    data_path = 'game_state_frame_data.parquet'

    process_strategies = ProcessGameState(data_path)
    process_strategies.read_data()
