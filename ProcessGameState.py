import pandas as pd


class ProcessGameState:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        self.columns = None

    def read_data(self):
        self.data = pd.read_parquet(self.file_path)
        self.columns = self.data.columns.tolist()

    def load_data(self):
        return self.data

    def check_boundaries(self):
        if self.columns is None:
            raise ValueError("Call read_data() before checking boundaries")

        boundary = {
            'round_num': (1, 30),
            'tick': (0, 3000),
            'side': ('T', 'CT'),
            'team': ('Team1', 'Team2'),
            'hp': (0, 100),
            'armor': (0, 100),
            'is_alive': (False, True),
            'total_utility': (0, 4),
            'equipment_value': (0, 5000),
            'area_name': ['TSpawn', 'TStairs', 'Tunnels', 'Fountain', 'LowerPark', 'Playground'],
            'seconds': (0, 60),
            'clock_time': ('00:00', '10:00'),
            't_alive': (1, 5),
            'ct_alive': (1, 5),
            'bomb_planted': (False, True),
            'map_name': 'de_overpass',
            'utility_used': (0, 10),
        }

        within_boundary = pd.Series(True, index=self.data.index)

        for column in self.columns:
            if column in boundary:
                bound = boundary[column]

                if isinstance(bound, tuple):
                    lower_bound, upper_bound = bound
                    if lower_bound is not None:
                        within_boundary &= self.data[column] >= lower_bound
                    if upper_bound is not None:
                        within_boundary &= self.data[column] <= upper_bound
                else:
                    within_boundary &= self.data[column].isin([bound])

        return within_boundary


if __name__ == '__main__':
    # Retrieve the file path to the data
    file_path = 'C:\\Users\\melos\\OneDrive\\Desktop\\CSGO-Strategy-Analyzer\\data\\game_state_frame_data.parquet'

    # Create a ProcessGameState object that takes in the file path
    game_state_processor = ProcessGameState(file_path)

    # Ingest the data
    game_state_processor.read_data()

    # Check the boundaries
    within_boundary = game_state_processor.check_boundaries()

    # Filter the data that makes it past the boundary check
    filtered_data = game_state_processor.load_data()[within_boundary]

    # Print the data
    for column in game_state_processor.columns:
        print(f"{column}")
        for _, row in filtered_data.iterrows():
            print("\t".join(str(value) for value in row[column]))
