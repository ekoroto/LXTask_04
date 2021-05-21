from db_handler import DBHandler
from file_handler import FileHandler


class DBOperationsHandler:
    """
        Processes DB data.
    """

    def __init__(self):
        self.db = DBHandler()
        self.file_handler = FileHandler()
        self.db.create_database()
        self.db.create_tables()
        self.db.create_room_birthday_idx()
        self.db.create_sex_index()
        self.insert_data()

    def insert_data(self):
        self.db.insert_rooms(self.file_handler.rooms)
        self.db.insert_students(self.file_handler.students)

    def get_data_from_queries(self):
        self.file_handler.write(self.queries_merger())
        self.db.close_connection()

    def queries_merger(self):
        merged_data = list()
        merged_data.append(self.db.get_students_num_in_rooms())
        merged_data.append(self.db.get_top_min_avg_age())
        merged_data.append(self.db.get_top_max_age_difference())
        merged_data.append(self.db.get_mixed_sex_rooms())
        return merged_data
