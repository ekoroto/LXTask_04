from mysql.connector import errorcode

import mysql.connector


class DBHandler:
    """
        Handles DB creation, inserting and queries.
    """

    DB_NAME = 'LXTask_04'
    TABLES = {}

    def __init__(self):
        self.connector = self.create_connection()
        self.cursor = self.connector.cursor()

    @staticmethod
    def create_connection():
        try:
            connector = mysql.connector.connect(host='localhost',
                                                user='root',
                                                password='passwroot',
                                                database='LXTask_04')
            return connector
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def close_connection(self):
        self.cursor.close()
        self.connector.close()

    def create_database(self):
        """
            Creates MySQL database.
        """

        self.cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {self.DB_NAME} DEFAULT CHARACTER SET 'utf8'"
        )
        self.cursor.execute(
            f"USE {self.DB_NAME}"
        )

    def create_tables(self):
        """
            Creates tables in DB_NAME database.
        """

        self.TABLES['rooms'] = (
            "CREATE TABLE IF NOT EXISTS `rooms` ("
            "  `id` SMALLINT NOT NULL,"
            "  `name` VARCHAR(64) NOT NULL,"
            "PRIMARY KEY (`id`)"
            ")"
        )
        self.TABLES['students'] = (
            "CREATE TABLE IF NOT EXISTS `students` ("
            "  `id` INT NOT NULL,"
            "  `name` VARCHAR(256) NOT NULL,"
            "  `birthday` DATETIME NOT NULL,"
            "  `room` SMALLINT NOT NULL,"
            "  `sex` ENUM('M', 'F') NOT NULL,"
            "PRIMARY KEY (`id`),"
            "CONSTRAINT fk_room FOREIGN KEY (`room`) "
            "  REFERENCES `rooms` (`id`) ON DELETE CASCADE"
            ")"
        )

        for table_name in self.TABLES:
            self.cursor.execute(self.TABLES[table_name])

    def insert_students(self, students_data):
        add_students = (
            "INSERT IGNORE INTO students "
            "(id, name, birthday, room, sex) "
            "VALUES (%(id)s, %(name)s, %(birthday)s, %(room)s, %(sex)s)"
        )

        self.cursor.executemany(add_students, students_data)
        self.connector.commit()

    def insert_rooms(self, rooms_data):
        add_rooms = (
            "INSERT IGNORE INTO rooms "
            "(id, name) "
            "VALUES (%(id)s, %(name)s)"
        )

        self.cursor.executemany(add_rooms, rooms_data)
        self.connector.commit()

    def create_room_birthday_idx(self):
        try:
            add_room_birthday_idx = (
                "CREATE INDEX idx_room_birthday ON students (room, birthday)"
            )

            self.cursor.execute(add_room_birthday_idx)
        except mysql.connector.Error as err:
            print(f"{err}")

    def create_sex_index(self):
        try:
            add_sex_index = (
                "CREATE INDEX idx_sex ON students (sex)"
            )
            self.cursor.execute(add_sex_index)
        except mysql.connector.Error as err:
            print(f"{err}")

    def get_students_num_in_rooms(self):
        """
            Returns list of rooms with number of students.
        """

        query = (
            "SELECT s.room, r.name, COUNT(s.room) FROM students as s "
            "  JOIN rooms as r ON s.room = r.id "
            "  GROUP BY s.room"
        )

        self.cursor.execute(query)
        data = self.cursor.fetchall()

        pretty_data = []
        for room in data:
            count_in_room = {
                'id': room[0],
                'room': room[1],
                'number': room[2]
            }
            pretty_data.append(count_in_room)
        result = {
            'title': 'List of rooms with number of students.',
            'rooms': pretty_data
        }
        return result

    def get_top_min_avg_age(self):
        """
            Returns top 5 rooms with the smallest average age of students.
        """

        query = (
            "SELECT s.room, r.name, AVG(TIMESTAMPDIFF(YEAR, birthday, CURDATE())) as age FROM students as s "
            "  JOIN rooms as r ON s.room = r.id "
            "  GROUP BY s.room ORDER BY age LIMIT 5"
        )
        self.cursor.execute(query)
        data = self.cursor.fetchall()

        pretty_data = []
        for room in data:
            avg_in_room = {
                'id': room[0],
                'room': room[1]
            }
            pretty_data.append(avg_in_room)
        result = {
            'title': 'Top 5 rooms with the smallest average age of students.',
            'rooms': pretty_data
        }
        return result

    def get_top_max_age_difference(self):
        """
            Returns top 5 rooms with the biggest difference in students age.
        """

        query = (
            "SELECT s.room, r.name, "
            "  MAX(TIMESTAMPDIFF(YEAR, birthday, CURDATE())) - MIN(TIMESTAMPDIFF(YEAR, birthday, CURDATE())) as age"
            "  FROM students as s "
            "  JOIN rooms as r ON s.room = r.id "
            "  GROUP BY s.room ORDER BY age, s.room LIMIT 5"
        )
        self.cursor.execute(query)
        data = self.cursor.fetchall()

        pretty_data = []
        for room in data:
            max_in_room = {
                'id': room[0],
                'room': room[1]
            }
            pretty_data.append(max_in_room)
        result = {
            'title': 'Top 5 rooms with the biggest difference in students age.',
            'rooms': pretty_data
        }
        return result

    def get_mixed_sex_rooms(self):
        """
            Returns list of rooms with students of different sexes.
        """

        query = (
            "SELECT s.room, r.name FROM students as s "
            "  JOIN rooms as r ON s.room = r.id "
            "  GROUP BY s.room "
            "  HAVING COUNT(DISTINCT sex) > 1"
        )

        self.cursor.execute(query)
        data = self.cursor.fetchall()

        pretty_data = []
        for room in data:
            mixed_in_room = {
                'id': room[0],
                'room': room[1]
            }
            pretty_data.append(mixed_in_room)
        result = {
            'title': 'Rooms with students of different sexes.',
            'rooms': pretty_data
        }
        return result

