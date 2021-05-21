from abc import ABC, abstractmethod
from argparse import ArgumentParser, ArgumentTypeError
from json import dumps, loads
from os import getcwd, mkdir
from os.path import exists

from dict2xml import dict2xml


class AbstractExporter(ABC):

    @abstractmethod
    def export(self, data):
        pass


class JSONExporter(AbstractExporter):
    """
        Exports data to json format
    """
    extension = '.json'

    def export(self, data):
        return dumps(data)


class XMLExporter(AbstractExporter):
    """
        Exports data to xml format
    """

    extension = '.xml'

    def export(self, data):
        return dict2xml(data)


class ArgsHandler:
    """
        Validates input arguments
    """

    _formats = ('xml', 'json')

    def __init__(self):
        self.args = self.parse_args()

    def get_exporter(self):
        if self.args.format == 'xml':
            return XMLExporter
        return JSONExporter

    def get_rooms_path(self):
        return self.args.rooms_path

    def get_students_path(self):
        return self.args.students_path

    def parse_args(self):
        parser = ArgumentParser()
        parser.add_argument('students_path', type=self._validate_path, help='Path to file with students.')
        parser.add_argument('rooms_path', type=self._validate_path, help='Path to file with rooms.')
        parser.add_argument('format', type=self._validate_format, help='Format of export.')
        return parser.parse_args()

    @staticmethod
    def _validate_path(value):
        if not exists(value):
            raise ArgumentTypeError(f'Incorrect path {value}')
        return value

    def _validate_format(self, value):
        if value not in self._formats:
            raise ArgumentTypeError(f'Incorrect format of export {value}')
        return value


class FileHandler:
    """
        Handles file processing
    """

    def __init__(self):
        self.args = ArgsHandler()
        self.students = self.read(self.args.get_students_path())
        self.rooms = self.read(self.args.get_rooms_path())
        self.exporter = self.args.get_exporter()

    def read(self, file_path):
        with open(file_path, 'r') as f:
            data = f.read()
            data = self._deserialize_json(data)
            return data

    def write(self, merged_data):
        export_data = self.exporter().export(merged_data)
        folder_path = getcwd() + '/output_data/'
        file_path = folder_path + 'output' + self.exporter.extension
        if not exists(folder_path):
            mkdir(folder_path)
        with open(file_path, 'w') as f:
            f.write(export_data)

    @staticmethod
    def _deserialize_json(data):
        return loads(data)
