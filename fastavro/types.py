class Record(dict):
    def set_name(self, name):
        self._fastavro_schema = name

    def get_name(self):
        return self._fastavro_schema
