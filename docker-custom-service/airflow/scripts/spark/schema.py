from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, DateType


class CustomSchema(object):
    def __init__(self, dict_schema: [str]):
        self.dic_schema = dict_schema

    def get_schema(self, schema):
        if schema in self.dic_schema:
            return self.dic_schema[f"{schema}"]
        else:
            return "Don't exist schema"

    def get_list_schema(self, item):
        return [x for x in self.dic_schema.keys()]

    def update_schema(self, schema: {}):
        self.dic_schema.update(schema)
