import json
import re
from itertools import izip

from apache_beam.io.gcp.internal.clients import bigquery

class SchemaSideInput():

    def __init__(self, row1, row2="", delimiter=","):
        """ Creates a BigQuery schema based on the first 2 rows of the dataset:
        - if the schema is flat only the first row will matter but we'll have to parse the 2nd to make sure
        - if the schema is nested we'll discover it with the 2nd row by reading the values.
        Args:
            row1: String that contains the column names of the file
            row2: String that contains the first row of actual data

        Returns:
            void
        """
        self.table_schema = bigquery.TableSchema()
        self.json_columns = []
        self.row1 = row1
        self.row2 = row2
        self.delimiter = delimiter

    #TODO Currently it only accepts delimiters that are not contained in the cells
    # So for example, we cannot have a comma delimiter because JSON strings contain it
    def parseSchema(self):
        """ Reads the schema of the given rows using row2 as a check for hidden
        json strings.
        Args:

        Returns:
            A tuple (json_columns, table_schema) where
            - json_columns is a list of the columns [{'name':<COLUMN_NAME>, 'is_json':<CONTAINS_A_JSON_STRING>}]
            - table_schema is the actual bigquery.TableSchema
        """
        column_names = self.row1.split(self.delimiter)

        for idx, cn in enumerate(column_names):
            column_schema = bigquery.TableFieldSchema()
            column_schema.name = cn
            column_schema.type = "string"
            column_schema.mode = 'nullable'
            is_json = False

            if self.row2 != "":
                data_values = re.findall(r'(?:[^\s' + self.delimiter  + '"]|"(?:\\.|[^"])*"|(?<=,))+', self.row2)
                check_json = self._is_json(data_values[idx].decode('string-escape').strip('"'))
                if check_json:
                    # Update the schema type and mark the column as json
                    column_schema.type = 'record'
                    is_json = True

                    # Converts JSON to TableFieldSchema and append it to the column
                    nested_schema = self._json_to_table_schema(check_json, column_schema)

                    # Appends
                    column_schema = nested_schema
            # Add the column name to the list and whether it is json or not
            self.json_columns.append({'name':cn, 'is_json':is_json})

            # Add the field to the schema
            self.table_schema.fields.append(column_schema)

        return self.json_columns, self.getSchema()

    def setSchema(self, schema):
        """ Sets the schema of the table.
        """
        self.table_schema.fields.append(schema)

    def getSchema(self):
        """ Returns the schema of the table.
        """
        return self.table_schema

    def _is_json(self, field):
        """ Checks if a fields looks like a json.
        Args:
            field: String to identify as a json or not
        Returns:
            False if not a json or the json object
        """
        try:
            js = json.loads(field)
            if isinstance(js, dict):
                return js
            return False
        except ValueError:
            return False

    def _json_to_table_schema(self, from_json, child_schema):
        """ Recursively converts a json object to a BigQuery TableFieldSchema
        Args:
            from_json: the json object to convert
        Returns:
            A TableFieldSchema that can be appened to an existing TableFieldSchema
        """
        for k, v in from_json.items():
            no = bigquery.TableFieldSchema()
            no.name = k
            no.type = "string"
            no.mode = 'nullable'
            if isinstance(v, dict):
                no.type = "record"
                self._json_to_table_schema(v, no)
            child_schema.fields.append(no)

        return child_schema

    def __getBigQueryFieldType(self, field):
        """ Returns the type of a field based on how it looks like. We make it
        private as it should only be accessed from the class
        Args:
            field: Object that needs to have its type identified
        Returns:
            Type of the field
        """
        if type(field) is 'str':
            return 'string'
        elif type(field) is 'int':
            return 'integer'
        elif type(field) is 'float':
            return 'float'
        elif type(field) is 'long':
            return 'integer'
        elif type(field) is 'list':
            return 'repeated'
        else:
            return 'string'


