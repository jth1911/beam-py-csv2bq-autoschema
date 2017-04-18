from __future__ import absolute_import

import argparse
import logging
import re
import subprocess
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.utils.pipeline_options import PipelineOptions
from apache_beam.utils.pipeline_options import SetupOptions

#from csvio import ReadFromCSV
from schema_side_input import SchemaSideInput

class ConvertToTableRowFn(beam.DoFn):
  """Parses each line of input text into TableRows."""

  def process(self, element, si_fields, delimiter=","):
    """ Creates a json-like object with values matching column names. element
    should have the same length as si_fields once split. The split ignores delimiters in the json (within a double quote)
    Args:
      element: row read from the text file
      si_fields: list of columns name and whether they have json content {str 'name', bool 'is_json'}
      delimiter: gives the separator between field names
    Returns:
      Something similar to {'field_1':value_1, ..., field_n:value_n}
    """
    values = re.findall(r'(?:[^\s' + delimiter  + '"]|"(?:\\.|[^"])*"|(?<=,))+', element)
    if len(values) != len(si_fields):
        raise ValueError(
            'Number of column fields does not match the number of cells in the row {}.'.format(element))

    tr = {}
    # Read the metadata of the column and if it is a json string, we convert it
    # to a json object before adding it to the json object to store in BQ.
    for k, v in enumerate(values):
        column_meta = si_fields[k]
        if column_meta["is_json"]:
            v = json.loads(v.decode('string-escape').strip('"'))
            #TODO Parse this json and make sure that arrays are converted to string
        tr[column_meta["name"]] = v

    yield(tr)

def run(argv=None):
  """Run the workflow."""

  # Parse the input options
  parser = argparse.ArgumentParser()
  parser.add_argument('--runner', dest='runner', default='DirectRunner')
  parser.add_argument('--input_file', dest='input_file', default='csv_sample.csv')
  parser.add_argument('--input_schema', dest='input_schema', default='')
  parser.add_argument('--input_delimiter', dest='input_delimiter', default=',')
  parser.add_argument('--output_project', dest='output_project', default='mam-cloud')
  parser.add_argument('--output_dataset', dest='output_dataset', default='dummy')
  parser.add_argument('--output_table', dest='output_table', default='df_from_csv')
  parser.add_argument('--cat_read', dest='cat_read', default='10000', help='Choose big enough to read at least 2 lines of the csv')
  parser.add_argument('--skip_row', dest='skip_row', default=1, help='We generally assume that the first row in the csv contains column names')

  known_args, pipeline_args = parser.parse_known_args(argv)

  # Set the options based on the runner
  if known_args.runner == 'DataflowRunner':
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--staging_location=gs://bq-connector-pii/staging',
        '--temp_location=gs://bq-connector-pii/temp',
        '--job_name=csv2bq-{}'.format(str(datetime.datetime.now()).replace("-","").replace(".","").replace(":","").replace(" ","")),
    ])

  # Create the pipeline
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Prepares schema as a side input unless there is one given. There is not head
  # in GCS so we need to guess a number big enough to cover 2 lines at least: 10000
  fields = known_args.input_schema
  if fields == '':
      cmd_head = "gsutil cat -r 0-{} ".format(known_args.cat_read) if known_args.input_file[:5] == "gs://" else "head -n 2 "
      cmd_head = cmd_head + known_args.input_file
      row1_n_row2 = subprocess.check_output(cmd_head.split(" ")).split("\n")
      fields = row1_n_row2[0]
      row2 = row1_n_row2[1]

  si_fields, table_schema = SchemaSideInput(fields, row2).parseSchema()

  # Reads csv file. We skip the first line as we give the schema in input
  csv_lines = p | 'Read CSV' >> ReadFromText(known_args.input_file, skip_header_lines=known_args.skip_row)

  # Converts to TableRow
  bq_rows = csv_lines | 'Convert to BQ rows' >> beam.ParDo(ConvertToTableRowFn(), si_fields).with_output_types(unicode)

  # Writes to BigQuery
  bq_rows | 'write' >> beam.io.Write(
      beam.io.BigQuerySink(
          "{}:{}.{}".format(known_args.output_project, known_args.output_dataset, known_args.output_table),
          schema=table_schema,
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

  # Run the pipeline (all operations are deferred until run() is called).
  p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
