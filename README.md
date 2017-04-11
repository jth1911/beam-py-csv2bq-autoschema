This script reads a csv file and load it to bigquery with its schema (assumed to be present on the first row). It uses a Dataflow pipeline. The schema finder works recursively on json objects but ignore arrays and treat them as strings. If there are arrays in the string, either adapt this code or do another transform from BigQuery to BigQuery using json parsing functions or UDF.

# Setup
Install beam
```
virtualenv venv
source venv/bin/activate
pip install apache-beam
pip install apache-beam[gcp]
```

#Caveats and good to know

1. The code is not able to handle comma separator if any of the values in the csv also contains comma (for now I put |). Shouldnt be hard to do but I'll let the customer do it. The code that breaks it currently is in schema_side_input.py
```
data_values = self.row2.split(self.delimiter)"
```
2. For some reason, if I name a column "location", it breaks. Not sure why and didn't investigate. Might be some other reserved words that do the same.
3. gsutil does not have a head option so I did a gsutil cat -r. We need to make sure that the value chosen for r covers at least 2 lines so take it big. It can be passed as a param to the pipeline through --cat_read. Is set up at 10000 by default.
4. It can take care of cell that contain json fields (up to a certain point, see below) but you have to make sure that row#2 contains a full json for all cells as the schema will be decided based on that line.
5. If the json contains an array, it will most break the script because:
- Array will be considered as string in schema_side_input (only have a condition for RECORD)
- But they will be json.loaded automatically in ConvertToTableRowFn and returned as an array and not a string. There is a TODO there where it is possible to parse the json and transform the array to string.
6. If you decided to provide the schema yourself (--input_schema), keep in mind point (4) of this list. Also make sure to skip the first row (--skip_row) accordingly depending on whether the first row has a schema as well or gives data directly. Provided schema will overwrite the automatic schema finder


# Run
## Locally
```
python -m csv_to_bq --output_project <YOUR_PROJECT> \
                    --output_dataset  <YOUR_DATASET> \
                    --output_table  <YOUR_TABLE>
```
## Dataflow
```
python -m csv_to_bq --runner DataflowRunner \
                    --project <your-gcp-project> \
                    --temp_location gs://<your-gcs-bucket>/tmp/
                    --staging_location gs://<your-gcs-bucket>/staging/
                    --output_project <YOUR_PROJECT> \
                    --output_dataset  <YOUR_DATASET> \
                    --output_table  <YOUR_TABLE> \
                    --input_file gs://<your-gcs-bucket>/<your-csv-file> \
```
