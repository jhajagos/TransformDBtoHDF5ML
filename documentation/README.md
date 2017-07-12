# Overview 

This manual describes the process of creating, executing and updating maps from relational database 
tables to the HDF5 matrix container format. The first step in the pipeline maps a database table 
to a JSON document. The JSON document is then mapped into the HDF5 matrix container format.

# Transforming relational database tables to a collection of JSON documents

## Mapping a single database table

### Setting up the runtime_config.json 

The `runtime_config.json` file is for setting parameters which will change at runtime. 
In general the `runtime_config.json` is not a version controlled file as it may contain passwords 
to connect to a database server. The file is divided in three sections. Only two of the sections 
are mandatory.

```json
{
  "source_db_config": {
    "connection_string": "sqlite:///./test/test_db.db3",
    "limit": null,
    "refresh_transactions_table": 1,
    "batch_size": 10000
  },
  "json_file_config": {
     "base_file_name": "transactions",
     "data_directory": "./test/"
  },
  "mongo_db_config": {
    "connection_string": "mongodb://localhost",
    "database_name": "encounters",
    "collection_name": "mapped_encounters",
    "refresh_collection": true
  },
  "use_ujson": false,
  "use_gzip_compression": false,
  "output_type": "json_file"
}
```

The first section `"source_db_config"` describes the source database which data will be extracted from: 

```json
{
    "connection_string": "sqlite:///./test/test_db.db3",
    "limit": null,
    "refresh_transactions_table": 1,
    "batch_size": 10000
}
```

The `"connection_string"` is a SQLAlchemy formatted [connection string](http://docs.sqlalchemy.org/en/latest/core/engines.html). 
In this example it is connecting to a SQLite file database. The mapper supports SQLite and PostgreSQL relational databases. 
The parameter `"limit"` is used for testing purposes to evaluate mapper output. By setting the
parameter to an integer n the mapper will be applied to n rows. The default value for this field is `null` 
with no limit. The parameter `"refresh_transactions_table"` is set by default to `1` to 
refresh an internal table that is used to join against. 
The final optional parameter is `"batch_size"` which sets the number of records that are included in each JSON file.

```json
{
     "base_file_name": "transactions",
     "data_directory": "./test/"
}
```

The `"data_directory"` is the file path. It should be written in a OS specific format, on a Linux system: 
`"/data/analysis/"` or in a windows environment: `"E:\\data\\analysis\"`. The parameter `"base_file_name"` is 
the prefix name for the JSON files, for example, setting it to `"encounters"` will generate 
files `"encounters_1.json"`, `"encounters_2.json"`, ... `"encounters_k.json"` where `k` 
is the last file number.

To store the JSON results in a MongoDB instance the configuration section `"mongo_db_config"` needs to
be set: 
```json
{
    "connection_string": "mongodb://localhost",
    "database_name": "encounters",
    "collection_name": "mapped_encounters",
    "refresh_collection": 1
 }
 ```
 
The parameter `"refresh_collection"` with a value `1` will replace an existing collection.

 For more optimal processing of large number of data there are two additional options. 
 These options make the outputted JSON documents less readable. The `"use_ujson"` which is default `0` 
 or `false` is to use the UltraJSON library which is significantly faster than
 the standard JSON library. The final option which saves disk storage space is to use the 
 gzip compression library on the generated JSON files.
 
 For mapping large volumes of data these options should be set.

### Creating a mapping.json file

The mapping file describes how table row data gets mapped to a JSON document. The simplest mapping.json
only includes a single mapping rule:

```json
{
    "main_transactions":
        {"table_name": "encounters",
             "fields_to_order_by": ["patient_id", "stay_id", "Discharge Date"],
             "where_criteria": null,
             "transaction_id": "encounter_id",
             "transaction_id_format": "int8",
             "schema": null
        },
    "mappings":
    [
        {
            "name": "discharge",
            "path": ["independent", "classes"],
            "table_name": "encounters",
            "type": "one-to-one",
            "fields_to_include":  ["encounter_id",  "medical_record_number",  "drg",
                                   "patient_gender", "patient_age", "day_from_start"]
        }
    ]
}
```

The `"main_transactions"` section specifies details about the base table `"table_name"`.   The `"transaction_id"` parameter
should point to the name primary key of the table. If the transaction_id is not unique than the mapping process will fail. By
default the data type of the transaction_id is assumed to be int8. The parameter `"schema"` sets the database schema.
For a subset of the table to select the SQL `"where_clause"` can be set. For generating matrices in a specific row order 
the `"fields_to_order_by"` sets this as a list of field names. 
 
The mapper configuration occurs in the `"mappings"` section.  Each mapping rule is an entry in a list. A mapping rule must have
a `"name"`, `"path"` and a `"type"`. Data is stored in nested dictionaries which creates a path. This is so data can be
 grouped together. By grouping data in paths this helps makes understanding the data clearer. It is general good practice
 to separate your independent variables with the dependent variables when building a predictive model. For example, setting the `"path"` to
 `["independent", "discharge"]` independent variable and for the variables that we are trying to predict 
 `["dependent", "discharge"]`.

The `"type"` parameter supports the following maps: `"one-to-one"`, `"one-to-many"` and `"one-to-many-class"`. The
simplest is `"one-to-one"`.  This pairs a `"transaction_id"` with one and only one row of the target table
 specified by the `"table_name"` parameter. 

```json
{
    "name": "discharge",
    "path": ["independent", "classes"],
    "table_name": "encounters",
    "type": "one-to-one",
    "fields_to_include":  ["encounter_id",  "medical_record_number",  "drg",
                           "patient_gender", "patient_age", "day_from_start"]
}
```

The `"table_name"` is the database table to extract data from. The `"path"` is the nested dictionary 
path where the `"name"` key is stored. As an example the above mapping rule would generate:
```json
{
    "999": {
        "independent": {
            "classes": {
                "discharge": {
                    "day_from_start": 15,
                    "drg": "002",
                    "encounter_id": 999,
                    "medical_record_number": 22,
                    "patient_age": 85,
                    "patient_gender": "U"
                }
            }
        }
    }
}
```
To get to the discharge details for `"transaction_id" = "999"` would be
`discharge_dict["999"]["independent"]["classes"]["discharge"]`. The final required parameter is 
`"fields_to_include"` which are the names of the database fields in the table to include in the extract.

## Mapping multiple relational database tables

The `"one-to-many`" maps a relation that is one-to-many between two database tables. As an example, 
an ordered set of diagnoses associated with a discharge that are stored in a separate table. The two tables will need 
to be linked by a common transaction id field. The transaction ID field must share the same name across both tables 
and be of the same type.

```json
{
    "name": "discharge_dx",
    "path": ["independent", "classes"],
    "table_name": "diagnoses",
    "fields_to_order_by": ["encounter_id", "sequence_id"],
    "type": "one-to-many",
    "fields_to_include": ["encounter_id", "sequence_id", "diagnosis_description", 
                           "diagnosis_code", "ccs_code", "ccs_description"]
}
```

An additional required parameter is `"fields_to_order_by"`. This parameter must include the linking field and
the ordering parameter. In the above case it is the `"sequence_id"`. If this is not set correctly 
the mapping process will have errors and fail.

The mapping `"one-to-many-class"` is similar to `"one-to-many"` except it splits the lists
into keyed entries in a dictionary/hash table. 

```json
{
    "name": "lab",
    "path": ["independent", "classes"],
    "table_name": "laboratory_tests",
    "fields_to_order_by": ["encounter_id", "test_name", "minutes_since_midnight"],
    "type": "one-to-many-class",
    "fields_to_include": ["encounter_id", "test_name", "code", "numeric_value","non_numeric_value",
      "test_status", "minutes_since_midnight"],
    "group_by_field": "test_name"
}
```

The additional parameter is `"group_by_field"` which is the field that is going 
to be used to split the entries into separate keyed lists. In this example the values will be split
by the `"test_name"`. This makes it easy to pull out the sequence of tests associated with
a specific test.

## Running the DB to document mapper script

Once the `runtime_config.json` and the `mapping_config.json` the mapper
script can be run. 

```bash
python build_document_mapping_from_db.py -c mapping_config.json -r runtime_config.json
```

The mapping file is given with `-c` flag and the runtime file is given with the `-r`. The script supports
the `-h` or help option.

The script will generate a set of JSON files that are stored in the `"data_directory"` 
defined in the `runtime_config.json` file. The
data will likely be split across multiple JSON files. The name of each generated file
is stored in a file that starts with `"base_file_name"` defined parameter 
and the file name ends in `"_batches.json"`. This file is needed for further steps in the 
pipeline.

# Mapping JSON Documents to a HDF5 matrix container

The JSON documents provide a structured way to examine data that is stored across multiple database tables.
To apply machine learning or data mining algorithms to the JSON the document needs to be mapped to a matrix format.
A mapping file provides instructions for mapping elements in the JSON document into a flat 2D matrix format. 
A JSON document can be mapped multiple ways.

```json
[
    {"path": ["independent", "classes", "discharge"],
     "variables": [
         {"cell_value": "age",
          "type": "integer"
         },
         {"cell_value": "age_float",
          "type": "float"
         },
         {
          "cell_value": "gender",
          "type": "categorical"
         },
         {
           "cell_value": "admit_date",
           "type": "datetime"
         }
     ],
     "type": "variables"
   }  
]
```
The `"path"` correspond to the nested levels of the JSON document. 
If the `"path"` is not found in a document then the value is not mapped. The `"cell_value"` parameter
corresponds to the field in the document. The `"type"` parameter specifies the data type of the field referenced
by `"cell_value"`.

A JSON document can be mapped to multiple matrices stored in a HDF5 container.

## Mapping a simple document

The simplest document to map is a document not composed of nested lists or nested documents. 

### Float / Integer Variables

Storing either an integer or a floating point value in a numeric matrix does not require any transformations.

### Categorical Variables

In order for categorical variables to be used in a machine learning model it needs to be transformed into a numeric value.
Such transformations are done through a process known as dummy coding.

## Mapping nested lists

A JSON document can contain nested lists. A nested list is used to represent a one-to-many relationship which is 
found in many relational databases.

### Mapping an ordered list

Sometimes the order of the nested list is important and should be represented in the list. As an example is a nested list
of categorical variables.

```json
{
    "path": ["independent", "classes", "diagnosis"],
    "type": "categorical_list",
    "process": "list_position",
    "field": "code",
    "name": "discharge_diagnosis",
    "label": "code",
    "cell_value": "code",
    "description": "description"
}
```

The following are also supported processes: `"process": "occurs_in_list"`,
`"process": "occurs_n_times_in_list"`.


### Mapping a numeric list

Only a single value can be stored in a 2-dimensional matrix.

```json
{
    "path": ["independent", "classes", "lab"],
    "export_path": ["independent", "classes", "lab", "count"],
    "type": "classes_templates",
    "class_type": "variables",
    "class_template":
        {
            "process": "count",
            "type": "numeric_list",
            "cell_value": "value"
        }
}
```

The following process types are supported for numeric lists:
`"process": "count"`, `"process": "median"`, `"process": "last_item"`, 
`"process": "first_item"`, `"process": "min"`, and `"process": "max"`.  

### Mapping a categorical list

```json
{
        "path": ["independent", "classes", "lab"],
        "export_path": ["independent", "classes", "lab", "category"],
        "type": "classes_templates",
        "class_type": "variables",
        "class_template":
            {
                "process": "count_categories",
                "type": "categorical_list",
                "cell_value": "result_category"
            }
}
```

### Mapping multiple targets 

```json
{
    "path": ["independent", "classes", "lab"],
    "export_path": ["independent", "classes", "lab", "category"],
    "type": "classes_templates",
    "class_type": "variables",
    "class_template":
        {
            "process": "count_categories",
            "type": "categorical_list",
            "cell_value": "result_category"
        }
}
```

### Filter criteria

Filtering by a single value:
```json
{
    "path": ["independent", "classes", "lab"],
    "export_path": ["independent", "classes", "lab", "day", "category"],
    "type": "classes_templates",
    "class_type": "variables",
    "class_template":
        {
            "process": "count_categories",
            "type": "categorical_list",
            "cell_value": "result_category",
            "filter": {"field": "day", "value": 1}
        }
}
```

Filtering by multiple values:
```json
{
    "path": ["independent", "classes", "lab"],
    "export_path": ["independent", "classes", "lab", "first_days", "category"],
    "type": "classes_templates",
    "class_type": "variables",
    "class_template":
        {
            "process": "count_categories",
            "type": "categorical_list",
            "cell_value": "result_category",
            "filter": {"field": "day", "value": [0,1, 2]}
        }
}
```

## Running the HDF5 container mapper script 

Once the mapping file is complete the matrix mapper script can be run. 
```bash
python build_hdf5_matrix_from_document.py -a data_file_base_name -b batch_dict.json -c data_template.json
```
It is recommended when developing the matrix mapping file to run a small subset of the data.

The mapper scripts requires a batch file that is generated by the mapper script.
```json
[
    {
        "batch_id": 1,
        "data_json_file": "C:\\Users\\janos\\GitHub\\TransformDBtoHDF5ML\\test\\files\\transactions_1_20170206.json",
        "sort_order_file_name": "C:\\Users\\janos\\GitHub\\TransformDBtoHDF5ML\\test\\files\\transactions_1_key_order_20170206.json"
    }
]
```
This file is automatically generated by the document mapper script and the user does not need to create it.

Running the HDF5 mapper script will generate an HDF5 file with a base name `data_file_base_name_20170101.hdf5`.

## Exploring the HDF5 container in Python

The mapper will create a single HDF5 file. A mapped matrix has three standard parts: 
`/core_array`, `/column_annotations`, and `/column_headers`. 

The example below shows how to access the encoded data in the file.
```python
import h5py
h5 = h5py.File("test_matrix_combined.hdf5","r")

list(h5["/independent/classes/diagnosis/"])
#[u'column_annotations', u'column_header', u'core_array']

h5["/independent/classes/diagnosis/core_array"][...]
#array([[ 1.,  0.,  3.,  2.],
#       [ 1.,  2.,  0.,  0.]])

h5["/independent/classes/diagnosis/column_annotations"][...]
#array([['discharge_diagnosis', 'discharge_diagnosis',
#        'discharge_diagnosis', 'discharge_diagnosis'],
#       ['1', '2', '4', '5'],
#       ['issues', 'problems', 'more issues', 'serious problems']],
#      dtype='|S128')


h5["/independent/classes/diagnosis/column_header"][...]
#array(['field_name', 'value', 'description'],
#      dtype='|S32')
```

## Basic summary file for an HDF5 container

To summarize file for basic analysis the script `summary_quick_hdf5.py` can be run.

```bash
python summary_quick_hdf5.py -f hdf5_filename.hdf5 -m 0.01
```

The script generates a CSV file: `hdf5_filename.hdf5.summary.csv`:
```csv
path,c1,c2,c3,non-zero,to_include,fraction non-zero
/lab/categories,Hemoglobin,Normal,,1,,0.3333333333333333
/lab/categories,Hemoglobin,Abnormal,,1,,0.3333333333333333
/lab/categories,Blood Glucose,Low,,1,,0.3333333333333333
/lab/categories,Blood Glucose,Normal,,2,1,0.6666666666666666
/lab/categories,Blood Glucose,High,,1,,0.3333333333333333
/lab/categories,WBC,Normal,,2,1,0.6666666666666666
/lab/categories,WBC,Abnormal,,3,1,1.0
/lab/category_count_normalized,Hemoglobin,Normal,,1,,0.3333333333333333
/lab/category_count_normalized,Hemoglobin,Abnormal,,1,,0.3333333333333333
/lab/category_count_normalized,Blood Glucose,Low,,1,,0.3333333333333333
/lab/category_count_normalized,Blood Glucose,Normal,,2,1,0.6666666666666666
/lab/category_count_normalized,Blood Glucose,High,,1,,0.3333333333333333
/lab/category_count_normalized,WBC,Normal,,2,1,0.6666666666666666
/lab/category_count_normalized,WBC,Abnormal,,3,1,1.0
/lab/category_present,Hemoglobin,Normal,,1,,0.3333333333333333
/lab/category_present,Hemoglobin,Abnormal,,1,,0.3333333333333333
/lab/category_present,Blood Glucose,Low,,1,,0.3333333333333333
/lab/category_present,Blood Glucose,Normal,,2,1,0.6666666666666666
/lab/category_present,Blood Glucose,High,,1,,0.3333333333333333
/lab/category_present,WBC,Normal,,2,1,0.6666666666666666
/lab/category_present,WBC,Abnormal,,3,1,1.
```

## Using the summary file to generate a field selection

```bash
python generate_field_selection_json_from_csv.py -f  hdf5_filename.hdf5.summary.csv -c to_include
```

Running the script generates a JSON file `hdf5_filename.hdf5.summary.csv.json` 
for field selection:
```json
[
    [
        "/lab/categories",
        [
            "Blood Glucose",
            "Normal"
        ],
        [
            "WBC",
            "Normal"
        ],
        [
            "WBC",
            "Abnormal"
        ]
    ],
    [
        "/lab/category_count_normalized",
        [
            "Blood Glucose",
            "Normal"
        ],
        [
            "WBC",
            "Normal"
        ],
        [
            "WBC",
            "Abnormal"
        ]
    ],
    [
        "/lab/category_present",
        [
            "Blood Glucose",
            "Normal"
        ],
        [
            "WBC",
            "Normal"
        ],
        [
            "WBC",
            "Abnormal"
        ]
    ]
]
```
This helps generate a smaller width matrix (less features) which can be easier to work with.

## Filtering the generated HDF5 container

### Selecting columns

A JSON file of nested lists is used to encode which variables or columns to include.
```json
[
    [
        "/identifiers/independent",
        [
            "start_julian_day"
        ],
        [
            "end_julian_day"
        ],
        [
            "patient_id"
        ],
        [
            "stay_id"
        ],
        [
            "stay_id_last_admission"
        ],
        [
            "transaction_id"
        ]
    ]
]
```

### Selecting rows with a query

A simple list of selection criteria can be used to reduce the number of rows across a matrix. 

```json
[
     ["/discharge/demographic", ["gender","m"], 1],
     ["/discharge/demographic", "age", [65, 66, 67]],
     ["/lab/values", "a1c", 8, ">="]
]
```
The filter is applied against all rows in all paths in the HDF5 container.
```bash
python compact_subset_hdf5.py -f hdf5_filename.hdf5 -o hdf5_filename.hdf5.summary.csv.out.hdf5 -c hdf5_filename.hdf5.summary.csv 
```

## Post processing the generated HDF5 container

Additional post processing of a matrix in the HDF5 container can be specified.
```json
[
   {
      "path": "/independent/classes/lab/categories/",
      "write_path": "/independent/classes/lab/categories_normalized/",
      "rule": "normalize_category_count"
   },
   {
      "path": "/independent/classes/procedure/",
      "write_path": "/independent/classes/procedure_present/",
      "rule": "zero_or_one"
   }
 ] 
```

The post processing is run as:
```bash
python post_process_hdf5_with_rules.py -f hdf5_filename.hdf5 -r rules.json
```