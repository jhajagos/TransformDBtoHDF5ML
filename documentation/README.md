# Overview of the pipeline

This tutorial describes the process of creating, executing and updating maps from relation database tables to the HDF5 file 
matrix format. Between the relational database and the HDF5 format there is a JSON format.
The pipeline can be run at various  points and does not need to be run to the final endpoint. A user may only want to 
map to a JSON document so it can be loaded into a MongoDB instance and another user might start with a JSON document 
and want to generate an HDF5 file.

# Going from relational database to a JSON document

## Mapping a single database table

### Setting up the runtime_config.json 

The `runtime_config.json` file is for setting parameters which will change. In general the runtime_config.json
is not a version controlled file as it may contain passwords to connect to a database. It is divided in 
three sections. Only two sections are mandatory.

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
In the example here it is connecting
to a local SQLite database. The two supported database system are SQLite and PostgreSQL. The 
parameter `"limit"` is used for testing purposes to evaluate mapper output. By setting the
parameter to `null` then there is no limit. The parameter `"refresh_transactions_table"` is set by default to `1` to 
refresh an internal table that is used to join against. The final optional parameter is `"batch_size"` which sets the number
of records that are included in each JSON file.

```json
{
     "base_file_name": "transactions",
     "data_directory": "./test/"
}
```

The `"data_directory"` is the file path. It should be written in a OS specific format, on a Linux system: 
`"/data/analysis/"` or in a windows environment: `"E:\\data\\analysis\"`. The parameter `"base_file_name"` is 
the prefix name for the JSON files, for example, setting it to `"encounters"` will generate 
files `"encounters_1.json"`, `"encounters_2.json"`, ... 

To store the JSON results in a MongoDB instance then the configuration section `"mongo_db_config"`: 
```json
{
    "connection_string": "mongodb://localhost",
    "database_name": "encounters",
    "collection_name": "mapped_encounters",
    "refresh_collection": 1
 }
 ```
 
 The parameter `"refresh_collection"` with a value `1` will replace an existing collection.

 For more optimal processing of large number of data there are two additional options. These options make the outputted
 JSON files less readable. The `"use_ujson"` which is default `0` or `false` is to use the UltraJSON library which is faster than
 the standard JSON library. The final option which saves considerable disk 
 storage space is to use the gzip compression library
 on the generated JSON files.

### Creating a mapping.json file

The mapping file describes how table data gets mapped to a JSON document. The simplest mapping.json
file includes a single mapping rule:

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
simplest to start with is `"one-to-one"`.  This pairs a `"transaction_id"` with one and only one row of the target table
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

The `"one-to-many`" maps a relation that is one-to-many between two database tables. As an example, an ordered set of diagnoses 
associated with a discharge that are stored in a separate table. The two tables will need to be linked by a common transaction id field.
The transaction ID field must share the same name across both tables and be of the same type.

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
the ordering parameter. In the above case that is `"sequence_id"`. If this is not set correctly 
the program will break apart your rows incorrectly.

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

Once the `runtime_config.json` and the `mapping_config.json` files running the 
script is straight forward. The JSON files can have any name but the mapping file
comes before the runtime file.

```bash
python build_document_mapping_from_db.py mapping_config.json runtime_config.json
```

The script will generate a set of JSON files that are stored in the `"data_directory"` 
defined in the `runtime_config.json` file. The
data will likely be split across multiple JSON files. The name of each generated file
is stored in a file that starts with `"base_file_name"` defined parameter 
and ends in `"_batches.json"`. This file is needed for further steps in the 
pipeline.

# Mapping a JSON Document to HDF5

Mapping a document to a matrix is an important step in machine learning  mining applications. 
A mapping file provides instructions for mapping elements in JSON document into a flat 2D matrix format. 
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
     ]
   }  
]
```
The `"path"` correspond to the nested levels of the JSON document. 
If the `"path"` is not found then the value is not mapped. The `"cell_value"` parameter
corresponds to the field in the documents. The `"type"` parameter specifies the data type of the field referenced
by `"cell_value"`.

A JSON document can be mapped to multiple matrices stored in a HDF5 container.


## Mapping a simple document

The simplest document to map is a document not composed of nested lists or nested documents. 

### Float / Integer Variables

### Categorical Variables

In order for categorical variables to be used in a machine learning model it needs to be transformed into a numeric value.
Such transformations are done throught a process known as dummy coding.

## Mapping nested lists

Often a document will contain a nested list. A nested list is used to represent a one-to-many relationship which is 
found in many databases.

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

### Mapping a numeric list

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

```"process": "count"```, ```"process": "median"```, ```"process": "last_item"```, ```"process": "first_item"```  


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

## Running the mapper script for a JSON document to HDF5 matrix

```bash
python build_hdf5_matrix_from_document.py data_file_base_name batch_dict.json data_template.json
```

## Exploring the HDF5 file in Python


## Filtering the generated HDF5 file

### Selecting columns

### Selecting rows

## Post processing the generated HDF5 file
