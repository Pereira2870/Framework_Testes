%sql
DROP TABLE IF EXISTS workbench_reportinghub.test_out_results;

%sql
CREATE TABLE IF NOT EXISTS workbench_reportinghub.test_out_results (
  --RESULT_ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  RESULT_ID INT,
  TEST_ID INT,
  QUERY STRING,
  RESULT STRING,
  PARTITION_FILTER_VALUE STRING,
  TIMESTAMP TIMESTAMP
);

%sql
DROP TABLE IF EXISTS workbench_reportinghub.test_out_results_details;
%sql
CREATE TABLE IF NOT EXISTS workbench_reportinghub.test_out_results_details (
  --DETAILS_ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  DETAILS_ID INT,
  RESULT_ID INT,
  KEY_FIELDS STRING,
  TIMESTAMP TIMESTAMP
);
%sql
DROP TABLE IF EXISTS workbench_reportinghub.test_config_parameters;
%sql
CREATE TABLE IF NOT EXISTS workbench_reportinghub.test_config_parameters (
  TEST_ID INT,
  TYPE_ID INT,
  SUBTYPE_ID INT,
  SOURCE_TABLE STRING,
  DEST_TABLE STRING,
  JOIN_KEY STRING,
  SOURCE_SELECT_FIELD STRING,
  DEST_SELECT_FIELD STRING,
  SOURCE_FILTER STRING,
  DEST_FILTER STRING,
  SOURCE_GROUPBY STRING,
  DEST_GROUPBY STRING,
  SOURCE_FLAG_PARTITION_FILTER BOOLEAN,
  DEST_FLAG_PARTITION_FILTER BOOLEAN,
  SOURCE_PARTITION_FILTER_FIELD STRING,
  DEST_PARTITION_FILTER_FIELD STRING,
  KEY_FIELDS STRING
);
%sql
DROP TABLE IF EXISTS workbench_reportinghub.test_rel_set_parameter;

%sql
CREATE TABLE IF NOT EXISTS workbench_reportinghub.test_rel_set_parameter (
  SET_ID INT,
  TEST_ID INT
);

%sql
DROP TABLE IF EXISTS workbench_reportinghub.test_config_set;

%sql
CREATE TABLE IF NOT EXISTS workbench_reportinghub.test_config_set (
  SET_ID INT,
  NAME STRING,
  DESCRIPTION STRING
);
%sql
DROP TABLE IF EXISTS workbench_reportinghub.test_config_subtype;

%sql
CREATE TABLE IF NOT EXISTS workbench_reportinghub.test_config_subtype (
  SUBTYPE_ID INT,
  NAME STRING,
  DESCRIPTION STRING
);
%sql
DROP TABLE IF EXISTS workbench_reportinghub.test_config_type;

%sql
CREATE TABLE IF NOT EXISTS workbench_reportinghub.test_config_type (
  TYPE_ID INT,
  NAME STRING,
  DESCRIPTION STRING
);

%sql
DROP TABLE IF EXISTS workbench_reportinghub.test_rel_type_subtype;

%sql
CREATE TABLE IF NOT EXISTS workbench_reportinghub.test_rel_type_subtype (
  TYPE_ID INT,
  SUBTYPE_ID INT,
  NAME STRING,
  DESCRIPTION STRING
);