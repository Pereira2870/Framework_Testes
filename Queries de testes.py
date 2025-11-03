# Databricks notebook source
# MAGIC %sql
# MAGIC select *
# MAGIC from hive_metastore.framework_testes.test_out_results_details
# MAGIC order by TIMESTAMP desc;
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC select *
# MAGIC from hive_metastore.framework_testes.test_out_results
# MAGIC order by TIMESTAMP desc;
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC select *
# MAGIC from hive_metastore.framework_testes.test_config_parameters;
# MAGIC
# MAGIC %sql
# MAGIC select *
# MAGIC from hive_metastore.framework_testes.test_rel_set_parameter;
# MAGIC
# MAGIC %sql
# MAGIC truncate table hive_metastore.framework_testes.test_out_results_details;
# MAGIC truncate table hive_metastore.framework_testes.test_out_results;
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC select *
# MAGIC from joaopereira.premier_league_data_2022_v3;
# MAGIC
# MAGIC %sql
# MAGIC select *
# MAGIC from joaopereira.premier_league_data_2022;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC SELECT clube FROM joaopereira.premier_league_data_2022 WHERE clube IS NULL OR vitorias IS NULL group by clube ;
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC select * from joaopereira.premier_league_data_2022;
# MAGIC
# MAGIC %sql
# MAGIC select * from iliyan.premier_league_data_2022;
# MAGIC %sql
# MAGIC insert into joaopereira.premier_league_data_2022_v2 values('Benfica','99',null,'0','0');
# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC             SELECT 
# MAGIC                 MIN(CASE
# MAGIC                     WHEN SRC.FIELD_1 = DEST.FIELD_1 THEN 'OK'
# MAGIC                     ELSE 'NOT OK'
# MAGIC                 END) AS FIELD_1
# MAGIC             ,SRC.clube
# MAGIC             FROM ( 
# MAGIC                 SELECT classificacao AS FIELD_1,clube 
# MAGIC                 FROM joaopereira.premier_league_data_2022
# MAGIC                 WHERE
# MAGIC                     1 = 1
# MAGIC                 
# MAGIC             ) SRC
# MAGIC             LEFT JOIN (
# MAGIC                 SELECT classificacao AS FIELD_1
# MAGIC                 FROM iliyan.premier_league_data_2022
# MAGIC                 WHERE
# MAGIC                     1 = 1
# MAGIC                 
# MAGIC             ) DEST
# MAGIC             ON
# MAGIC                 SRC.FIELD_1 = DEST.FIELD_1
# MAGIC             GROUP BY SRC.clube;
# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC             SELECT 
# MAGIC                 MIN(CASE
# MAGIC                     WHEN SRC.FIELD_1 = DEST.FIELD_1 THEN 'OK'
# MAGIC                     ELSE 'NOT OK'
# MAGIC                 END) AS FIELD_1
# MAGIC                 ,SRC.clube as teste
# MAGIC             
# MAGIC             FROM ( 
# MAGIC                 SELECT clube AS FIELD_1,clube
# MAGIC                 FROM joaopereira.premier_league_data_2022
# MAGIC                 WHERE
# MAGIC                     1 = 1
# MAGIC                 
# MAGIC             ) SRC
# MAGIC             LEFT JOIN (
# MAGIC                 SELECT clube AS FIELD_1
# MAGIC                 FROM joaopereira.premier_league_data_2022_v3
# MAGIC                 WHERE
# MAGIC                     1 = 1
# MAGIC                 
# MAGIC             ) DEST
# MAGIC             ON
# MAGIC                 SRC.FIELD_1 = DEST.FIELD_1
# MAGIC             GROUP BY clube
# MAGIC         
# MAGIC         
# MAGIC         
# MAGIC         
# MAGIC         
# MAGIC