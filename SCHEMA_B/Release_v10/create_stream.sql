USE {{ params.schema_name }};

CREATE OR REPLACE STREAM my_stream ON TABLE my_table;
