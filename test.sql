USE SCHEMA TEST_DEV_DB.TEST_SCHEMA;

CREATE TABLE IF NOT EXISTS my_table (
    id INT,
    first_name STRING,
    last_name STRING
);

INSERT INTO my_table VALUES
    (1, 'John', 'Doe'),
    (2, 'Jane', 'Smith');
	
CREATE OR REPLACE FUNCTION concatenate_names(first_name STRING, last_name STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
    // JavaScript code for concatenating names
    function concatenate_names(first_name, last_name) {
        return first_name + ' ' + last_name;
    }
$$;

SELECT
    id,
    first_name,
    last_name,
    concatenate_names(first_name, last_name) AS full_name
FROM
    my_table;
