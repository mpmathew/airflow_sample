USE SCHEMA TEST_DEV_DB.TEST_SCHEMA;

CREATE OR REPLACE FUNCTION addone(i int)
RETURNS INT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'addone_py'
as
$$
def addone_py(i):
  return i+1
$$;

SELECT addone(3);
