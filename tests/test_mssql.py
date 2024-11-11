import pyodbc
import pytest
from deo.mssql import insert, dict_to_mssql, update


def test_insert_query():
    """
    Testing to make sure that insert query for MSSQL is generated correctly.
    """
    data = {
        "num_col": [1, 2, 3],
        "str_col": ["a", "b", "c"],
        "bool_col": [True, False, True],
        "dt_col": ["2021-01-01", "2021-01-02", "2021-01-03"],
    }

    # testing quey generation for default schema and non-default schema
    assert (
        insert(data, "mssql")
        == """INSERT INTO [dbo].[mssql] \n([num_col], [str_col], [bool_col], [dt_col]) \nVALUES (?, ?, ?, ?)"""
    )
    assert (
        insert(data, "mssql", "sch")
        == """INSERT INTO [sch].[mssql] \n([num_col], [str_col], [bool_col], [dt_col]) \nVALUES (?, ?, ?, ?)"""
    )


def test_dict_insert():
    """
    Testing to make sure that data is inserted into MSSQL using insert query.
    """
    data = {
        "num_col": [1, 2, 3],
        "str_col": ["a", "b", "c"],
        "bool_col": [True, False, True],
        "dt_col": ["2021-01-01", "2021-01-02", "2021-01-03"],
    }

    query = insert(data, "mssql")
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=deo;Trusted_Connection=yes;"

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE dbo.mssql")
        conn.commit()

    dict_to_mssql(connection_string=conn_str, data_dict=data, query=query)

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM dbo.mssql")

        assert cursor.fetchone()[0] == 3


def test_update_query():
    '''
    Testing to make sure that update query for MSSQL is generated correctly.
    '''
    data = {
        "num_col": [1, 2, 3],
        "str_col": ["a", "b", "c"],
        "bool_col": [True, False, True],
        "dt_col": ["2021-01-01", "2021-01-02", "2021-01-03"],
    }
    
    assert (
        update(data, "mssql", ["num_col"])
        == """UPDATE [dbo].[mssql] \nSET [str_col] = ?, [bool_col] = ?, [dt_col] = ? \nWHERE [num_col] = ?;"""
    )
    assert (
        update(data, "mssql", ["num_col", "str_col"])
        == """UPDATE [dbo].[mssql] \nSET [bool_col] = ?, [dt_col] = ? \nWHERE [num_col] = ? AND [str_col] = ?;"""
    )
    
if __name__ == "__main__":
    pytest.main([__file__])