import pyodbc


def insert(data_dict:dict, table_name: str, schema_name: str = "dbo") -> str:
    """
    This function generates an Insert query for Microsoft SQL Server based on a Python
    dictionary as an input.
    """
    assert isinstance(data_dict, dict), "data_dict must be a dictionary."
    assert isinstance(table_name, str), "table_name must be a string."
    
    # Get column names and wrap each in []
    columns = list(data_dict.keys())
    columns_str = ", ".join(f"[{col}]" for col in columns)

    # Create a placeholder string for values ('?, ?, ?' for each column)
    placeholders = ", ".join("?" for _ in columns)

    # Prepare the SQL INSERT query
    query = f"INSERT INTO [{schema_name}].[{table_name}] \n({columns_str}) \nVALUES ({placeholders})"

    return query


def update(
    data_dict:dict,
    table_name: str,
    id_columns: list,
    schema_name: str = "dbo",
) -> str:
    assert isinstance(id_columns, list), "id_columns must be a list of column names."
    assert len(id_columns) > 0, "id_columns must contain at least one column name."
    assert isinstance(data_dict, dict), "data_dict must be a dictionary."
    assert isinstance(table_name, str), "table_name must be a string."
    
    columns = [column for column in data_dict.keys() if column not in id_columns]
    wrapped_columns = [f"[{col}]" for col in columns]

    # Create the SET clause with placeholders
    set_clause = ", ".join([f"{col} = ?" for col in wrapped_columns])

    # Create the WHERE clause with placeholders for the id_columns
    where_clause = " AND ".join([f"[{col}] = ?" for col in id_columns])

    # Construct the full update query
    query = f"UPDATE [{schema_name}].[{table_name}] \nSET {set_clause} \nWHERE {where_clause};"

    return query


def dict_to_mssql(
    connection_string: str, data_dict, query: str, batch_size: int = 1000
) -> None:
    """
    This function executes a given query in Microsoft SQL Server based on a Python
    dictionary as an input. This function is primarily for batch processing.

    if using this wth polars use data_dict = df.to_dict()
    """
    assert isinstance(connection_string, str), "connection_string must be a string."
    assert isinstance(data_dict, dict), "data_dict must be a dictionary."
    assert isinstance(query, str), "query must be a string."
    
    if not data_dict:
        raise Exception("Data Dict is Empty. Cannot execute query with no data.")

    # Ensure all lists in the dictionary have the same length
    length = len(next(iter(data_dict.values())))
    for key, values in data_dict.items():
        if len(values) != length:
            raise ValueError(
                f"All columns must have the same number of rows. Column '{key}' has a different length."
            )

    columns = list(data_dict.keys())
    # Extract the rows to be inserted by zipping the lists
    values = list(zip(*[data_dict[col] for col in columns]))

    def process_batch(batch):
        try:
            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()
                cursor.fast_executemany = True
                cursor.executemany(query, batch)
                conn.commit()
        except pyodbc.Error as e:
            raise Exception(f"Error processing data: {e}")

    # Insert batches sequentially
    for i in range(0, len(values), batch_size):
        process_batch(values[i : i + batch_size])
