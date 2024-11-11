from datetime import datetime, date
import polars as pl
import pandas as pd


def current_fiscal_year(start_month=10) -> int:
    """
    Calculate the current fiscal year based on the given start month.

    Args:
        start_month (int): The month in which the fiscal year starts. Default is October (10).

    Returns:
        int: The start year of the current fiscal year.

    Example:
        If the current date is November 2023 and the start month is October, the fiscal year would be 2023.
    """
    now = datetime.now()
    current_year = now.year

    # If current month is before the start of the fiscal year, use the previous year as the fiscal year start
    if now.month < start_month:
        fiscal_year = current_year - 1
    else:
        fiscal_year = current_year

    return fiscal_year


def get_fiscal_year(date, start_month=10) -> int:
    """
    Calculate the fiscal year for a given date based on the given start month.

    Args:
        date (datetime): The date for which to calculate the fiscal year.
        start_month (int): The month in which the fiscal year starts. Default is October (10).

    Returns:
        int: The start year of the fiscal year for the given date.

    Example:
        If the date is November 2023 and the start month is October, the fiscal year would be 2023.
    """
    current_year = date.year

    # If the month of the date is before the start of the fiscal year, use the previous year as the fiscal year start
    if date.month < start_month:
        fiscal_year = current_year - 1
    else:
        fiscal_year = current_year

    return fiscal_year


def get_fiscal_quarter(date, fiscal_start_month) -> int:
    """
    Calculate the fiscal quarter for a given date based on the fiscal year start month.

    Args:
        date (datetime.date): The date for which to determine the fiscal quarter.
        fiscal_start_month (int): The month (1-12) when the fiscal year starts.

    Returns:
        int: The fiscal quarter (1-4) for the given date.
    """
    month = (date.month - fiscal_start_month) % 12 + 1
    return (month - 1) // 3 + 1

# TODO: Add type hints and docstrings
# TODO: Add tests
# TODO: Fix fiscal year and quarter calculation
def create_date_dimension(start_date, end_date) -> pl.DataFrame:
    """Creates a date dimension dataframe.

    Args:
        start_date (date): Start date object.
        end_date (date): End date object.

    Returns:
        pl.DataFrame: Date dimension Polars DataFrame.
    """
    
    assert isinstance(start_date, date), "start_date must be a date object."
    assert isinstance(end_date, date), "end_date must be a date object."
    
    if start_date > end_date:
        raise ValueError("Start date must be before end date.")
    
    dim_date = pl.date_range(start_date, end_date, "1d", eager=True).alias("date").to_frame()
    
    dim_date = dim_date.with_columns(
        pl.col("date").dt.strftime("%Y%m%d").alias("date_id"),
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.is_leap_year().alias("is_leap_year"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.strftime('%B').alias("month_name"),
        pl.col("date").dt.month_start().alias("month_start"),
        pl.col("date").dt.month_end().alias("month_end"),
        pl.col("date").dt.strftime("%b").alias("month_abbr"),
        pl.col("date").dt.strftime("%Y%m").alias("year_month"),
        pl.col("date").dt.day().alias("day_of_month"),
        pl.col("date").dt.weekday().alias("weekday"),
        pl.col("date").dt.ordinal_day().alias("day_of_year"),
        pl.col("date").dt.strftime("%A").alias("day_name"),
        pl.col("date").dt.strftime("%a").alias("day_abbr"),
        pl.col("date").dt.week().alias("week_of_year"),
        pl.col("date").dt.quarter().alias("quarter"),
        pl.col("date").dt.weekday().gt(5).alias("is_weekend"),
        # pl.col("date").map_elements(lambda x: x.year if x.month >= fiscal_year_start else x.year - 1, return_dtype=pl.Int64).alias("fiscal_year"),
        # (((pl.col("date").dt.month() - fiscal_year_start) % 12) + 1).alias("fiscal_quarter")
    ) 

    return dim_date


def generate_date_ranges(start_date, end_date, freq="D", string_fmt="%Y/%m/%d"):
    # Generate date ranges
    date_ranges = pd.date_range(start=start_date, end=end_date, freq=freq)

    # Check if date_ranges is empty and handle the case
    if date_ranges.empty:
        yield (
            pd.to_datetime(start_date).strftime(string_fmt),
            pd.to_datetime(end_date).strftime(string_fmt),
        )
        return

    # Iterate through the ranges and yield start-end pairs
    for i in range(len(date_ranges) - 1):
        start = date_ranges[i]
        end = date_ranges[i + 1]
        yield (start.strftime(string_fmt), end.strftime(string_fmt))

    # Handle the last range (final interval to the exact end_date)
    if date_ranges[-1] < pd.to_datetime(end_date):
        yield (
            date_ranges[-1].strftime(string_fmt),
            pd.to_datetime(end_date).strftime(string_fmt),
        )
