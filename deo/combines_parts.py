import os
from glob import iglob
import pyarrow.parquet as pq
from loguru import logger


def file_generator(find_files: str):
    """
    Generates file paths that match the given pattern.

    Args:
        find_files (str): The pattern to match file paths.

    Yields:
        str: File paths that match the given pattern.
    """
    return iglob(find_files)


def combine_parquets(
    file_base: str, keep_parts: bool = True, part_count: int = None, validate: list = []
) -> None:
    """
    This function takes Parquet files with the same base and combines them into a single Parquet file.

    Parameters:
    - file_base (str): The base name of the Parquet files to combine.
    - keep_parts (bool): Whether to keep the individual part files after combining. Default is True.
    - part_count (int, optional): The expected number of part files. If provided, the function will validate the count.
    - validate (list, optional): A list containing two elements [rows, columns] to validate the combined Parquet file.

    The function performs the following steps:
    1. Generates a list of part files based on the file_base.
    2. Combines the part files into a single output Parquet file.
    3. Validates the number of part files if part_count is provided.
    4. Validates the combined Parquet file's row and column count if validate is provided.
    5. Deletes the individual part files if keep_parts is set to False.
    """

    file_counter = 0

    output_file = f"{file_base}_combined.parquet"

    parts = file_generator(f"{file_base}*.parquet")

    # Get the schema from the first file
    first_file = next(parts)
    schema = pq.ParquetFile(first_file).schema_arrow

    # Create the ParquetWriter with the schema
    with pq.ParquetWriter(output_file, schema=schema) as writer:
        # Write the first file
        writer.write_table(pq.read_table(first_file, schema=schema))
        file_counter += 1

        # Write the remaining files
        for file in parts:
            writer.write_table(pq.read_table(file, schema=schema))
            file_counter += 1

    if part_count:
        assert (
            part_count == file_counter
        ), f"Expected {part_count} files but found {file_counter} files."
        logger.success(
            f"Expected part_count matches the number of files found.  {part_count} files found."
        )

    if validate:
        if len(validate) != 2:
            raise ValueError(
                "The validate list must contain 2 elements: [rows, columns]"
            )
        rows = validate[0]
        columns = validate[1]
        logger.info(
            f"Validating the combined file {output_file} against the following: Rows - {rows:,}, Columns - {columns}"
        )

    if not keep_parts:
        parts = file_generator(f"{file_base}*.parquet")

        logger.info("Deleting individual parts.")

        # Delete the individual parts
        for part in parts:
            if "_combined" in part:
                continue
            os.remove(part)

    logger.success(
        f"{file_counter:,} parquet files combined and written to {output_file}"
    )
