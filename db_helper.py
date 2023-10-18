from dotenv import load_dotenv
import psycopg2
from psycopg2 import extras

import logging
import os

# Get logger for current module
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Connect to database
db_params = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}
connection = psycopg2.connect(**db_params)


def select(query, values=None):
    """
    Execute a SELECT query on the database and retrieve the results.

    Parameters:
        query (str): The query to be executed.
        values (tuple): The values to be used for the query (optional).

    Returns:
        list: If the query was a SELECT query
        None: If the query was not a SELECT query

    Raises:
        Exception: If there is an error during the database interaction.
    """

    results = None

    try:
        cursor = connection.cursor()

        if values is None:
            cursor.execute(query)
        else:
            cursor.execute(query, values)

        # Check if query is a SELECT query
        if cursor.description:
            results = cursor.fetchall()

        connection.commit()
    except (Exception, psycopg2.Error) as error:
        logger.error(error)
        connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
    return results


def insert_single(query, values):
    """
    Execute a single-row INSERT query with the provided values.

    Parameters:
        query (str): The query to be executed.
        values (tuple): The values to be inserted into the database.

    Raises:
        Exception: If there is an error during the database interaction.
    """

    try:
        cursor = connection.cursor()
        cursor.execute(query, values)
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        logger.error(error)
        connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()


def insert_multiple(query, values):
    """
    Execute a multi-row INSERT query with the provided values.

    Parameters:
        query (str): The query to be executed.
        values (list of tuples): List of tuples containing values for multiple rows.

    Raises:
        Exception: If there is an error during the database interaction.
    """

    try:
        cursor = connection.cursor()
        extras.execute_values(cursor, query, values)
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        logger.error(error)
        connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()


def delete(query, values=None):
    """
    Execute a DELETE query with the provided values.

    Parameters:
        query (str): The query to be executed.
        values (tuple): The values to be used for the query (optional).

    Raises:
        Exception: If there is an error during the database interaction.
    """

    try:
        cursor = connection.cursor()

        if values is None:
            cursor.execute(query)
        else:
            cursor.execute(query, values)

        connection.commit()
    except (Exception, psycopg2.Error) as error:
        logger.error(error)
        connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()


def update_single(query, values):
    """
    Execute a multi-row UPDATE query with the provided values.

    Parameters:
        query (str): The query to be executed.
        values (list): List of lists containing the data to be updated.

    Raises:
        Exception: If there is an error during the database interaction.
    """

    try:
        cursor = connection.cursor()
        cursor.execute(query, values)
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        logger.error(error)
        connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()


def update_multiple(query, values):
    """
    Execute a multi-row UPDATE query with the provided values.

    Parameters:
        query (str): The query to be executed.
        values (list): List of lists containing the data to be updated.

    Raises:
        Exception: If there is an error during the database interaction.
    """

    try:
        cursor = connection.cursor()
        extras.execute_values(cursor, query, values)
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        logger.error(error)
        connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
