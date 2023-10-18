"""
sheets_helper.py

This module provides functions to perform various operations with Google Sheets,
such as writing data, retrieving data, and clearing data.
"""

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

import logging
import os

# Get logger for current module
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

SERVICE_ACCOUNT_KEY_PATH = os.getenv("SERVICE_ACCOUNT_KEY_PATH")
LEADERBOARD_SPREADSHEET_ID = os.getenv("LEADERBOARD_SPREADSHEET_ID")
DB_SPREADSHEET_ID = os.getenv("DB_SPREADSHEET_ID")

scopes = ["https://www.googleapis.com/auth/spreadsheets"]

# Load service account credentials from JSON key file
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_KEY_PATH, scopes=scopes)

# Create a service client
service = build("sheets", "v4", credentials=credentials)


def calculate_range(starting_cell, values):

    start_col, start_row = starting_cell[0], int(starting_cell[1:])
    end_col = chr(ord(start_col) + len(values[0]) - 1)
    end_row = start_row + len(values) - 1
    return f"{starting_cell}:{end_col}{end_row}"


def write_data(SPREADSHEET_ID, values, sheet_name, starting_cell):

    range_name = calculate_range(starting_cell, values)
    body = {"values": values}
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!{range_name}",
        valueInputOption="RAW",
        body=body
    ).execute()


def get(SPREADSHEET_ID, range_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_name
    ).execute()
    values = result.get('values', [])
    return values


def clear(SPREADSHEET_ID, sheet_name, range_name):

    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!{range_name}"
    ).execute()
