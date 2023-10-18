import db_helper
import sheets_helper
from sheets_helper import DB_SPREADSHEET_ID
from globals import *

from discord import utils
from dotenv import load_dotenv

from datetime import datetime
import os
import logging

# Get logger for current module
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
MODERATOR_ROLE_ID = os.getenv("MODERATOR_ROLE_ID")


def get_moderator_role(ctx):
    """
    Get the moderator role from the context's guild.

    Parameters:
        ctx: The context of the command.

    Returns:
        discord.Role or None: The moderator role if found, None otherwise.
    """

    return utils.get(ctx.guild.roles, id=int(MODERATOR_ROLE_ID))


def fill_db_spreadsheet():
    try:
        query = f"""
            SELECT timestamp, course_id, player_id, character, score
            FROM {SCORES_TABLE}
            ORDER BY round_id;
        """
        sheet = [list(row) for row in db_helper.select(query)]

        # Clear the spreadsheet before writing
        sheets_helper.clear(DB_SPREADSHEET_ID, "Scores", "A2:F")

        # Change every player ID to a string so it doesn't get truncated by the sheet
        for i in range(len(sheet)):
            sheet[i][2] = str(sheet[i][2])
        header = ("timestamp", "course_id", "player_id", "character", "score")
        sheet.insert(0, header)
        sheets_helper.write_data(DB_SPREADSHEET_ID, sheet, "Scores", "A1")

        now = datetime.utcnow()
        formatted_time = now.strftime("%m/%d/%Y %H:%M:%S")
        last_updated_msg = f"Last sync (UTC): {formatted_time}"
        sheets_helper.write_data(
            DB_SPREADSHEET_ID, [[last_updated_msg]], "Scores", "F1")
    except Exception as e:
        print(e)
        raise