import bot_commands
import db_helper
import utils
from globals import *

import discord
from discord.commands import option
from discord.ext import commands, tasks
from dotenv import load_dotenv

import os
import logging

# Load environment variables
load_dotenv()
TOKEN = os.getenv("TOKEN")

# Get logger for current module
logger = logging.getLogger(__name__)

# Create bot with default intents
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="/", intents=intents)

################################################################################
# BOT EVENTS
################################################################################

@bot.event
async def on_ready():
    """
    Event that prints the bot's username and user ID to the console when the bot
    successfully logs in to Discord.

    Parameters:
        none

    Returns:
        none
    """

    print(f'Logged in as {bot.user.name} - {bot.user.id}')
    print('------')

    generate_rankings_task.start()
    # update_difficulty_indices_task.start()

    # # Wait for 1 hour after bot logs on
    # await asyncio.sleep(3600)
    sync_spreadsheet_with_database_task.start()

################################################################################
# BOT HELPER FUNCTIONS
################################################################################

async def get_player_names(ctx: discord.AutocompleteContext):
    query = f"""
        SELECT player_name
        FROM {PLAYERS_TABLE}
    """
    return [item[0] for item in db_helper.select(query)]

################################################################################
# BOT SLASH COMMANDS
################################################################################

@bot.slash_command(name="pick18", description="Picks a random 18-hole course.")
async def pick18(ctx):
    """
    Slash command to pick a random 18-hole golf course.

    Parameters:
        ctx: The context of the command.

    Returns:
        none
    """

    await bot_commands.pick18(ctx)

@bot.slash_command(name="pick9", description="Picks a random 9-hole course.")
async def pick9(ctx):
    """
    Slash command to pick a random 9-hole course.

    Parameters:
        ctx: The context of the command.

    Returns:
        none
    """

    await bot_commands.pick9(ctx)


@bot.slash_command(name="rankings", description="Get the rankings sheet.")
async def get_rankings(ctx):
    """
    Slash command to get a link to the server rankings sheet.

    Parameters:
        ctx: The context of the command.

    Returns:
        none
    """

    await bot_commands.get_rankings_sheet(ctx)


@bot.slash_command(name="change_name", description="Change your name on the leaderboard.")
@option("new_name", description="Enter your new name")
async def change_name(ctx, new_name):
    """
    Slash command to change a player's name on the leaderboard.

    Parameters:
        ctx: The context of the command.
        new_name: The player's new name

    Returns:
        none
    """

    await bot_commands.change_player_display_name(ctx, new_name)


@bot.slash_command(name="top10", description="Get the top 10 players in the rankings.")
async def get_top_10(ctx):
    """
    Slash command to create and post the current top 10 players in the rankings.

    Parameters:
        ctx: The context of the command.

    Returns:
        none
    """
    await bot_commands.get_top_10_table(ctx)


@bot.slash_command(name="difficulty_indices", description="Get the difficulty indices for each course.")
async def get_difficulty_indices(ctx):
    """
    Slash command to create and post the current difficulty indices for each course.

    Parameters:
        ctx: The context of the command.

    Returns:
        none
    """
    await bot_commands.get_difficulty_indices_table(ctx)


@bot.slash_command(name="profile", description="Get a player's profile.")
@option("player_id", description="Enter the Discord ID of the player. Leave blank to get your profile.", required=False)
async def get_profile(ctx, player_id):
    """
    Slash command to create and post the current player's profile.

    Parameters:
        ctx: The context of the command.

    Returns:
        none
    """

    await bot_commands.get_player_profile(ctx, player_id)


@bot.slash_command(name="recent_scores", description="Get your 40 most recent scores.")
@option("player_id", description="Enter the Discord ID of the player. Leave blank to get your recent scores.", required=False)
async def get_recent_scores(ctx, player_id):
    """
    Slash command to create and post the current player's most 40 recent scores.

    Parameters:
        ctx: The context of the command.

    Returns:
        none
    """

    await bot_commands.get_recent_score_table(ctx, player_id)


@bot.slash_command(name="submit_score", description="Submit a score into into the queue to be verified.")
@option("course", choices=COURSES, description="Enter the course")
@option("nine", choices=NINES, description="Enter the nine")
@option("character", choices=CHARACTERS, description="Enter the character")
@option("score", description="Enter the score in terms of +/- par")
async def submit_score(ctx, course, nine, character, score: int):
    """
    Slash command to submit a golf score for verification.

    Parameters:
        ctx: The context of the command.
        course (str): The name of the golf course.
        nine (str): The nine played.
        character (str): The character associated with the score.
        score (int): The golf score achieved.

    Returns:
        none
    """

    await bot_commands.add_score_to_queue(ctx, course, nine, character, score)


@bot.slash_command(name="verify", description="(Moderators only). Verify a score for a pending round.")
@option("round_id", description="Enter the round ID")
async def verify(ctx, round_id):
    """
    Slash command to verify a player's score for a pending round.

    Parameters:
        ctx: The context of the command.
        round_id (str): The hash of the pending round.

    Returns:
        none
    """

    await bot_commands.verify_score(ctx, round_id)


@bot.slash_command(name="remove_score", description="(Moderators only). Remove a pending score from the queue.")
@option("round_id", description="Enter the round ID")
async def remove_score(ctx, round_id):
    """
    Slash command to remove a pending score from the queue.

    Parameters:
        ctx: The context of the command.
        round_id (str): The hash of the pending round to be removed.

    Returns:
        none
    """

    await bot_commands.remove_score_from_queue(ctx, round_id)


@bot.slash_command(name="sync_spreadsheet", description="(Moderators only). Syncs scores spreadsheet with database.")
async def sync_spreadsheet(ctx):
    """
    Slash command to sync the spreadsheet with the database.

    Parameters:
        ctx: The context of the command.

    Returns:
        none
    """

    await bot_commands.sync_spreadsheet_with_database(ctx)


@bot.slash_command(name="update_database", description="(Moderators only). Updates scores using spreadsheet, then recalculates player ratings.")
async def update_database(ctx):
    """
    Slash command to update the scores in the database to match the spreadsheet.

    Parameters:
        ctx: The context of the command.

    Returns:
        none
    """

    await bot_commands.update_database_from_spreadsheet(ctx)

################################################################################
# BOT TASKS
################################################################################

@tasks.loop(hours=12.0)
async def generate_rankings_task():
    """
    Task that generates rankings and updates the rankings spreadsheet.

    This task runs every 12 hours and updates the rankings sheet with the latest
    data.

    Parameters:
        none

    Returns:
        none
    """

    bot_commands.generate_rankings_table()


@tasks.loop(hours=12.0)
async def sync_spreadsheet_with_database_task():

    logger.info("Syncing spreadsheet with database...")

    try:
        utils.fill_db_spreadsheet()
        logger.info("Synced spreadsheet with database.")
    except:
        logger.error("An error occured while syncing the spreadsheet. Please try again later.")

################################################################################
# BOT INITIALIZATION FUNCTION
################################################################################

def run_bot():
    """
    Initializes and runs the Discord bot with the specified token.

    Parameters:
        none

    Returns:
        none
    """

    bot.run(TOKEN)
