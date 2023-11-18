"""
bot_commands.py

This module implements the core functionality of the bot, including submitting
scores to the database, calculating adjusted scores and player ratings.
"""

import db_helper
import sheets_helper
from sheets_helper import LEADERBOARD_SPREADSHEET_ID, DB_SPREADSHEET_ID
from globals import *
import table_generation
import utils

import discord

from collections import defaultdict
from datetime import datetime
import math
import random
import logging
import string

# Get logger for current module
logger = logging.getLogger(__name__)

################################################################################
# BOT USER COMMANDS
################################################################################

def pick18(ctx):
    """
    Pick a random 18-hole golf course.

    Parameters:
        ctx: The context of the command.

    Returns:
        str: The chosen golf course.
    """

    course = random.choice(COURSES)
    return ctx.respond(f"{course}")


def pick9(ctx):
    """
    Pick a random 9-hole golf course.

    Parameters:
        ctx: The context of the command.

    Returns:
        str: The chosen nine.
    """

    course = random.choice(COURSES)
    nine = random.choice(NINES)
    return ctx.respond(f"{course} ({nine})")
    

def get_rankings_sheet(ctx):
    
    message = f"Mario Golf 64 Netplay Server Rankings: https://docs.google.com/spreadsheets/d/{LEADERBOARD_SPREADSHEET_ID}/htmlview#"
    return ctx.respond(message)


def change_player_display_name(ctx, new_name):
    """
    Change the display name of a player in a game database.

    Parameters:
        ctx: The context of the command.
        new_name: The new display name.

    Returns:
        str: A message indicating the result of the name change operation.
    """

    if (len(new_name) > 32):
        return ctx.respond("Error: Name is too long. Name change not applied.")
    
    player_id = ctx.author.id

    # Get all player IDs from the database
    query = f"""
        SELECT discord_id
        FROM {PLAYERS_TABLE}
    """
    player_ids = [item[0] for item in db_helper.select(query)]

    if player_id not in player_ids:
        return ctx.respond("You must have at least one score verified to change your name.")
    
    # Update display name in database with the new name
    update_query = f"""
        UPDATE {PLAYERS_TABLE} p
        SET player_name = %s
        WHERE p.discord_id = %s;
    """
    db_helper.update_single(update_query, (new_name, player_id))

    return ctx.respond(f"You changed your name to {new_name}.")


def get_hash():
    """
    Generate a random 16 character alphanumeric hash.

    Parameters:
        none

    Returns:
        str: A randomly generated hash.
    """

    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))


def get_course_id(course, nine):
    """
    Retrieve the ID of a golf course based on its details.

    Parameters:
        course (str): The name of the golf course.
        nine (str): The nine played.

    Returns:
        int: The ID of the golf course if found.

    Raises:
        ValueError: If the course details are not found in the database.
    """

    query = f"""
        SELECT course_id FROM {COURSES_TABLE} 
        WHERE course_name = %s AND nine = %s
    """
    values = (course, nine)
    result = db_helper.select(query, values)
    if result:
        return result[0][0]
    else:
        raise ValueError("Course details not found.")


def add_score_to_queue(ctx, course, nine, character, score):
    """
    Add a golf score entry to the verification queue.

    Parameters:
        ctx: The context of the command.
        course (str): The name of the golf course.
        nine (str): The nine played.
        character (str): The character associated with the score.
        score (int): The golf score achieved.

    Returns:
        str: A message indicating the status of the operation.

    Raises:
        ValueError: If there is an issue with the provided values.
        Exception: If there is an error during the database interaction.
    """

    player_name = ctx.author.name
    player_id = ctx.author.id
    current_datetime = datetime.utcnow()
    timestamp = int(current_datetime.timestamp())
    new_hash = get_hash()

    try:
        course_id = get_course_id(course, nine)
        query = f"""
            INSERT INTO {PENDING_SCORES_TABLE} (timestamp, hash, course_id, player_id, player_name, character, score)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        values = (timestamp, new_hash, course_id, player_id, player_name, character, score)
        db_helper.insert_single(query, values)

        # Make it so it always shows the sign of the score
        score_str = str(score)
        if score == 0:
            score_str = f"±{score}"
        if score > 0:
            score_str = f"+{score}"

        moderator_role = utils.get_moderator_role(ctx)
        message = f"{moderator_role.mention} New score to be verified: `{player_name}: {character}, {score_str} @ {course} ({nine})` (Round ID: {new_hash})"
        return ctx.respond(message)
    except ValueError as ve:
        return ctx.respond(f"Error: {ve}")
    except Exception as e:
        return ctx.respond(f"Error: {e}")


def is_queue_empty():
    """
    Check if the pending scores queue is empty.

    Parameters:
        none

    Returns:
        bool: True if the queue is empty, False otherwise.

    Raises:
        Exception: If there is an error during the database interaction.
    """

    query = f"""
        SELECT NOT EXISTS (
            SELECT 1 
            FROM {PENDING_SCORES_TABLE} 
            LIMIT 1
        );
    """
    try:
        empty = db_helper.select(query)[0][0]
        return empty
    except Exception:
        # Assume queue is empty
        return True


def get_pending_round(hash):
    """
    Retrieve pending round details using the round's hash.

    Parameters:
        hash (str): The hash of the pending round.

    Returns:
        tuple or None: A tuple containing round details (timestamp, course_id,
            player_id, character, score) if found, None otherwise.

    Raises:
        Exception: If there is an error during the database interaction.
    """

    query = f"""
        SELECT timestamp, course_id, player_id, character, score, player_name
        FROM {PENDING_SCORES_TABLE}
        WHERE hash = %s
        LIMIT 1;
    """
    try:
        pending_round = db_helper.select(query, (hash,))
        return pending_round[0] if pending_round else None
    except Exception:
        return None


def get_difficulty_indices():
    """
    Retrieve difficulty indices of all courses.

    Parameters:
        none

    Returns:
        list: The difficulty indices for all courses.

    Raises:
        Exception: If there is an error during the database interaction.
    """

    query = f"""
        SELECT difficulty_index
        FROM {COURSES_TABLE}
        ORDER BY course_id;
    """
    try:
        difficulty_indices = [element[0]
                              for element in db_helper.select(query)]
        return difficulty_indices
    except Exception:
        return []


def verify_score(ctx, hash):
    """
    Verify a player's score for a pending round.

    Parameters:
        ctx: The context of the command.
        hash (str): The hash of the pending round.

    Returns:
        str: A message indicating the result of the verification.

    Raises:
        Exception: If there is an error during the verification process.
    """

    moderator_role = utils.get_moderator_role(ctx)
    if moderator_role not in ctx.author.roles:
        return ctx.respond("You don't have permission to use this command.")

    try:
        if is_queue_empty():
            return ctx.respond("Queue is currently empty.")

        pending_round = get_pending_round(hash)
        if not pending_round:
            return ctx.respond("ID not found in queue.")

        timestamp, course_id, player_id, character, score, player_name = pending_round

        # Calculate adjusted score
        difficulty_indices = get_difficulty_indices()
        difficulty_index = difficulty_indices[course_id - 1]
        adjusted_score = float(score) - difficulty_index

        # Insert score into database
        insert_query = f"""
            INSERT INTO {SCORES_TABLE} (timestamp, course_id, player_id, character, score, adjusted_score)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        values = (timestamp, course_id, player_id, character, score, adjusted_score)
        db_helper.insert_single(insert_query, values)

        # Delete from pending queue
        delete_query = f"""
            DELETE FROM {PENDING_SCORES_TABLE}
            WHERE hash = %s;
        """
        db_helper.delete(delete_query, (hash,))
        
        # Get all adjusted scores for the given player
        select_query = f"""
            SELECT adjusted_score
            FROM {SCORES_TABLE}
            WHERE player_id = %s
            ORDER BY timestamp;
        """
        scores = [entry[0] for entry in db_helper.select(select_query, (player_id,))]

        # Calculate new rating
        new_rating = calculate_player_rating(scores)

        # Update score entry with new rating
        update_query = f"""
            UPDATE {SCORES_TABLE}
            SET rating = %s
            WHERE player_id = %s AND timestamp = %s;
        """
        db_helper.update_single(update_query, (new_rating, player_id, timestamp))

        # Update ratings table
        ratings_insert_query = f"""
            INSERT INTO {PLAYERS_TABLE} (discord_id, player_name, rating)
            VALUES (%s, %s, %s)
            ON CONFLICT (discord_id)
            DO UPDATE SET
                rating = excluded.rating;
        """
        db_helper.insert_single(ratings_insert_query, (player_id, player_name, new_rating))

        return ctx.respond(f"Successfully submitted round {hash}.")
    except Exception as e:
        return ctx.respond(f"Error: {e}")


def remove_score_from_queue(ctx, hash):
    """
    Remove a pending score from the queue.

    Parameters:
        ctx: The context of the command.
        hash (str): The hash of the pending round to be removed.

    Returns:
        str: A message indicating the result of the removal operation.

    Raises:
        Exception: If there is an error during the database interaction.
    """

    moderator_role = utils.get_moderator_role(ctx)
    if moderator_role not in ctx.author.roles:
        return ctx.respond("You don't have permission to use this command.")

    try:
        delete_query = f"""
            DELETE FROM {PENDING_SCORES_TABLE}
            WHERE hash = %s;
        """
        db_helper.delete(delete_query, (hash,))

        return ctx.respond(f"Successfully removed round {hash}.")
    except Exception as e:
        return ctx.respond(f"Error: {e}")
    

def calculate_player_rating(scores):
    """
    Calculates a player's rating based on their adjusted scores.

    This function calculates a player's rating based on their adjusted
    scores. The rating is determined by either taking the average of all scores
    if there are fewer than 40 scores available, or by taking the average of the
    most recent 40 scores if there are more than 40 scores available. If the
    player has less than the minimum required scores, then the player is
    assigned a default value for their rating. 

    Parameters:
        scores (list of int): A list of scores sorted from oldest to newest.

    Returns:
        float: The player's calculated rating based on their scores.
    """

    MIN_REQUIRED_SCORES = 6
    ROLLING_AVERAGE_WINDOW = 40

    if len(scores) < MIN_REQUIRED_SCORES:
        # Not enough scores for a rating, use default value
        return INVALID_RATING
    elif len(scores) < ROLLING_AVERAGE_WINDOW:
        # Calculate rating as average of all scores
        return sum(scores) / len(scores)
    else:
        most_recent_scores = scores[-ROLLING_AVERAGE_WINDOW:]
        # Calculate rating as average of scores in the rolling window
        return sum(scores[-ROLLING_AVERAGE_WINDOW:]) / ROLLING_AVERAGE_WINDOW


def update_adjusted_scores():
    """
    Update adjusted golf scores in the database.

    This function updates all adjusted scores in the database for all players.

    Parameters:
        none

    Returns:
        none
    """

    # Retrieve round info for specified players and difficulty indices for all
    # courses, formatted into lists
    score_query = f"""
        SELECT round_id, course_id, score, adjusted_score
        FROM {SCORES_TABLE};
    """
    scores = [list(entry) for entry in db_helper.select(score_query)]

    # Recalculate all adjusted scores
    indices = get_difficulty_indices()
    for entry in scores:
        entry[3] = entry[2] - indices[entry[1] - 1]

    # Update database with new adjusted scores
    update_query = f"""
        UPDATE {SCORES_TABLE} s
        SET adjusted_score = score.adj_score
        FROM (VALUES %s) AS score(id, course_id, score, adj_score)
        WHERE s.round_id = score.id;
    """
    db_helper.update_multiple(update_query, scores)


def update_player_ratings():
    """
    Updates player ratings based on their score history.

    This function updates player ratings by calculating them based on the
    history of their adjusted scores. It retrieves score data from a database
    table, sorts the data by timestamp, and then iteratively calculates player
    ratings for each round of scores.

    The updated ratings are then stored in the database, and the function also
    updates a table containing info for each player with the players' current
    ratings.

    Parameters:
        none

    Returns:
        none
    """

    select_query = f"""
        SELECT round_id, player_id, timestamp, course_id, adjusted_score
        FROM {SCORES_TABLE}
    """
    player_score_data = db_helper.select(select_query)

    # Dictionary to store player IDs and their corresponding score info
    score_data_dict = {}

    # Populate score data dictionary
    for entry in player_score_data:
        round_id, player_id, timestamp, course_id, adjusted_score = entry

        if player_id not in score_data_dict:
            score_data_dict[player_id] = []

        score_data_dict[player_id].append(
            [round_id, timestamp, course_id, adjusted_score, 0.0])

    player_index = 0
    current_ratings = []

    # Calculate and update player ratings based on score history
    for player_id in score_data_dict:
        score_data_dict[player_id].sort(key=lambda x: x[1])  # Sort by timestamp
        
        for round_id in range(len(score_data_dict[player_id])):
            adjusted_scores = [entry[3] for entry in score_data_dict[player_id][:round_id + 1]]
            rating = calculate_player_rating(adjusted_scores)
            score_data_dict[player_id][round_id][4] = rating

        # Get the player's rating after their most recent score and add an entry for their current rating
        current_rating = score_data_dict[player_id][-1][4]
        current_ratings.append((player_id, current_rating))

    # Flatten dictionary for database update
    flattened_score_data = [score 
                            for entry in score_data_dict.values() 
                            for score in entry]

    # Update database with new ratings
    update_query = f"""
        UPDATE {SCORES_TABLE} s
        SET rating = round.rating
        FROM (VALUES %s) AS round(id, timestamp, round_format, adj_score, rating)
        WHERE s.round_id = round.id;
    """
    db_helper.update_multiple(update_query, flattened_score_data)
    
    update_query = f"""
        UPDATE {PLAYERS_TABLE} p
        SET rating = player.rating
        FROM (VALUES %s) AS player(discord_id, rating)
        WHERE p.discord_id = player.discord_id;
    """
    db_helper.update_multiple(update_query, current_ratings)
    

def generate_rankings_table():
    """
    Generate a table of player rankings based on player ratings.

    Returns:
        list: A list containing player rankings with their IDs and ratings.

    Raises:
        Exception: If there is an error during the database interaction.
    """

    logger.info("Updating rankings...")

    now = datetime.utcnow()
    formatted_time = now.strftime("%m/%d/%Y %H:%M:%S")

    # Recalculate all adjusted scores and player ratings to ensure correctness
    update_adjusted_scores()
    update_player_ratings()

    try:
        query = f"""
            SELECT player_id, rating, player_name
            FROM {PLAYERS_TABLE};
        """
        player_data = db_helper.select(query)
    except Exception as e:
        return print(f"Error: {e}")

    # Separate players with no valid rating
    rated_players = []

    for player_id, rating, player_name in player_data:
        if rating != INVALID_RATING:
            rated_players.append([player_name, rating])

    if len(rated_players) == 0:
        logger.info("No players are currently rated.")
        logger.info("Finished updating rankings.")
        return
    
    # Sort players based on rating
    rated_players.sort(key=lambda x: x[1])

    # Assign ranks to rated players
    rankings_sheet = [[rank, player_name, rating] 
                      for rank, [player_name, rating] in enumerate(rated_players, start=1)]

    last_updated_msg = f"Last updated: {formatted_time}"
    sheets_helper.write_data(
        LEADERBOARD_SPREADSHEET_ID, rankings_sheet, "Rankings", "A4")
    sheets_helper.write_data(
        LEADERBOARD_SPREADSHEET_ID, [[last_updated_msg]],  "Rankings", "A2")
    
    logger.info("Finished updating rankings.")


def sync_spreadsheet_with_database(ctx):
    """
    Synchronize the spreadsheet with the data in the database.

    Parameters:
        ctx: The context of the command.

    Returns:
        str: A response indicating the result of the synchronization.

    Raises:
        Exception: If an error occurred when synchronizing the spreadsheet.
    """

    moderator_role = utils.get_moderator_role(ctx)
    if moderator_role not in ctx.author.roles:
        return ctx.respond("You don't have permission to use this command.")

    try:
        utils.fill_db_spreadsheet()
        return ctx.respond("Synced spreadsheet with database.")
    except Exception as e:
        return ctx.respond(f"Error syncing spreadsheet: {e}")


def check_row_valid(row):
    """
    Check the validity of a data row extracted from a spreadsheet.

    Parameters:
        row (list[str]): A row of data from the spreadsheet.

    Returns:
        bool: True if the row is valid, False otherwise.
    """
        
    if len(row) < 5:
        return False

    for element in row:
        if element == '':
            return False

    timestamp, course_id, player_id, character, score = row
    if int(course_id) > 12 or int(course_id) < 1:
        return False

    if character not in CHARACTERS:
        return False

    return True


def update_database_from_spreadsheet(ctx):
    """
    Updates a database with data from the spreadsheet.

    The function performs the following steps:
    1. Retrieves data from the Google Sheets spreadsheet.
    2. Validates the data rows using the check_row_valid() function.
    3. Deletes existing database tables and resets serials.
    4. Inserts the validated data into the database.
    5. Recalculates adjusted scores and player ratings.
    6. Updates the player table with player IDs from the spreadsheet, along with
      their display names.

    Parameters:
        ctx: The context of the command.

    Returns:
        str: A response indicating the result of the update.

    Raises:
        Exception: If an error occurred while updating the database.
    """

    moderator_role = utils.get_moderator_role(ctx)
    if moderator_role not in ctx.author.roles:
        return ctx.respond("You don't have permission to use this command.")

    try:
        data = sheets_helper.get(DB_SPREADSHEET_ID, "Scores!A2:E")

        # Check that table has no missing/invalid values
        for i in range(len(data)):
            row = data[i]
            if check_row_valid(row) is False:
                # header row + 1
                return ctx.respond(f"Error updating database: One or more elements missing/invalid at row {i + 2}.")

        # Delete rounds and players tables and reset the serials
        db_helper.delete(f"TRUNCATE TABLE {SCORES_TABLE}, {PLAYERS_TABLE} RESTART IDENTITY;")

        # Insert the data from the spreadsheet into the scores table, using a dummy value for adjusted score
        insert_data = [(int(timestamp), int(course_id), int(player_id), character, int(score), 0.0) 
                       for [timestamp, course_id, player_id, character, score] in data]
        insert_query = f"""
            INSERT INTO {SCORES_TABLE} (timestamp, course_id, player_id, character, score, adjusted_score) 
            VALUES %s;
        """
        db_helper.insert_multiple(insert_query, insert_data)

        # Recalculate all adjusted scores and player ratings to ensure correctness
        update_adjusted_scores()
        update_player_ratings()

        # Get the IDs currently in the players table
        query = f"""
            SELECT discord_id
            FROM {PLAYERS_TABLE}
        """
        current_player_ids = [elem[0] for elem in db_helper.select(query)]

        # Get the player IDs and their display names from the sheet
        data = sheets_helper.get(DB_SPREADSHEET_ID, "Players!A2:B")

        unnamed_players = []
        no_score_players = []

        for entry in data:
            player_id = int(entry[0])

            if len(entry) == 1:
                # ID does not have a name associated with it
                unnamed_players.append(player_id)
                continue

            # Get the newly entered player's current rating
            query = f"""
                SELECT rating
                FROM {SCORES_TABLE}
                WHERE (player_id, timestamp) IN (
                    SELECT player_id, MAX(timestamp) AS latest_timestamp
                    FROM {SCORES_TABLE}
                    WHERE player_id = %s
                    GROUP BY player_id
                );
            """
            rating = db_helper.select(query, (player_id,))

            if rating == []:
                no_score_players.append(player_id)
                continue

            rating = rating[0][0]
            player_name = entry[1]

            # Update the player table with the new player ID, their display name and their rating
            ratings_insert_query = f"""
                INSERT INTO {PLAYERS_TABLE} (discord_id, player_name, rating)
                VALUES (%s, %s, %s)
                ON CONFLICT (discord_id)
                DO UPDATE SET
                    rating = excluded.rating,
                    player_name = excluded.player_name;
            """
            db_helper.insert_single(ratings_insert_query, (player_id, player_name, rating))

        if (len(unnamed_players) != 0):
            return ctx.respond(f"Players {unnamed_players} in the Players sheet do not have names. Excluding them from the rankings table.")
        elif (len(no_score_players) != 0):
            return ctx.respond(f"Players {unnamed_players} in the Players sheet do not have any scores registered.")

        return ctx.respond("Finished updating database.")
    except Exception as e:
        return ctx.respond(f"Error updating database: {e}")
    

def get_top_10_table(ctx):

    try:
        query = f"""
            SELECT player_id, rating, player_name
            FROM {PLAYERS_TABLE};
        """
        player_data = db_helper.select(query)
    except Exception as e:
        return print(f"Error: {e}")

    # Separate players with no valid rating
    rated_players = []

    for player_id, rating, player_name in player_data:
        if rating != INVALID_RATING:
            rated_players.append([player_name, rating])

    if len(rated_players) == 0:
        logger.info("No players are currently rated.")
        logger.info("Finished updating rankings.")
        return ctx.respond("No players are currently ranked.")
    
    # Sort players based on rating
    rated_players.sort(key=lambda x: x[1])

    # Assign ranks to rated players
    rankings_sheet = [[rank, player_name, f"{rating:.2f}"] 
                      for rank, [player_name, rating] in enumerate(rated_players, start=1)]
    
    # Use the top 10 entries of the rankings to create the table
    top_10_table = table_generation.create_ascii_table("Server Rankings", ["Rank", "Player", "Rating"], rankings_sheet[:10])
    table_stream = table_generation.create_image_from_table(str(top_10_table))
    attachment = discord.File(fp=table_stream, filename="rankings.png")
    table_stream.close()
    return ctx.respond(file=attachment)


def get_difficulty_indices_table(ctx):

    difficulty_indices = get_difficulty_indices()
    difficulty_indices_data = []

    for course_id in range(len(COURSES)):
        course = COURSES[course_id]
        front_9_index = difficulty_indices[course_id * 2]
        back_9_index = difficulty_indices[course_id * 2 + 1]
        difficulty_indices_data.append([course, f"{front_9_index:.2f}", f"{back_9_index:.2f}"])
    
    # Create and post the difficulty indices table from the data
    difficulty_indices_table = table_generation.create_ascii_table("Course Difficulty Indices", ["Course", "Front 9", "Back 9"], difficulty_indices_data)
    table_stream = table_generation.create_image_from_table(str(difficulty_indices_table))
    attachment = discord.File(fp=table_stream, filename="indices.png")
    table_stream.close()
    return ctx.respond(file=attachment)


def get_player_profile(ctx, player_id):

    if player_id is not None and player_id.isdigit() == False:
        return ctx.respond("Player ID must be an integer.")

    if player_id == None:
        # If no ID was entered, get the poster's profile
        player_id = ctx.author.id

    # Get the player's name and rating from the database
    query = f"""
        SELECT player_name, rating
        FROM {PLAYERS_TABLE}
        WHERE discord_id = %s
    """
    result = db_helper.select(query, (player_id,))

    if result == []:
        return ctx.respond("Player not found.")
    
    player_name, player_rating = result[0]

    # Retrieve the player's scores
    query = f"""
        SELECT course_id, character, score
        FROM {SCORES_TABLE}
        WHERE player_id = %s
        ORDER BY timestamp DESC;
    """
    result = db_helper.select(query, (player_id,))

    character_stats = defaultdict(int)
    scores_dict = {}
    total_scores = len(result)

    for course_id, character, score in result:
        character_stats[character] += 1
        
        if course_id in scores_dict:
            scores_dict[course_id].append(score)
        else:
            scores_dict[course_id] = [score]

    # Calculate and save character usage percentages
    character_percentages = {char: (count / total_scores) * 100 for char, count in character_stats.items()}

    # Find the top 3 most used characters
    top_characters = dict(sorted(character_percentages.items(), key=lambda item: item[1], reverse=True)[:3])

    # Generate a formatted list
    top_characters_list = [
        f"   {i + 1}. {char} ({percentage:.2f}%)"
        for i, (char, percentage) in enumerate(top_characters.items())
    ]

    # Join the list into a string
    top_characters_str = "\n".join(top_characters_list)

    # Calculate average score for each course
    average_dict = {}
    for course_id, scores in scores_dict.items():
        average = sum(scores) / len(scores)

        # Make it so it always shows the sign for the score
        average_str = f"{average:.2f}"
        if average == 0.0:
            average_str = f"±{average_str}"
        elif average > 0.0:
            average_str = f"+{average_str}"

        average_dict[course_id] = average_str

    course_averages_table_data = []

    for course_index in range(len(COURSES)):
        course_name = COURSES[course_index]
        course_entry = [course_name, "--", "--"]
        for nine_index in range(len(NINES)):
            course_id = ((course_index * 2) + nine_index) + 1
            if (course_id not in average_dict.keys()):
                continue

            course_entry[nine_index + 1] = average_dict[course_id]

        course_averages_table_data.append(course_entry)

    course_averages_table = table_generation.create_ascii_table("Course Averages", ["Course", "Front 9", "Back 9"], course_averages_table_data)
    rating_str = f"{player_rating:.2f}" if player_rating != INVALID_RATING else "NR"

    table_str = (
    f" Player: {player_name}\n"
    f" Rating: {rating_str}\n"
    f" Favorite Characters:\n"
    f"{top_characters_str}\n"
    "\n"
    f"{course_averages_table}"
    )

    table_stream = table_generation.create_image_from_table(table_str)
    attachment = discord.File(fp=table_stream, filename="profile.png")
    table_stream.close()
    return ctx.respond(file=attachment)


def get_recent_score_table(ctx, player_id):

    if player_id is not None and player_id.isdigit() == False:
        return ctx.respond("Player ID must be an integer.")

    if player_id == None:
        # If no ID was entered, get the poster's recent scores
        player_id = ctx.author.id

    # Get the player's name and rating from the database
    query = f"""
        SELECT player_name
        FROM {PLAYERS_TABLE}
        WHERE discord_id = %s
    """
    result = db_helper.select(query, (player_id,))

    if result == []:
        return ctx.respond("Player not found.")
    
    player_name = result[0][0]

    # Retrieve the player's scores
    query = f"""
        SELECT s.timestamp, s.course_id, s.character, s.score, c.difficulty_index, s.adjusted_score, s.rating
        FROM {SCORES_TABLE} s
        JOIN {COURSES_TABLE} c ON s.course_id = c.course_id
        WHERE player_id = %s;
    """
    result = [list(entry) for entry in db_helper.select(query, (player_id,))]
    result = sorted(result, key=lambda x: x[0], reverse=True)  # Sort by timestamp in descending order (most recent scores first)

    recent_scores_table_data = []

    # Get the most recent 40 scores
    for timestamp, course_id, character, score, difficulty_index, adjusted_score, rating in result[:40]:
        formatted_date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        nine = NINES[(course_id - 1) % 2]
        course = COURSES[math.ceil(course_id / len(NINES)) - 1]

        score_str = str(score)
        if score == 0:
            score_str = f"±{score_str}"
        elif score > 0:
            score_str = f"+{score_str}"

        rating_str = f"{rating:.2f}" if rating != INVALID_RATING else "NR"

        entry = [formatted_date, f"{course} ({nine})", character, score_str, f"{difficulty_index:.2f}", f"{adjusted_score:.2f}", rating_str]
        recent_scores_table_data.append(entry)

    
    recent_scores_table = table_generation.create_ascii_table(f"Recent Scores ({player_name})", ["Date", "Course", "Character", "Score", "Diff.Ind.", "Adj.Score", "Rating"], recent_scores_table_data)
    table_stream = table_generation.create_image_from_table(str(recent_scores_table))
    attachment = discord.File(fp=table_stream, filename="table.png")
    table_stream.close()
    return ctx.respond(file=attachment)


def update_difficulty_indices():

    # Number of scores for each course to be considered for average calculation
    NUM_REQUIRED_SCORES = 8

    # Get the score info for all players who have played each course more than
    # the minimum number of times required
    query = f"""
        SELECT s.player_id, s.course_id, s.timestamp, s.score
        FROM {SCORES_TABLE} s
        WHERE s.player_id IN (
        SELECT p.discord_id
        FROM {PLAYERS_TABLE} p
        WHERE (
            SELECT COUNT(DISTINCT c.course_id)
            FROM {COURSES_TABLE} c
            WHERE (
            SELECT COUNT(s.course_id)
            FROM {SCORES_TABLE}  s
            WHERE s.player_id = p.discord_id
            AND s.course_id = c.course_id
            ) >= {NUM_REQUIRED_SCORES}
        ) = (SELECT COUNT(*) FROM {COURSES_TABLE})
        );
    """
    scores = [list(entry) for entry in db_helper.select(query)]

    # Create a dictionary to store player data.
    player_data = defaultdict(list)
    
    # Sort the data by timestamp
    scores.sort(key=lambda x: x[2])
    
    for player_id, course_id, date, score in scores:
        player_data[player_id].append((course_id, date, score))

    player_course_averages = {}
    
    for player_id, rounds in player_data.items():
        course_averages = defaultdict(list)
        
        for course_id, date, score in rounds:
            course_averages[course_id].append(score)
        
        player_course_averages[player_id] = {}
        
        for course_id, scores in course_averages.items():
            recent_scores = scores[-NUM_REQUIRED_SCORES:]
            average_score = sum(recent_scores) / len(recent_scores)
            player_course_averages[player_id][course_id] = average_score
    
    # Calculate the average of average scores for each player for each course.
    course_averages_by_player = defaultdict(lambda: defaultdict(list))
    
    for player_id, course_averages in player_course_averages.items():
        for course_id, average_score in course_averages.items():
            course_averages_by_player[course_id][player_id] = average_score
    
    final_averages = {}
    
    for course_id, player_averages in course_averages_by_player.items():
        average = sum(player_averages.values()) / len(player_averages)
        final_averages[course_id] = average

    difficulty_indices = {}

    # Calculate the overall average of course averages
    overall_avg = sum(final_averages.values()) / len(final_averages)

    difficulty_indices = {}

    for course, course_avg in final_averages.items():
        # Calculate the difficulty index for each course
        difficulty_indices[course] = course_avg - overall_avg

    # Flatten the dictionary into a list
    flattened_indices = [(course, difficulty_index) for course, difficulty_index in difficulty_indices.items()]

    # Update database with new indices
    update_query = f"""
        UPDATE {COURSES_TABLE} c
        SET difficulty_index = v.index
        FROM (VALUES %s) AS v(id, index)
        WHERE c.course_id = v.id;
    """
    db_helper.update_multiple(update_query, flattened_indices)

def generate_difficulty_indices_sheet():
    """
    Generate a spreadsheet table with difficulty indices for all courses.

    Parameters:
        none

    Returns:
        None

    Raises:
        Exception: If there is an error during the database interaction.
    """

    try:
        query = f"""
            SELECT difficulty_index
            FROM {COURSES_TABLE}
            ORDER BY course_id;
        """
        result = db_helper.select(query)

        indices_table = [[] for _ in range(len(COURSES))]

        for course_index in range(len(COURSES)):
            front_index = result[(course_index * 2)][0]
            back_index = result[(course_index * 2) + 1][0]
            row = [front_index, back_index]
            indices_table[course_index] = row

        sheets_helper.write_data(
            LEADERBOARD_SPREADSHEET_ID, indices_table, "Difficulty Indices", "B4")

        now = datetime.utcnow()
        formatted_time = now.strftime("%m/%d/%Y %H:%M:%S")
        last_updated_msg = f"Last updated: {formatted_time}"
        sheets_helper.write_data(
            LEADERBOARD_SPREADSHEET_ID, [[last_updated_msg]], "Difficulty Indices", "A2")
    except Exception:
        raise
