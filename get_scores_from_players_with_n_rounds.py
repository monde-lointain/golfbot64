import db_helper
from globals import *

from collections import defaultdict
import math

num_required_scores = 1
num_players = 1

def get_player_data(data):
    # Create a dictionary to store player data.
    player_data = defaultdict(list)
    
    # Sort the data by timestamp
    data.sort(key=lambda x: x[2])
    
    for player_id, course_id, date, score in data:
        player_data[player_id].append((course_id, date, score))

    return player_data


def calculate_average_scores(player_data):
    
    player_course_averages = {}
    
    for player_id, rounds in player_data.items():
        course_averages = defaultdict(list)
        
        for course_id, date, score in rounds:
            course_averages[course_id].append(score)
        
        player_course_averages[player_id] = {}
        
        for course_id, scores in course_averages.items():
            recent_scores = scores[-num_required_scores:]
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
    
    return final_averages

while True:
    # Get the score info for all players who have played each course more than the minimum number of times required
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
            ) >= {num_required_scores}
        ) = (SELECT COUNT(*) FROM {COURSES_TABLE})
        );
    """
    scores = [list(entry) for entry in db_helper.select(query)]

    if scores == []:
        break

    player_data = get_player_data(scores)
    averages = calculate_average_scores(player_data)

    differentials = {}

    # Calculate the overall average of course averages
    overall_avg = sum(averages.values()) / len(averages)

    differentials = {}

    for course, course_avg in averages.items():
        # Calculate the differential for each course
        diff = course_avg - overall_avg
        differentials[course] = diff

    # Sort the course IDs in ascending order
    sorted_courses = sorted(differentials.keys())

    print("Number of required scores:", num_required_scores)
    print("Number of players:", len(player_data))
    print("Scores used per course:", num_required_scores * len(player_data))
    print("Overall average score:", overall_avg)
    print("\nDifficulty Indices:")
    for course_id in sorted_courses:
        nine = NINES[(course_id - 1) % 2]
        course = COURSES[math.ceil(course_id / len(NINES)) - 1]
        print(f"{course} ({nine}): {differentials[course_id]:.2f}")

    print("\n")

    num_required_scores += 1
