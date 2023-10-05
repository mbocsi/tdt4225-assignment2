from Database import Database
from tabulate import tabulate
from datetime import datetime, timedelta
from haversine import haversine, Unit
from collections import deque

# 1
def sol1(db : Database) -> None:
    print('Number of users after insert:')
    query = 'SELECT COUNT(*) FROM User;'
    print(db.query_data(query)[0][0])

    print('Number of activities after insert:')
    query = 'SELECT COUNT(*) FROM Activity;'
    print(db.query_data(query)[0][0])

    print('Number of trackpoints after insert:')
    query = 'SELECT COUNT(*) FROM TrackPoint;'
    print(db.query_data(query)[0][0])

# 2
def sol2(db : Database) -> None:
    print('Minimum number of trackpoints for a user:')
    query = '''
            SELECT MIN(count) FROM (
                SELECT COUNT(*) AS count
                FROM Activity
                JOIN `TrackPoint`
                ON Activity.id = TrackPoint.activity_id
                GROUP BY user_id
            ) AS Counts;
            '''
    print(db.query_data(query)[0][0])

    print('Maximum number of trackpoints for a user:')
    query = '''
            SELECT MAX(count) FROM (
                SELECT COUNT(*) AS count
                FROM Activity
                JOIN `TrackPoint`
                ON Activity.id = TrackPoint.activity_id
                GROUP BY user_id
            ) AS Counts;
            '''
    print(db.query_data(query)[0][0])

    print('Average number of trackpoints for a user:')
    query = '''
            SELECT AVG(count) FROM (
                SELECT COUNT(*) AS count
                FROM Activity
                JOIN `TrackPoint`
                ON Activity.id = TrackPoint.activity_id
                GROUP BY user_id
            ) AS Counts;
            '''
    print(db.query_data(query)[0][0])

# 3
def sol3(db : Database) -> None:
    print("Top 15 users by activity count:")
    query = '''
            SELECT user_id
            FROM `Activity`
            GROUP BY user_id
            ORDER BY COUNT(*) DESC
            LIMIT 15
            '''
    print(tabulate(db.query_data(query), headers=['user_id']))

# 4
def sol4(db : Database) -> None:
    print("Users that have used the bus:")
    query = '''
            SELECT user_id
            FROM `Activity`
            WHERE transportation_mode="bus"
            GROUP BY user_id
            '''
    print(tabulate(db.query_data(query), headers=['user_id']))

# 5
def sol5(db : Database) -> None:
    print("Top 10 users by number of transportation modes:")
    query = '''
            SELECT user_id
            FROM `Activity`
            GROUP BY user_id
            ORDER BY COUNT(DISTINCT transportation_mode) DESC
            LIMIT 10
            '''
    print(tabulate(db.query_data(query), headers=['user_id']))

# 6
def sol6(db : Database) -> None:
    print("Activities that are registered multiple times:")
    query = '''
            SELECT user_id, transportation_mode, start_date_time, end_date_time
            FROM `Activity`
            GROUP BY user_id, transportation_mode, start_date_time, end_date_time
            HAVING COUNT(*) > 1
            '''
    print(tabulate(db.query_data(query), headers=['user_id', 'transportation_mode', 'start_date_time', 'end_date_time']))

# 7
# a
def sol7a(db : Database) -> None:
    print("Number of users that have an activity that starts and ends on different days:")
    query = '''
            SELECT COUNT(DISTINCT user_id)
            FROM `Activity`
            WHERE DAY(start_date_time) < DAY(end_date_time);
            '''
    print(db.query_data(query)[0][0])

# b
def sol7b(db : Database) -> None:
    print("Activities that start and end on different days:")
    query = '''
            SELECT user_id, transportation_mode, start_date_time, end_date_time
            FROM `Activity`
            WHERE DAY(start_date_time) < DAY(end_date_time)
            '''
    print(tabulate(db.query_data(query), headers=['user_id', 'transportation_mode', 'start_date_time', 'end_date_time']))

# 8
def sol8(db : Database) -> None:
    print("Number of users that have been close in time and space:")
    query = '''
            SELECT Activity.user_id, TrackPoint.lat, TrackPoint.lon, TrackPoint.date_time
            FROM `Activity`
            JOIN `TrackPoint` ON Activity.id = TrackPoint.activity_id
            ORDER BY TrackPoint.date_time;
            '''
    data = db.query_data(query) # Get all track points
    result = {}
    potentials = deque()
    for user, lat, lon, time in data:
        # Remove track points that are too far in time from the potentials queue
        for _, _, _, prev_time in list(potentials):
            if time - prev_time > timedelta(seconds=30):
                potentials.popleft()
            else:
                break # Since potentials are in time order, the rest must be fine
        
        for prev_user, prev_lat, prev_lon, _ in potentials:
            if user == prev_user:   # Skip if the track point is for the same user
                continue
            
            if user in result and prev_user in result: # Both users are already in solution set, no need to check
                continue

            prev_latlon = (prev_lat, prev_lon)
            dist = haversine(prev_latlon, (lat, lon), unit=Unit.METERS) # Skip if the distances are too far apart
            if dist > 50:
                continue
            
            # The two track points are close in time and space => Add the users to the results
            result[prev_user] = True
            result[user] = True

        potentials.append((user, lat, lon, time)) # Add the current track point to the end of potentials queue

    print(len(result)) # Result stores the exact users, but we only want the total number of users

# 9
def sol9(db : Database) -> None:
    print("Top 15 users by meters gained (gross):")
    query = '''
            SELECT Alts.user_id, SUM(Alts.curr_alt - Alts.prev_alt)*0.3048 AS meters_gained
            FROM (
                SELECT Activity.user_id, TrackPoint.activity_id, TrackPoint.altitude AS curr_alt, LAG(TrackPoint.altitude, 1) OVER(ORDER BY TrackPoint.activity_id, TrackPoint.id) AS prev_alt
                FROM `TrackPoint`
                JOIN `Activity` ON TrackPoint.activity_id = Activity.id
                WHERE TrackPoint.altitude != -777
            ) AS Alts
            WHERE Alts.curr_alt - Alts.prev_alt > 0
            GROUP BY Alts.user_id
            ORDER BY SUM(Alts.curr_alt - Alts.prev_alt) DESC
            LIMIT 15
            '''
    print(tabulate(db.query_data(query), headers=['user_id', 'meters_gained']))

# 10
def sol10(db : Database) -> None:
    print("Longest distance by user per transportation mode:")
    query = '''
            SELECT Activity.user_id, TrackPoint.activity_id, Activity.transportation_mode, TrackPoint.lat, TrackPoint.lon, TrackPoint.date_time
            FROM `Activity`
            JOIN `TrackPoint` ON Activity.id = TrackPoint.activity_id
            WHERE transportation_mode IS NOT NULL
            ORDER BY Activity.id, TrackPoint.id
            '''
    data = db.query_data(query)
    result = {}
    prev_activity = None
    prev_latlon = () 
    day = {} # Tracks the day that the user is on
    curr_dist = {} # Track the distances for the users for their current day (day)
    for user, activity_id, transportation_mode, lat, lon, time in data:
        if prev_activity != activity_id:
            prev_activity = activity_id
            prev_latlon = (lat, lon)
            continue

        if user not in day:
            day[user] = time.date()

        if time.date() != day[user]: # Day is over for user
            day[user] = time.date()
            for mode, val in curr_dist[user].items(): # Get distances for user day
                if mode not in result:
                    result[mode] = {user: val}
                else:
                    if user not in result[mode]: # Insert value if user is not an option in the final result
                        result[mode][user] = val
                    elif result[mode][user] < val: # Insert if value is greater than the current final result
                        result[mode][user] = val
                curr_dist[user][mode] = 0           # Clear the current distances for the user
                        
        dist = haversine(prev_latlon,(lat, lon), unit=Unit.KILOMETERS)
        if user not in curr_dist:
            curr_dist[user] = {transportation_mode: dist}
        elif transportation_mode not in curr_dist[user]:   
            curr_dist[user][transportation_mode] = dist
        else:
            curr_dist[user][transportation_mode] += dist
        prev_latlon = (lat, lon)

    table = []
    for mode, users in result.items():
        top_user = max(users, key=users.get)
        table.append((mode, top_user, users[top_user]))

    print(tabulate(table, headers=['transportation_mode', 'top_user', 'distance']))

# 11
def sol11(db : Database) -> None:
    query = '''
            SELECT Counts.user_id, COUNT(*) AS invalid_activities
            FROM (
                SELECT Sub.curr_id, Sub.user_id, COUNT(*) FROM (
                    SELECT Activity.id AS curr_id, 
                        LAG(Activity.id, 1) OVER(ORDER BY Activity.id, TrackPoint.id) AS prev_id, 
                        Activity.user_id AS user_id, 
                        TrackPoint.date_time AS curr_date, 
                        LAG(TrackPoint.date_time, 1) OVER(ORDER BY Activity.id, TrackPoint.id) AS prev_date 
                    FROM `Activity`
                    JOIN `TrackPoint` ON Activity.id = TrackPoint.activity_id
                ) AS Sub
                WHERE TIMEDIFF(Sub.curr_date, Sub.prev_date) > "00:05:00" AND Sub.curr_id = Sub.prev_id
                GROUP BY Sub.curr_id, Sub.user_id
            ) AS Counts
            GROUP BY Counts.user_id;
            ''' 
    print(tabulate(db.query_data(query), headers=['user_id', 'invalid_activities']))


# 12
def sol12(db : Database) -> None:
    query = '''
            SELECT User.id, transportation_mode
            FROM User
            JOIN `Activity` ON User.id = Activity.user_id
            WHERE User.has_labels AND transportation_mode IS NOT NULL
            ORDER BY User.id
            '''
    data = db.query_data(query)
    users = {}
    for i in data: 
        if i[0] not in users:
            users[i[0]] = [i[1]]
        else:
            users[i[0]].append(i[1])

    # Calculate mode
    data = []
    for user, modes in users.items():
        data.append((user, max(set(modes), key=modes.count)))

    print(tabulate(data, headers=['user_id', 'most_used_transportation_mode']))

if __name__ == '__main__':
    db = Database()
    sol1(db)
    sol2(db)
    sol3(db)
    sol4(db)
    sol5(db)
    sol6(db)
    sol7a(db)
    sol7b(db)
    sol8(db)
    sol9(db)
    sol10(db)
    sol11(db)
    sol12(db)