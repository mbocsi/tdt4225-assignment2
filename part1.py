from Database import Database
from pathlib import Path
import os
from datetime import datetime
import logging

FORMAT = '%(asctime)s : %(message)s'
logging.basicConfig(filename='part1.log', filemode='w', level=logging.INFO, format=FORMAT)

def main() -> None:
    global activity_counter
    activity_counter = 0
    LABELED_IDS = []
    with open(Path('.') / 'dataset' / 'labeled_ids.txt', 'r') as f:
        lines = f.readlines()
        for line in lines:
            LABELED_IDS.append(line.replace('\n', ''))
    logging.debug(LABELED_IDS)

    # Create the tables
    db = Database()
    if not db.create_user_table():
        quit()
    if not db.create_activity_table():
        quit()
    if not db.create_trackpoint_table():
        quit()

    for root, dirs, files in os.walk("dataset/Data", topdown=True):
        if root == 'dataset/Data':
            for dir in dirs:
                if not db.insert_user(dir, dir in LABELED_IDS): # Insert the users into the database
                    quit()
                logging.info(f'Inserted user: {dir}')
        else:
            for file in files:
                activity_counter += 1
                if file[-3:] != 'plt':
                    continue

                user = Path(root).parts[2] # User ID
                filename = file.split('.')[0] # Activity ID
                plt = []

                with open(Path(root) / file, 'r') as f:
                    plt = f.readlines()

                if len(plt[6:]) > 2500: # Only insert activites with fewer than 2501 points
                    logging.info(f'Skipped activity: {filename}! TOO BIG!')
                    continue

                first_line = plt[6].split(',')
                start_date = first_line[-2]
                start_time = first_line[-1].replace('\n', '')
                start_datetime = datetime.strptime(f'{start_date} {start_time}', '%Y-%m-%d %H:%M:%S', )  # Start datetime

                last_line = plt[-1].split(',')
                end_date = last_line[-2]
                end_time = last_line[-1].replace('\n', '')
                end_datetime = datetime.strptime(f'{end_date} {end_time}', '%Y-%m-%d %H:%M:%S') # End datetime

                transportation = None # Transportation mode
                if user in LABELED_IDS:
                    with open(Path(*Path(root).parts[:3]) / 'labels.txt', 'r') as f:
                        labels = f.readlines()
                    for label in labels[1:]:
                        label = label.split()
                        label_start = datetime.strptime(f'{label[0]} {label[1]}', '%Y/%m/%d %H:%M:%S')
                        label_end = datetime.strptime(f'{label[2]} {label[3]}', '%Y/%m/%d %H:%M:%S', )
                        if label_start == start_datetime and label_end == end_datetime:
                            transportation = label[4]
                            break
                
                logging.debug(f'Inserting: {(activity_counter, user, transportation, start_datetime, end_datetime)}')

                # Insert the activity into the database
                if not db.insert_activity(id=activity_counter, 
                                user_id=user, 
                                transportation_mode=transportation,
                                start_date_time=start_datetime.strftime('%Y-%m-%d %H:%M:%S'), 
                                end_date_time=end_datetime.strftime('%Y-%m-%d %H:%M:%S')):
                    quit()
                logging.info(f'Inserted activity: {(activity_counter, user, transportation, start_datetime, end_datetime)}')

                track_points = []
                for point in plt[6:]:
                    lat, lon, _, alt, days, date, time = point.split(',')
                    time = time.replace('\n', '')
                    point_datetime = f"{date} {time}"
                    track_points.append((activity_counter, lat, lon, alt, days, point_datetime))

                # Insert the trackpoints associated with the activity
                if not db.insert_trackpoints(track_points):
                    quit()
                logging.info(f'Inserted trackpoints for activity: {activity_counter}')
                    
if __name__ == '__main__':
    main()