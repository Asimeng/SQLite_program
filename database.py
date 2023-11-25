import sqlite3
import datetime
import os

database_file_path = "./furnace_db.db"
video_folder_path = "./furnace_videos/"

def query_db(database_file_path, video_folder_path):  

    #Create database
    db = sqlite3.connect(database_file_path)

    #get a cursor object
    cursor = db.cursor()

    #Create table
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS imagelog(
        furnace_id INTEGER,
        timestamp REAL,
        PRIMARY KEY (furnace_id, timestamp)
        )"""
    )

    db.commit()

    def process_video_filename(filename):    
        filename_lowercase = filename.lower()
        furnace_id = int(filename[3])
        datetime_string = filename_lowercase.removeprefix(f"fce{furnace_id}_visual_").removesuffix(".mp4")
        datetime_object = datetime.datetime.strptime(datetime_string, "%Y%m%d_%H%M%S")
        timestamp = datetime_object.timestamp()
        return furnace_id, timestamp

    video_paths = []
    for entry in os.scandir(video_folder_path):
        if entry.is_file() and entry.name.lower().endswith(".mp4"):
            video_filename = entry.name.lower()
            furnace_id, timestamp = process_video_filename(video_filename)
            #Add to database
            cursor.execute(
                """INSERT INTO imagelog(furnace_id, timestamp) VALUES(?,?)""",
                (furnace_id, timestamp)
            )
            video_paths.append(entry.path)

    for video_path in video_paths:
        image_path = video_path.removesuffix(".mp4") + ".png"
        
        os.remove(video_path)
        try:
            os.remove(image_path)
        except:
            print(f"failed to delete {image_path}")

    cursor.execute(
        """SELECT furnace_id, timestamp FROM imagelog"""
    )
    rows = cursor.fetchall()
    output_rows = []
    for furnace_id, timestamp in rows:
        event_datetime = datetime.datetime.fromtimestamp(timestamp)
        output_rows.append([furnace_id, event_datetime])
        
    db.commit()
    cursor.close()   
    db.close()
    return output_rows

print(query_db("./myname.db", "./furnace_videos/"))