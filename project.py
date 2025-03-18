import sys
import mysql.connector
from setup_db import initialize_db, populate_db
import sys

def open_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="test",
        password="password",
        database="cs122a",
        allow_local_infile=True
    )

def import_data(folder_name):
# assumes the folder to read is in the same directory as project.py.
# todo: determine if this assumption is correct. check ed discussion, ask TA, etc.
    try:
        print(f"Importing folder: {folder_name}")
        db_connection = open_db_connection()
        initialize_db(db_connection)
        populate_db(db_connection, folder_name)
        db_connection.close()
        return True
    except Exception as e:
        print(f"Error importing folder '{folder_name}': {e}")
        return False


def insert_viewer(
    uid,
    email,
    nickname,
    street,
    city,
    state,
    zip,
    genres,
    joined_date,
    first,
    last,
    subscription
):
    db = open_db_connection()
    cursor = db.cursor()
    #Tries to insert a user first
    try:
       pass 
       cursor.execute("INSERT INTO USERS (uid,email,joined_date,nickname,street,city,state,zip,genres) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                      (uid, email, joined_date, nickname, street, city, state, zip, genres))
       db.commit()
    except Exception:
        #Should only fail when there's a duplicate in the system
       pass 
   
    #Then tries to insert a viewer
    try:
        print(f"Inserting Viewer: uid={uid}, email={email}, nickname={nickname}, street={street}, city={city}, state={state}, zip={zip}, genres={genres}, joined_date={joined_date}, first={first}, last={last}, subscription={subscription}")
       
        cursor.execute("INSERT INTO Viewers (uid, subscription, first_name, last_name) VALUES (%s,%s,%s,%s)",
                       (uid, subscription, first, last))
        db.commit()
        print("Success") # good catch on fixing this.... do we need to print "Success" or return the vaule "Success"? 
                        # EdStem #419 says we can return False/Fail, but dcoument says to print, so I'm a little confused too
    except Exception as e:
        print("Fail")
        print(e)
    cursor.close()
    db.close()
   

def add_genre(uid, genre):
    try:
        print(f"Adding Genre: uid={uid}, genre={genre}")
       
        db = open_db_connection()
        cursor = db.cursor()

        cursor.execute("SELECT genres FROM Users WHERE uid = %s", (uid,))
        genres = cursor.fetchone()[0].strip()
        
        if genres != "":                    # only add semicolon if existing genres for User
            genre = genres + ";" + genre
        
        cursor.execute("UPDATE Users set genres = %s WHERE uid = %s;", (genre, uid))
        db.commit()
        db.close()
        return True
    except Exception as e:
        print(f"Error adding genre: {e}")
        return False

def delete_viewer(uid):
    try:
        print(f"Deleting viewer: uid={uid}")
       
        db_connection = open_db_connection()
        cursor = db_connection.cursor()

        cursor.execute("DELETE FROM Viewers WHERE uid = %s", (uid,))
        
        db_connection.commit()
        db_connection.close()
        return True
    except Exception as e:
        print(f"Error deleting viewer: {e}")
        return False


def insert_movie(rid, website_url):
    try:
        print(f"Inserting movie: rid={rid}, website_url={website_url}")
       
        db_connection = open_db_connection()

        # Database logic goes here

        db_connection.close()
        return True
    except Exception as e:
        print(f"Error inserting movie: {e}")
        return False


def insert_session(
    sid,
    uid,
    rid,
    ep_num,
    initiate_at,
    leave_at,
    quality,
    device
):
    try:
        print(f"Inserting Session: sid={sid}, uid={uid}, rid={rid}, ep_num={ep_num}, initiate_at={initiate_at}, leave_at={leave_at}, quality={quality}, genres={genres}, device={device}")
       
        db_connection = open_db_connection()
        
        # Database logic goes here

        db_connection.close()
        return True
    except Exception as e:
        print(f"Error inserting session: {e}")
        return False

def update_release(rid, title):
    try:
        # print(f"Updating Release: rid={rid}, title={title}")    
        db = open_db_connection()
        cursor = db.cursor()
        cursor.execute("UPDATE Releases set title = %s WHERE rid = %s;", (title, rid))
        db.commit()
        db.close()
        print("Success")
    except Exception as e:
        print(f"Error updating release: {e}")
        print("Fail")

def list_releases(uid):
    try:
        print(f"Listing releases reviewed by viewer: uid={uid}")
       
        db_connection = open_db_connection()

        # Database logic goes here

        db_connection.close()
        return True                 # todo: return table
    except Exception as e:
        print(f"Error listing releases: {e}")
        return False

def popular_release(n):
    try:
        print(f"Listing popular releases: n={n}")
       
        db_connection = open_db_connection()

        # Database logic goes here

        db_connection.close()
        return True                 # todo: return table
    except Exception as e:
        print(f"Error listing popular releases: {e}")
        return False

def release_title(sid):
    try:
        #print(f"Getting title for release: sid={sid}")
       
        db = open_db_connection()
        cursor = db.cursor()

        cursor.execute("""
            SELECT r.rid, r.title AS release_title, r.genre,
                   v.title AS video_title, v.ep_num, v.length
            FROM Sessions AS s
            INNER JOIN Releases r ON r.rid = s.rid
            INNER JOIN Videos v ON r.rid = v.rid
            WHERE s.sid = %s
            ORDER BY r.title ASC;
            """, (sid,)
        )

        results = cursor.fetchall()

        # Print column names
        headers = ["RID", "Release Title", "Genre", "Video Title", "Episode #", "Length"]
        print(f"{headers[0]:<5} {headers[1]:<20} {headers[2]:<15} {headers[3]:<20} {headers[4]:<10} {headers[5]:<10}")
        print("-" * 80)

        # Print each row returned
        for row in results:
            print(f"{row[0]:<5} {row[1]:<20} {row[2]:<15} {row[3]:<20} {row[4]:<10} {row[5]:<10}")

    except Exception as e:
        print(f"Error getting release title: {e}")

    cursor.close()
    db.close()


def active_viewers(n, start, end):
    try:
        print(f"Listing active viewers: n={n}, start={start}, end={end}")
       
        db_connection = open_db_connection()

        # Database logic goes here

        db_connection.close()
        return True                 # todo: return table
    except Exception as e:
        print(f"Error listing active viewers: {e}")
        return False

def videos_viewed(rid):
    try:
        print(f"Listing count of unique viewers for release: rid={rid}")
       
        db_connection = open_db_connection()

        # Database logic goes here

        db_connection.close()
        return True                 # todo: return table
    except Exception as e:
        print(f"Error listing viewer count: {e}")
        return False

def main():
    function = sys.argv[1].lower()
    
    #7) If the input is NULL, treat it as the None type in Python, not a string called “NULL”.
    for i in range(len(sys.argv)):
        if sys.argv[i] == "NULL":
            sys.argv[i] = None
            

    if function == "import":
        import_data(sys.argv[2])
    elif function == "insertviewer":
        insert_viewer(
            sys.argv[2],  # uid
            sys.argv[3],  # email
            sys.argv[4],  # nickname
            sys.argv[5],  # street
            sys.argv[6],  # city
            sys.argv[7],  # state
            sys.argv[8],  # zip
            sys.argv[9],  # genres
            sys.argv[10], # joined_date
            sys.argv[11], # first
            sys.argv[12], # last
            sys.argv[13]  # subscription
        )
    elif function == "addgenre":
        add_genre(
            sys.argv[2],  # uid
            sys.argv[3]   # genre
        )
    elif function == "deleteviewer":
        delete_viewer(
            sys.argv[2]   # uid
        )
    elif function == "insertmovie":
        insert_movie(
            sys.argv[2],  # rid
            sys.argv[3]   # website_url
        )
    elif function == "insertsession":
        insert_session(
            sys.argv[2],  # sid
            sys.argv[3],  # uid
            sys.argv[4],  # rid
            sys.argv[5],  # ep_num
            sys.argv[6],  # initiate_at
            sys.argv[7],  # leave_at
            sys.argv[8],  # quality
            sys.argv[9]   # device
        )
    elif function == "updaterelease":
        update_release(
            sys.argv[2],  # rid
            sys.argv[3]   # title
        )
    elif function == "listreleases":
        list_releases(
            sys.argv[2]  # uid
        )
    elif function == "popularrelease":
        popular_release(
            sys.argv[2]   # N
        )
    elif function == "releasetitle":
        release_title(
            sys.argv[2]   # sid
        )
    elif function == "activeviewer":
        active_viewers(
            sys.argv[2],  # n
            sys.argv[3],  # start
            sys.argv[4]   # end
        )
    elif function == "videosviewed":
        videos_viewed(
            sys.argv[2]   # rid
        )

if __name__ == "__main__":
    main()