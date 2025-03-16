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
    try:
        print(f"Inserting Viewer: uid={uid}, email={email}, nickname={nickname}, street={street}, city={city}, state={state}, zip={zip}, genres={genres}, joined_date={joined_date}, first={first}, last={last}, subscription={subscription}")
        db = open_db_connection()
        cursor = db.cursor()
        
        #Assumes user is already created, and existing in Users table
        cursor.execute("INSERT INTO Viewers (uid, subscription, first_name, last_name) VALUES (%s,%s,%s,%s)",
                       (uid, subscription, first, last))
        db.commit()
        print("Success")
    except Exception as e:
        print("Fail")
        print(e)
    cursor.close()
    db.close()
    


'''def add_genre(uid, genre):
    try:
        print(f"Adding Genre: uid={uid}, genre={genre}")
       
        db_connection = open_db_connection()
        initialize_db(db_connection)

        # Database logic goes here

        db_connection.close()
        return True
    except Exception as e:
        print(f"Error adding genre: {e}")
        return False'''

def delete_viewer(uid):
    try:
        print(f"Deleting viewer: uid={uid}")
       
        db_connection = open_db_connection()
        initialize_db(db_connection)

        # Database logic goes here

        db_connection.close()
        return True
    except Exception as e:
        print(f"Error deleting viewer: {e}")
        return False


def insert_movie(rid, website_url):
    try:
        print(f"Inserting movie: rid={rid}, website_url={website_url}")
       
        db_connection = open_db_connection()
        initialize_db(db_connection)

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
        initialize_db(db_connection)

        # Database logic goes here

        db_connection.close()
        return True
    except Exception as e:
        print(f"Error inserting session: {e}")
        return False

def update_release(rid, title):
    try:
        print(f"Updating Release: rid={rid}, title={title}")
       
        db_connection = open_db_connection()
        initialize_db(db_connection)

        # Database logic goes here

        db_connection.close()
        return True
    except Exception as e:
        print(f"Error updating release: {e}")
        return False

def list_releases(uid):
    try:
        print(f"Listing releases reviewed by viewer: uid={uid}")
       
        db_connection = open_db_connection()
        initialize_db(db_connection)

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
        initialize_db(db_connection)

        # Database logic goes here

        db_connection.close()
        return True                 # todo: return table
    except Exception as e:
        print(f"Error listing popular releases: {e}")
        return False

def release_title(sid):
    try:
        print(f"Getting title for release: sid={sid}")
       
        db_connection = open_db_connection()
        initialize_db(db_connection)

        # Database logic goes here

        db_connection.close()
        return True                 # todo: return table
    except Exception as e:
        print(f"Error getting release title: {e}")
        return False

def active_viewers(n, start, end):
    try:
        print(f"Listing active viewers: n={n}, start={start}, end={end}")
       
        db_connection = open_db_connection()
        initialize_db(db_connection)

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
        initialize_db(db_connection)

        # Database logic goes here

        db_connection.close()
        return True                 # todo: return table
    except Exception as e:
        print(f"Error listing viewer count: {e}")
        return False

def main():
    function = sys.argv[1].lower()

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
    elif function == "insertsession": #[sid:int] [uid:int] [rid:int] [ep_num:int] [initiate_at:datetime] [leave_at:datetime] [quality:str] [device:str]
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
        update_releases(
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
