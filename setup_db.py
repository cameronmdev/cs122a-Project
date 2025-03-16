import mysql.connector

def create_table_users(cursor):
    cursor.execute("""
        CREATE TABLE Users (
            uid INT,
            email TEXT NOT NULL,
            joined_date DATE NOT NULL,
            nickname TEXT NOT NULL,
            street TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            genres TEXT,
            PRIMARY KEY (uid)
        );
    """)

def create_table_producers(cursor):
    cursor.execute("""
        CREATE TABLE Producers (
            uid INT,
            bio TEXT,
            company TEXT,
            PRIMARY KEY (uid),
            FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
        );
    """)

def create_table_viewers(cursor):
    cursor.execute("""
        CREATE TABLE Viewers (
            uid INT,
            subscription ENUM('free', 'monthly', 'yearly'),
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            PRIMARY KEY (uid),
            FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
        );
    """)

def create_table_releases(cursor):
    cursor.execute("""
        CREATE TABLE Releases (
            rid INT,
            producer_uid INT NOT NULL,
            title TEXT NOT NULL,
            genre TEXT NOT NULL,
            release_date DATE NOT NULL,
            PRIMARY KEY (rid),
            FOREIGN KEY (producer_uid) REFERENCES Producers(uid) ON DELETE CASCADE
        );
    """)

def create_table_movies(cursor):
    cursor.execute("""
        CREATE TABLE Movies (
            rid INT,
            website_url TEXT,
            PRIMARY KEY (rid),
            FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
        );
    """)

def create_table_series(cursor):
    cursor.execute("""
        CREATE TABLE Series (
            rid INT,
            introduction TEXT,
            PRIMARY KEY (rid),
            FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
        );
    """)

def create_table_videos(cursor):
    cursor.execute("""
        CREATE TABLE Videos (
            rid INT,
            ep_num INT NOT NULL,
            title TEXT NOT NULL,
            length INT NOT NULL,
            PRIMARY KEY (rid, ep_num),
            FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
        );
    """)

def create_table_sessions(cursor):
    cursor.execute("""
        CREATE TABLE Sessions (
            sid INT,
            uid INT NOT NULL,
            rid INT NOT NULL,
            ep_num INT NOT NULL,
            initiate_at DATETIME NOT NULL,
            leave_at DATETIME NOT NULL,
            quality ENUM('480p', '720p', '1080p'),
            device ENUM('mobile', 'desktop'),
            PRIMARY KEY (sid),
            FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
            FOREIGN KEY (rid, ep_num) REFERENCES Videos(rid, ep_num) ON DELETE CASCADE
        );
    """)

def create_table_reviews(cursor):
    cursor.execute("""
        CREATE TABLE Reviews (
            rvid INT,
            uid INT NOT NULL,
            rid INT NOT NULL,
            rating DECIMAL(2, 1) NOT NULL CHECK (rating BETWEEN 0 AND 5),
            body TEXT,
            posted_at DATETIME NOT NULL,
            PRIMARY KEY (rvid),
            FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
            FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
        );
    """)

def drop_all_tables(cursor):
    cursor.execute("DROP TABLE IF EXISTS Reviews;")
    cursor.execute("DROP TABLE IF EXISTS Sessions;")
    cursor.execute("DROP TABLE IF EXISTS Videos;")
    cursor.execute("DROP TABLE IF EXISTS Series;")
    cursor.execute("DROP TABLE IF EXISTS Movies;")
    cursor.execute("DROP TABLE IF EXISTS Releases;")
    cursor.execute("DROP TABLE IF EXISTS Viewers;")
    cursor.execute("DROP TABLE IF EXISTS Producers;")
    cursor.execute("DROP TABLE IF EXISTS Users;")

def initialize_db(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")

    drop_all_tables(cursor)

    create_table_users(cursor)
    create_table_producers(cursor)
    create_table_viewers(cursor)
    create_table_releases(cursor)
    create_table_movies(cursor)
    create_table_series(cursor)
    create_table_videos(cursor)
    create_table_sessions(cursor)
    create_table_reviews(cursor)

    db_connection.commit()
    cursor.close()

def populate_db(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")

    cursor.execute(f"LOAD DATA LOCAL INFILE '{folder_name}/users.csv' INTO TABLE Users FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")
    cursor.execute(f"LOAD DATA LOCAL INFILE '{folder_name}/producers.csv' INTO TABLE Producers FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")
    cursor.execute(f"LOAD DATA LOCAL INFILE '{folder_name}/viewers.csv' INTO TABLE Viewers FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")
    cursor.execute(f"LOAD DATA LOCAL INFILE '{folder_name}/releases.csv' INTO TABLE Releases FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")
    cursor.execute(f"LOAD DATA LOCAL INFILE '{folder_name}/movies.csv' INTO TABLE Movies FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")
    cursor.execute(f"LOAD DATA LOCAL INFILE '{folder_name}/series.csv' INTO TABLE Series FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")
    cursor.execute(f"LOAD DATA LOCAL INFILE '{folder_name}/videos.csv' INTO TABLE Videos FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")
    cursor.execute(f"LOAD DATA LOCAL INFILE '{folder_name}/sessions.csv' INTO TABLE Sessions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")
    cursor.execute(f"LOAD DATA LOCAL INFILE '{folder_name}/reviews.csv' INTO TABLE Reviews FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")

    db_connection.commit()
    cursor.close()