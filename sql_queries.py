# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id serial,
                                start_timestamp TIMESTAMP WITHOUT TIME ZONE,
                                user_id int,
                                level varchar,
                                song_id varchar,
                                artist_id varchar,
                                session_id int,
                                location varchar,
                                user_agent varchar,
                                PRIMARY KEY (start_timestamp, user_id, session_id),
                                FOREIGN KEY (start_timestamp) REFERENCES time(timestamp),
                                FOREIGN KEY (user_id) REFERENCES users(user_id),
                                FOREIGN KEY (song_id) REFERENCES songs(song_id),
                                FOREIGN KEY (artist_id) REFERENCES artists(artist_id));
                        """)


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                user_id int PRIMARY KEY,
                                first_name varchar,
                                last_name varchar,
                                gender char(1),
                                level varchar);
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                song_id varchar PRIMARY KEY,
                                title varchar,
                                artist_id varchar REFERENCES artists,
                                year int,
                                duration float);
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id varchar PRIMARY KEY,
                                name varchar,
                                location varchar,
                                latitude float,
                                longitude float);
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                timestamp TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
                                start_time TIME,
                                hour int,
                                day int,
                                week int,
                                month int,
                                year int,
                                weekday int);
                    """)

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(start_timestamp, user_id, level, song_id, artist_id, session_id, location, user_agent)
                             SELECT DISTINCT %s, %s::INTEGER, %s, %s, %s, %s, %s, %s
                             ON CONFLICT ON CONSTRAINT songplays_pkey DO
                             UPDATE SET level = songplays.level, location = songplays.location, user_agent = songplays.user_agent;
                        """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT %s::INTEGER, %s, %s, %s, %s
                        ON CONFLICT (user_id) DO
                        UPDATE SET level = users.level;
                    """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT %s, %s, %s, %s, %s
                        ON CONFLICT (song_id) DO
                        UPDATE SET title = songs.title,
                                    artist_id = songs.artist_id,
                                    year = songs.year,
                                    duration = songs.duration;
                        """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                            SELECT DISTINCT %s, %s, %s, %s, %s
                            ON CONFLICT (artist_id) DO
                            UPDATE SET location = artists.location,
                                        latitude = artists.latitude,
                                        longitude = artists.longitude;
                        """)


time_table_insert = ("""INSERT INTO time(timestamp, start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT %s, %s, %s, %s, %s, %s, %s, %s
                        ON CONFLICT (timestamp) DO
                        UPDATE SET start_time = time.start_time,
                                    hour= time.hour,
                                    day = time.day,
                                    week = time.week,
                                    month = time.month,
                                    year = time.year,
                                    weekday = time.weekday;
                        """)

# FIND SONGS
song_select = ("""SELECT C.song_id, B.artist_id
                    FROM ((SELECT %s::VARCHAR as title, %s::VARCHAR as artist_name, ROUND(%s, 5)::FLOAT as duration) as A
                        LEFT JOIN artists B ON B.name = A.artist_name)
                        LEFT JOIN songs C ON C.title = A.title AND C.duration = A.duration;
""")


# QUERY LISTS

#create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]