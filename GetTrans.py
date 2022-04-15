import argparse
# used in dnld_save_trans_csv() ? Think about renoving
import csv
from datetime import datetime
import json
import logging
import os
import pandas as pd
from pathlib import Path
import re
import sqlite3
import sys
import time
# from sqlite3 import Error # ????????
from youtube_transcript_api import YouTubeTranscriptApi
from wordcloud import WordCloud, ImageColorGenerator
from tqdm import tqdm


def main():

    # config first =====================
    # Read in values from  shared config file
    version = "0.10.05"
    trans_base_dir = "../transcripts"
    config_log_file = "../logs/GetTrans.log"

    failed_get_file = "Failed_transcript_download.log"
    vid_info_lookup_file = "VideoId_Date_Title_Ref.csv"

    wordcloud_size = (250, 300)

    # Args overide configs =======================================================
    args = config_argsparse(version).parse_args()

    # Logfile for application
    log_file = args.logFile if args.logFile else config_log_file
    log_level = args.logLevel if args.logLevel else ""
    if log_level == "critical":
        log_level = logging.CRITICAL
    elif log_level == "debug":
        log_level = logging.DEBUG
    elif log_level == "info":
        log_level = logging.INFO
    elif log_level == "warning":
        log_level = logging.WARNING
    elif log_level == "error":
        log_level = logging.ERROR
    elif log_level == "notset":
        log_level = logging.NOTSET
    else:
        log_level = logging.WARNING

    logging.basicConfig(filename=log_file,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S', level=log_level)

    debug = args.debug
    corpus = args.corpus
    language = args.lang
    cust_stop_words = args.stopwords
    # print(cust_stop_words)

    if debug:
        trans_base_dir = f"{trans_base_dir}Test"
    trans_dir = f'{trans_base_dir}/{corpus}'

    if language and language != 'en':
        trans_dir = f'{trans_dir}/{language}'

    if debug:
        logging.info(
            f"===== DEBUG ===== DEBUG ===== {trans_dir} ===== DEBUG ===== DEBUG =====")
    else:
        logging.info(
            f"============================= {trans_dir} =============================")

    logging.info(vars(args))

    progress = args.progress
    wordcloud = args.wordcloud
    # wordcloud = False
    sqlite_db_name = args.db

    # one of these inputs required
    you_text = args.youText
    you_chan = args.youChan     # now is a seperate project youtube-python-api-client
    you_ids = args.youIds       # Not implimented (simple id list)

    # ====== Process job
    t0 = time.time()
    logging.info(f"Execution start time: {t0}")

    process_job(you_text, trans_dir, corpus, wordcloud, wordcloud_size, sqlite_db_name,
                vid_info_lookup_file, failed_get_file, progress, cust_stop_words, language)
    logging.info("Finished processing you_text input job")

    t1 = time.time()
    totalTime = t1-t0
    logging.info(f"Execution end time: {t0} - duration {totalTime}")

# ====================================================================================================
# ??? This could be the start of a module for the youText option ???


def process_job(job_file, trans_dir, corpus, wordcloud, wordcloud_size, db_name, vid_info_lookup_file, failed_get_file, progress, custom_stop_words, language='en'):
    """ get transcripts 
        make word cloud 
        optionally save db """
    conn = ""                       # sqlite database connection
    job_arr = []                    # the list of video ids to process
    video_id_to_count_db = {}       # track if transcripts exist in db
    video_id_to_count = {}          # track if transcripts exist in trans dir
    wordcloud_to_count = {}         # track if wordClouds exist
    video_id_to_title_ref = {}      # lookup show (title, ref) for video id
    # failed trans download dict for file creation / tracking
    video_id_to_title_fails = {}

    Path(trans_dir).mkdir(parents=True, exist_ok=True)
    logging.info(f"Using Directory {trans_dir} for the corpus of {corpus}")

    # Option DB ~ setup DB stuff
    if db_name:
        # from DB
        # HCK #### language,
        db_location = f"{trans_dir}/{db_name}"
        conn = db_create_connection(db_location)
        db_create_video_table(conn)
        load_dict_from_db(video_id_to_count_db, conn)  # only use the job ???
        logging.info("Created video table in db DB")

    load_dict_from_dir(video_id_to_count, trans_dir, ".csv", "_transcript")
    logging.info(
        "video_id_to_count for existing transcript items tracking has loaded from directory")

    load_dict_from_dir(wordcloud_to_count, trans_dir, ".png", "_wordcloud")
    logging.info(
        "wordcloud_to_count for existing wordcloud items tracking has loaded from directory")
    # wordcloud_to_count = {} # Remove this

    load_lookup_array_from_csv(job_file, trans_dir, video_id_to_title_ref)
    logging.info(
        "video_id_to_title_ref for lookup has loaded from the csv file")

    make_lookup_file_from_csv(
        job_file, trans_dir, corpus, vid_info_lookup_file)
    logging.info("make the title geDoc lookup file")

    load_job_arrray_from_csv(job_file, trans_dir, corpus, job_arr)
    logging.info("job_arr has loaded from the csv file")

    for video_id, job in job_arr:

        trans_import_lines = []  # array of transcript lines to import to db

        # 1. CSV donwload and save
        get_save_csv(video_id, video_id_to_count, trans_dir,
            trans_import_lines, video_id_to_title_fails, language)

        # 2. Build wordcloud - not optomised for using saved text .. cache if just downloaded in step 1 above.
        if wordcloud:
            build_wordcloud(video_id, wordcloud_to_count, video_id_to_count,
                            trans_dir, wordcloud_size, custom_stop_words)

        # 3. Optionally, save to sqlite database - not used for much yet .. still designing
        if db_name:
            save_to_db(conn, video_id, job, trans_import_lines,
            video_id_to_count_db, video_id_to_title_fails)

        # if debug : sys.stderr.write(f'vid_id : {vidId}\n')
        # if progress : sys.stderr.flush()

        # logger.flush() # not this easy See https://docs.python.org/3/howto/logging.html#logging-advanced-tutorial


    if len(video_id_to_title_fails):
        today = datetime.now().strftime("%y%m%d %H%M%S")
        failed_get_trans_log = f'{trans_dir}/{corpus}_{failed_get_file}'
        with open(failed_get_trans_log, 'a+', encoding="utf-8") as cvs_trans_failed_file:
            if len(video_id_to_title_fails):
                cvs_trans_failed_file.write(
                    "# ==========================================================================================\n")
            for k, v in video_id_to_title_fails.items():
                cvs_trans_failed_file.write(
                    f'[{today}] {k} {video_id_to_title_ref[k][0]} because {v}\n')

    #### RETURN JSON
    outString = {
        "attempted":  length(job_arr),
        "failed": len(video_id_to_title_fails)
    }

    print(json.dumps(outString))


# =====================================================================================================


def load_job_arrray_from_csv(job_file, trans_dir, corpus, job_arr):
    """ load the jobs to be processed by reading transcript directory
    into the job array.
    fields: pub_date,title,video,ref,thumb,desc
    """
    df_jobs = pd.read_csv(job_file, header=None)
    df_jobs = df_jobs.fillna('')

    # I think we should break with the 4 field csv and use the 5 fiels with date.
    # patch the other input files to be v2??

    for _, job in df_jobs.iterrows():
        video_id = 0

        if job[2]:
            video_url = re.sub(r'[\s+]', '', job[2])
            m = re.search("(.{11})$", video_url)
            if m:
                video_id = m.group(1)
                job_arr.append(
                    [video_id, [job[0], job[1], job[2], job[3], job[4], job[5]]])
        else:
            if job[1]:
                with open(f'{trans_dir}/{corpus}_noVideo.log', 'a+') as noVidAvailable:
                    noVidAvailable.write(
                        f"{job[1]} have no video available \n")


    

def load_lookup_array_from_csv(job_file, trans_dir, video_id_to_title_ref):
    """ load the jobs to be processed by reading transcript directory into the job array. """
    df_jobs = pd.read_csv(job_file, header=None)
    df_jobs = df_jobs.fillna('')

    for _, job in df_jobs.iterrows():
        video_id = 0
        if job[2]:
            video_url = re.sub(r'[\s+]', '', job[2])
            m = re.search("(.{11})$", video_url)
            if m:
                video_id = m.group(1)
                video_id_to_title_ref[video_id] = (job[1], job[3])


def make_lookup_file_from_csv(job_file, trans_dir, corpus, lookup_file):
    """ Make a lookup file for searchTrans to load for convenience """
    array_vid_title_ref = []
    df_jobs = pd.read_csv(job_file, header=None)
    df_jobs = df_jobs.fillna('')

    for _, job in df_jobs.iterrows():
        video_id = 0
        if job[2]:
            video_url = re.sub(r'[\s+]', '', job[2])
            m = re.search("(.{11})$", video_url)
            if m:
                video_id = m.group(1)
                array_vid_title_ref.append((video_id, job[0], job[1], job[3]))

    df_jobs_new = pd.DataFrame(array_vid_title_ref, columns=[
        'videoId', 'date', 'title', 'ref'])
    df_jobs_new.to_csv(f'{trans_dir}/{corpus}_{lookup_file}')  # , index=False


def load_dict_from_dir(dict, trans_dir, ext, contextStr):
    """ For duplicate / existence tracking for updates and """
    directory = os.path.join(trans_dir)
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(ext):
                pass
                m = re.search("(.{11})" + contextStr + ext, file)
                if m:
                    k = m.group(1)
                    dict[k] = 1


def load_dict_from_db(video_id_to_count, conn):
    df = pd.read_sql(
        'select transcript_table_name as video_id, show as ShowTitle, ref from videos', conn)

    # use a comprehension syntax *** https://docs.python.org/3/tutorial/datastructures.html#list-comprehensions
    for ind, row in df.iterrows():
        video_id = row[0]
        video_id_to_count[video_id] = 1
    return


def dnld_save_trans_csv(video_id, trans_dir, trans_import_lines, language):
    """
    Downloads transcript from YouTubeTranscriptApi.get_transcript(video_id)
    Saves {video_id}_transcript.csv & returns list of the transcript lines.
    Returned lines could be bulk inserted in db as secondary storage an future design. 
    """
    vid_trans = ''
    try:
        vid_trans = YouTubeTranscriptApi.get_transcript(
            video_id, languages=[language])
    except Exception as err:
        logging.error(
            f'{err} {video_id} transcription download encountered an exception')
        raise err

    if not vid_trans:
        logging.error(
            f'download: {video_id} transcription download attempted but empty?')
        return trans_import_lines

    logging.info(video_id + " transcription extraction in progress ...")

    # save into trans_import_lines for sqlite bulk import
    Path(trans_dir).mkdir(parents=True, exist_ok=True)
    with open(f'{trans_dir}/{video_id}_transcript.csv', 'w', newline='', encoding="utf-8") as cvsTransFile:
        writer = csv.writer(cvsTransFile)
        for segment in vid_trans:
            row_data = list(segment.values())
            writer.writerow(row_data)
            trans_import_lines.append(row_data)
            # insert_transcripts(conn, row_data, video_id)  # !!! TOOO SLOW !!! use CSV import technique instead.

# Database functions


def db_create_connection(db_location):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None

    try:
        conn = sqlite3.connect(db_location)
        logging.info(
            f"SQLite version {sqlite3.version} connected to {db_location}")
    except Error as e:
        print(e)

    return conn


def db_create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def db_create_transcript_table(conn, video_id):

    sql_db_create_transcript_table = """CREATE TABLE IF NOT EXISTS [""" + video_id + """](
                                    id TEXT PRIMARY KEY,
                                    spoken TEXT,
                                    speaker TEXT,
                                    seg_start INTEGER,
                                    seg_duration INTEGER
                                );"""
    db_create_table(conn, sql_db_create_transcript_table)
    # print(sql_db_create_transcript_table)


def db_create_video_table(conn):
    sql_create_videos_table = """ CREATE TABLE IF NOT EXISTS videos (
                                id integer PRIMARY KEY,
                                date TEXT,
                                show TEXT NOT NULL,
                                ref TEXT,
                                video TEXT,
                                transcript_table_name TEXT NOT NULL UNIQUE,
                                thumbnail TEXT,
                                description TEXT
                            );"""
    db_create_table(conn, sql_create_videos_table)


def db_insert_video_rec(conn, video_id, video_data):
    if (len(video_data) == 4):
        m = re.search(r"(\d{8})", video_data[0])
        if m:
            vid_date = m.group(0)
        else:
            vid_date = ""
        video_data.insert(0, vid_date)

    video_data.insert(4, video_id)

    sql = '''INSERT or IGNORE INTO videos(date,show,video,ref,transcript_table_name,thumbnail,description) VALUES(?,?,?,?,?,?,?)'''

    cur = conn.cursor()
    try:
        cur.execute(sql, video_data)
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        raise(e)


def db_add_transcript(conn, video_id, trans_import_lines):

    # OPTIMIZATION BULK LOAD HACK
    # print(trans_import_lines[0:5])
    # Insert 'id,' and speaker field placeholder
    trans_import_lines_modified = []
    line_id = 0
    for idx, line in enumerate(trans_import_lines):
        line.insert(0, idx)
        line.insert(2, "")
        trans_import_lines_modified.append(line)

    db_create_transcript_table(conn, video_id)
    cur = conn.cursor()
    sql = ''' INSERT INTO [''' + video_id + \
        '''](id,spoken,speaker,seg_start,seg_duration) VALUES(?,?,?,?,?); '''

    try:
        cur.executemany(sql, trans_import_lines_modified)
        conn.commit()
        return cur.lastrowid
    except Error as e:
        raise(e)


def save_to_db(conn, video_id, job, trans_import_lines, video_id_to_count_db, video_id_to_title_fails):
    # check if table exists
    attempt_store_in_db = True
    sql = f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{video_id}';"
    c = conn.cursor()
    try:
        c.execute(sql)
        if c.fetchone()[0] == 1:
            logging.info(
                f"Table exists for transcript of {video_id} ~ {video_id_to_count_db[video_id]}, skipping")
            attempt_store_in_db = False
    except Exception as err:
        video_id_to_title_fails[
            video_id] = f'Exception : failed db lookup check if table exists for: {err}'
        attempt_store_in_db = False
        logging.error(
            f"{video_id} failed db lookup check if table exists -> {err}")

    if attempt_store_in_db:
        if len(trans_import_lines):
            try:
                db_add_transcript(conn, video_id, trans_import_lines)
                db_insert_video_rec(conn, video_id, job)
                logging.info(f"Stored transcript in DB for {video_id}")
            except Exception as err:
                video_id_to_title_fails[
                    video_id] = f'Exception : problem inserting trans in db: {err}'
                logging.error(
                    f"{video_id} problem inserting trans in db -> {err}")
        else:
            video_id_to_title_fails[video_id] = 'trans_import_lines empty'
            logging.error(f"{video_id} trans_import_lines empty")


def get_save_csv(video_id, video_id_to_count, trans_dir, trans_import_lines, video_id_to_title_fails, language):
    ''' CSV donwload and save '''

    if video_id_to_count.get(video_id, 0):
        video_id_to_count[video_id] = video_id_to_count[video_id] + 1
        logging.info(
            f"Already have transcript {video_id} ~ {video_id_to_count[video_id]}, skipping")
    else:
        try:
            dnld_save_trans_csv(video_id, trans_dir,
                                trans_import_lines, language)
            video_id_to_count[video_id] = 1   # HCK test
            logging.info(f"Saved csv transcript for {video_id}")
        except Exception as err:
            # exception_type = type(err).__name__ __cause__ __context__
            video_id_to_title_fails[
                video_id] = f'Exception : transcript download and save csv attempt : {err}'
            logging.error(
                f"{video_id} transcript download and save csv -> {err}")


def build_wordcloud(video_id, wordcloud_to_count, video_id_to_count, trans_dir, wordcloud_size, custom_stop_words):
    ''' Assumes directory exists '''

    stop_words = set([
        "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
        "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
        "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further",
        "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
        "hers", "herself", "him", "himself", "his", "how", "how's",
        "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself",
        "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not",
        "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours	ourselves", "out", "over", "own",
        "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
        "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these",
        "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very",
        "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where",
        "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't",
        "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yoursel",
        "Um", "Uh"
    ])
    stop_words.update(custom_stop_words)

    if wordcloud_to_count.get(video_id, 0):
        wordcloud_to_count[video_id] = wordcloud_to_count[video_id] + 1
        logging.info(
            f"Already have wordcloud {video_id} ~ {wordcloud_to_count[video_id]}, skipping")
    else:
        if video_id_to_count.get(video_id, 0):
            df = pd.read_csv(
                f'{trans_dir}/{video_id}_transcript.csv', header=None)
            fileText = ''.join(df[0])
            # error handeling for wordcloud call ???

            try:
                wordcloud = WordCloud(
                    width=wordcloud_size[0], stopwords=stop_words, height=wordcloud_size[1]).generate(fileText)
                wordcloud.to_file(f'{trans_dir}/{video_id}_wordcloud.png')
            except Exception as err:
                logging.error(
                    f"{video_id} failed to create wordcloud -> {err}")
            else:
                logging.info(f"Created wordcloud for {video_id}")


def config_argsparse(version):
    parser = argparse.ArgumentParser(prog='python ./GetTrans.py',
                                     #usage='%(prog)s [options] [-f outputFile]',
                                     allow_abbrev=False,
                                     description='update / create transcripts download store.',
                                     epilog='Enjoy the program! :)')

    getGroup = parser.add_mutually_exclusive_group(required=True)
    parser.version = f'{version}'

    parser.add_argument('-d', '--debug',
                        action='store_true',
                        help='set debug mode for testing and diagnostics')

    parser.add_argument('-p', '--progress',
                        action='store_true',
                        help='show progress of search')

    parser.add_argument('-c', '--corpus',
                        type=str,
                        action='store',
                        metavar='corpus',
                        required=True,
                        help='prefix for storing different corpus')

    parser.add_argument('--stopwords',
                        nargs='*',
                        metavar='stopwords to ignore in wordcloud"',
                        default='',
                        help='stop words to ignore in wordcloud generation')

    parser.add_argument('--lang',
                        type=str,
                        action='store',
                        metavar='de',
                        default='en',
                        help='language of transcript defaults en, english')

    parser.add_argument('-v', action='version')

    parser.add_argument('-w', '--wordcloud',
                        action='store_true',
                        help='generate wordcloud files')

    getGroup.add_argument('--youChan',
                          type=str,
                          action='store',
                          metavar='channel',
                          help='download transcripts youtube channel address')

    getGroup.add_argument('--youText',
                          type=str,
                          action='store',
                          metavar='youText',
                          help='download transcripts youtube video csv job list')

    getGroup.add_argument('--youIds',
                          nargs='*',
                          type=str,
                          action='store',
                          metavar='youIds',
                          help='download transcripts from youtube ids on command line')

    parser.add_argument('--db',
                        type=str,
                        action='store',
                        metavar='sqliteDBname',
                        help='database name. CSV is always kept')

    parser.add_argument('-f', '--logFile',
                        type=str,
                        action='store',
                        metavar='logfilename',
                        help='LogFile (default GetTrans.log)')

    parser.add_argument('-l', '--logLevel',
                        type=str,
                        action='store',
                        choices=['critical', 'debug', 'info',
                                 'warning', 'error', 'notset'],
                        metavar='logLevel',
                        help='the logging level')

    return parser


# Standard boilerplate to call the main() function to begin the program.
if __name__ == '__main__':
    main()

# HCK to try again [210816 210430] iwQxOl2qE0E  "20180327 GCC P2P Discovery intersections: Helmut Leitner and Tammy Lea Meye" because trans_import_lines empty
# HCK to try again [210816 210430] GgKeDyHMT0I  "20180323 GCC P2P Discovery: Helmut Leitner" because trans_import_lines empty
