import sqlite3
import pandas as pd
import os
import logging

# --- Configure logger ---
log_path = os.path.join(
    r'C:\Users\zadeboye\Documents\Database\Course_data_analysis\prod\Logs', 
    'etl.log'
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def extract(db_path):
    logger.info(f"Extracting data from {db_path}")
    conn = sqlite3.connect(db_path)
    tables = {
        'cademycode_students': pd.read_sql_query("SELECT * FROM cademycode_students", conn),
        'student_jobs': pd.read_sql_query("SELECT * FROM cademycode_student_jobs", conn),
        'courses': pd.read_sql_query("SELECT * FROM cademycode_courses", conn)
    }
    conn.close()

    raw_counts = {name: len(df) for name, df in tables.items()}
    logger.info(f'Row counts before cleaning: {raw_counts}')
    return tables, raw_counts


def transform(tables):
    logger.info("Transforming data")
    cademycode_students = tables['cademycode_students']
    student_jobs = tables['student_jobs']
    courses = tables['courses']

    # --- Cleaning ---
    cademycode_students['dob'] = pd.to_datetime(cademycode_students['dob'], errors='coerce')
    cademycode_students['time_spent_hrs'] = pd.to_numeric(cademycode_students['time_spent_hrs'], errors='coerce')
    cols = ['job_id', 'num_course_taken', 'current_career_path_id']
    cademycode_students[cols] = cademycode_students[cols].apply(pd.to_numeric, errors='coerce').astype('Int64')

    student_jobs = student_jobs.drop_duplicates()
    cademycode_students = cademycode_students.dropna(subset=['job_id'])
    cademycode_students = cademycode_students.dropna(subset=['current_career_path_id', 'time_spent_hrs', 'num_course_taken'])

    if 'contact_info' in cademycode_students.columns:
        contact_df = cademycode_students['contact_info'].apply(
            lambda x: pd.Series(eval(x)) if pd.notna(x) else pd.Series({'email': None, 'phone': None})
        )
        cademycode_students = pd.concat([cademycode_students.drop(columns=['contact_info']), contact_df], axis=1)

    cademycode_students.rename(columns={'uuid':'student_id'}, inplace=True)

    student_jobs = student_jobs[student_jobs['job_id'].notna()]
    cademycode_students = cademycode_students[cademycode_students['student_id'].notna()]

    clean_counts = {
        'cademycode_students': len(cademycode_students),
        'student_jobs': len(student_jobs),
        'courses': len(courses)
    }
    logger.info(f'Row counts after cleaning: {clean_counts}')

    return cademycode_students, student_jobs, courses


def load(cademycode_students, student_jobs, courses, out_db, out_csv):
    logger.info(f"Loading cleaned data into {out_db} and {out_csv}")
    conn = sqlite3.connect(out_db)
    cademycode_students.to_sql("cademycode_students_clean", conn, if_exists="replace", index=False)
    student_jobs.to_sql("student_jobs_clean", conn, if_exists="replace", index=False)
    courses.to_sql("courses_clean", conn, if_exists="replace", index=False)
    conn.close()

    merged = cademycode_students.merge(student_jobs, on="job_id", how="left") \
                                .merge(courses, left_on='current_career_path_id', right_on='career_path_id', how='left')
    merged.to_csv(out_csv, index=False)
    logger.info("Load complete — cleaned tables written to database and CSV")


def main():
    try:
        logger.info("ETL pipeline started")
        tables, raw_counts = extract(r"C:\Users\zadeboye\Documents\Database\Course_data_analysis\dev\cademycode.db")
        cademycode_students, student_jobs, courses = transform(tables)
        load(
            cademycode_students, student_jobs, courses,
            out_db=r'C:\Users\zadeboye\Documents\Database\Course_data_analysis\prod\cleaned_cademycode_dev.db',
            out_csv=os.path.join(r'C:\Users\zadeboye\Documents\Database\Course_data_analysis\prod', 'clean_data.csv')
        )
        logger.info("ETL pipeline completed successfully!")
    except Exception as e:
        logger.exception("ETL pipeline failed due to an error")
        raise


if __name__ == "__main__":
    main()
