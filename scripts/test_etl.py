import unittest
import os
import sqlite3
import pandas as pd
from etl_pipeline import extract, transform, load

class TestETLPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup paths
        cls.input_db = r"C:\Users\zadeboye\Documents\Database\Course_data_analysis\dev\cademycode.db"
        cls.temp_db = "test_cleaned.db"
        cls.temp_csv = "test_cleaned.csv"
        
        # Run extraction and transformation once for all tests
        tables, cls.raw_counts = extract(cls.input_db)
        cls.cleaned_students, cls.cleaned_jobs, cls.cleaned_courses = transform(tables)
        load(cls.cleaned_students, cls.cleaned_jobs, cls.cleaned_courses, cls.temp_db, cls.temp_csv)

    def test_schema_consistency(self):
        """Check that cleaned database tables exist with expected columns."""
        conn = sqlite3.connect(self.temp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = {row[0] for row in cursor.fetchall()}
        conn.close()

        self.assertIn("cademycode_students_clean", table_names)
        self.assertIn("student_jobs_clean", table_names)
        self.assertIn("courses_clean", table_names)

        # Optional: check for critical columns
        required_student_cols = {"student_id", "dob", "job_id"}
        self.assertTrue(required_student_cols.issubset(set(self.cleaned_students.columns)))

    def test_table_joins(self):
        """Ensure cleaned tables can join without losing critical keys."""
        merged = self.cleaned_students.merge(
            self.cleaned_jobs, on="job_id", how="left"
        ).merge(
            self.cleaned_courses, left_on="current_career_path_id", right_on="career_path_id", how="left"
        )
        # Should not be empty
        self.assertGreater(len(merged), 0)

    def test_row_counts_changed(self):
        """Ensure cleaning step actually modified rows (unless already clean)."""
        clean_counts = {
            "cademycode_students": len(self.cleaned_students),
            "student_jobs": len(self.cleaned_jobs),
            "courses": len(self.cleaned_courses)
        }
        # If raw table had rows, cleaned table should have <= raw rows
        for table in clean_counts:
            if self.raw_counts[table] > 0:
                self.assertLessEqual(clean_counts[table], self.raw_counts[table])

    @classmethod
    def tearDownClass(cls):
        # Clean up temp files
        if os.path.exists(cls.temp_db):
            os.remove(cls.temp_db)
        if os.path.exists(cls.temp_csv):
            os.remove(cls.temp_csv)


if __name__ == "__main__":
    unittest.main()
