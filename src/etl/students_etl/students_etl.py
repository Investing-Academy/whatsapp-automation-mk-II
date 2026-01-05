from src.etl.students_etl.transform import transform
from src.etl.students_etl.load_mongo_stats import load
from src.etl.students_etl.load_sheets_updates import update_practice_dates
from src.etl.students_etl.calculate_performance import calculate_all_student_performance
from src.etl.students_etl.load_teachers_sheet import sync_new_lessons_to_teachers_sheet

def run_students_etl(messages):
    transformed_data = transform(messages)

    # Load to MongoDB and track new lesson creations
    mongo_stats = load(transformed_data)

    # Sync new lessons to teachers payment tracking sheet
    # This must run after load() to ensure new lessons are detected
    sync_new_lessons_to_teachers_sheet(mongo_stats)

    # Update practice dates in student sheet
    update_practice_dates(transformed_data)

    # Calculate performance classifications for all students
    # This runs after MongoDB stats are updated and processes all students,
    # not just those with new messages in this batch
    calculate_all_student_performance()