from src.etl.students_etl.transform import transform
from src.etl.students_etl.load_mongo_stats import load
from src.etl.students_etl.load_sheets_updates import update_practice_dates

def run_students_etl(messages):

    transformed_data = transform(messages)
    load(transformed_data)
    update_practice_dates(transformed_data)