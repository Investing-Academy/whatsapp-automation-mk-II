import os
from typing import Dict, List
from dotenv import load_dotenv

from src.sheets_connect import init_google_sheets
from src.etl.db.mongodb.mongo_handler import get_mongo_connection

# Load environment variables
load_dotenv()

SHEET_ID = os.getenv('SHEET_ID')
HELPER_WORKSHEET_NAME = "helper"


def update_helper_sheet_stats():
    """
    Update the helper sheet with total statistics from MongoDB.
    Updates two cells:
    - J2: Total practices count across all students
    - K2: Total messages count across all students

    Returns:
        Dictionary with update statistics
    """
    print(f"{'='*60}")
    print(f"Updating helper sheet with total statistics")
    print(f"{'='*60}")

    # Initialize Google Sheets connection
    try:
        client = init_google_sheets()
        if not client:
            raise Exception("Failed to initialize Google Sheets client")

        spreadsheet = client.open_by_key(SHEET_ID)
        sheet = spreadsheet.worksheet(HELPER_WORKSHEET_NAME)
    except Exception as e:
        print(f"✗ Failed to connect to Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

    # Connect to MongoDB and fetch all student stats
    mongo_conn = None
    total_practices = 0
    total_messages = 0

    try:
        mongo_conn = get_mongo_connection()
        stats_collection = mongo_conn.get_students_stats_collection()

        # Fetch all student records from MongoDB
        all_students = stats_collection.find({})

        for student in all_students:
            lessons = student.get('lessons', [])

            # Sum up practice_count and message_count from all lessons
            for lesson in lessons:
                total_practices += lesson.get('practice_count', 0)
                total_messages += lesson.get('message_count', 0)

        print(f"✓ Calculated totals from MongoDB:")
        print(f"  Total practices: {total_practices}")
        print(f"  Total messages: {total_messages}")

    except Exception as e:
        print(f"✗ Failed to fetch student data from MongoDB: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

    # Update the helper sheet cells
    try:
        updates = [
            {
                'range': 'J2',
                'values': [[total_practices]]
            },
            {
                'range': 'K2',
                'values': [[total_messages]]
            }
        ]

        sheet.batch_update(updates)
        print(f"✓ Successfully updated helper sheet:")
        print(f"  J2 (total_practices): {total_practices}")
        print(f"  K2 (total_messages): {total_messages}")

        print(f"{'='*60}")
        print(f"Helper sheet update complete")
        print(f"{'='*60}")

        return {
            'success': True,
            'total_practices': total_practices,
            'total_messages': total_messages
        }

    except Exception as e:
        print(f"✗ Failed to update helper sheet: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


def update_teacher_stats():
    """
    Update the helper sheet with teacher statistics from MongoDB.
    Updates three columns starting from row 2:
    - L: Teacher names (one per row)
    - M: Total messages for each teacher
    - N: Total practices for each teacher

    Returns:
        Dictionary with update statistics
    """
    print(f"{'='*60}")
    print(f"Updating helper sheet with teacher statistics")
    print(f"{'='*60}")

    # Initialize Google Sheets connection
    try:
        client = init_google_sheets()
        if not client:
            raise Exception("Failed to initialize Google Sheets client")

        spreadsheet = client.open_by_key(SHEET_ID)
        sheet = spreadsheet.worksheet(HELPER_WORKSHEET_NAME)
    except Exception as e:
        print(f"✗ Failed to connect to Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

    # Connect to MongoDB and aggregate teacher statistics
    mongo_conn = None
    teacher_stats = {}

    try:
        mongo_conn = get_mongo_connection()
        stats_collection = mongo_conn.get_students_stats_collection()

        # Fetch all student records from MongoDB
        all_students = stats_collection.find({})

        for student in all_students:
            lessons = student.get('lessons', [])

            # Aggregate stats by teacher
            for lesson in lessons:
                teacher = lesson.get('teacher', '').strip()

                # Skip empty teacher names
                if not teacher:
                    continue

                if teacher not in teacher_stats:
                    teacher_stats[teacher] = {
                        'messages': 0,
                        'practices': 0
                    }

                teacher_stats[teacher]['messages'] += lesson.get('message_count', 0)
                teacher_stats[teacher]['practices'] += lesson.get('practice_count', 0)

        print(f"✓ Calculated statistics for {len(teacher_stats)} teachers:")
        for teacher, stats in sorted(teacher_stats.items()):
            print(f"  {teacher}: {stats['practices']} practices, {stats['messages']} messages")

    except Exception as e:
        print(f"✗ Failed to fetch student data from MongoDB: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

    # Sort teachers alphabetically for consistent ordering
    sorted_teachers = sorted(teacher_stats.keys())

    # Prepare data for batch update
    try:
        # Prepare lists for each column
        teacher_names = [[teacher] for teacher in sorted_teachers]
        teacher_messages = [[teacher_stats[teacher]['messages']] for teacher in sorted_teachers]
        teacher_practices = [[teacher_stats[teacher]['practices']] for teacher in sorted_teachers]

        updates = []

        if teacher_names:
            # Calculate the range for each column
            end_row = 2 + len(sorted_teachers) - 1

            # Column L: Teacher names
            updates.append({
                'range': f'L2:L{end_row}',
                'values': teacher_names
            })

            # Column M: Total messages
            updates.append({
                'range': f'M2:M{end_row}',
                'values': teacher_messages
            })

            # Column N: Total practices
            updates.append({
                'range': f'N2:N{end_row}',
                'values': teacher_practices
            })

            # Clear any old data below the current range
            # Get the current last row with data in column L
            try:
                existing_data = sheet.get(f'L2:L')
                if existing_data and len(existing_data) > len(sorted_teachers):
                    clear_start = end_row + 1
                    clear_end = 2 + len(existing_data) - 1
                    # Clear old rows
                    updates.append({
                        'range': f'L{clear_start}:N{clear_end}',
                        'values': [['', '', '']] * (clear_end - clear_start + 1)
                    })
            except:
                pass  # If no existing data, no need to clear

        # Batch update all cells at once
        if updates:
            sheet.batch_update(updates)
            print(f"✓ Successfully updated helper sheet with {len(sorted_teachers)} teachers")
        else:
            print(f"⚠ No teacher data to update")

        print(f"{'='*60}")
        print(f"Teacher statistics update complete")
        print(f"{'='*60}")

        return {
            'success': True,
            'teachers_count': len(sorted_teachers),
            'teacher_stats': teacher_stats
        }

    except Exception as e:
        print(f"✗ Failed to update teacher sheet statistics: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }
