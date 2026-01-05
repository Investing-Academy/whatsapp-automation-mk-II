import os
from typing import List, Dict, Any
from datetime import datetime
from dotenv import load_dotenv
import hashlib

from src.sheets_connect import init_google_sheets
from src.etl.db.mongodb.mongo_handler import get_mongo_connection, MongoDBConnection

# Load environment variables
load_dotenv()

TEACHERS_SHEET_ID = os.getenv('TEACHERS_SHEET_ID')


def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse timestamp string in multiple formats to datetime object.
    Supports:
    - ISO 8601: '2025-12-02T16:15:42.998+00:00'
    - Custom format: 'HH:MM, DD.MM.YYYY'
    """
    try:
        # Try ISO 8601 format first
        if 'T' in timestamp_str:
            # Handle ISO format with timezone
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        else:
            # Handle custom format
            return datetime.strptime(timestamp_str, '%H:%M, %d.%m.%Y')
    except Exception as e:
        print(f"Error parsing timestamp '{timestamp_str}': {e}")
        raise


def format_timestamp(dt: datetime) -> str:
    """
    Format datetime object to string in format 'HH:MM, DD.MM.YYYY'
    Uses zero-padded format for consistency
    """
    return dt.strftime('%H:%M, %d.%m.%Y')


def generate_teacher_payment_id(phone_number: str, lesson: str) -> str:
    """
    Generate a unique ID for teacher payment tracking.
    Combines phone_number and lesson for uniqueness.
    """
    combined = f"{phone_number}_{lesson}"
    return hashlib.md5(combined.encode()).hexdigest()


def extract_new_lessons_from_stats(stats_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract information about newly created lessons from load_mongo_stats results.

    This function needs to be called after load_mongo_stats to detect which lessons
    were newly created (student progressed to new lesson).

    Returns: List of dicts with lesson info for teacher payment tracking
    """
    new_lessons = []

    if not stats_data or 'new_lessons_created' not in stats_data:
        return new_lessons

    # Extract new lesson data
    for lesson_info in stats_data['new_lessons_created']:
        new_lessons.append({
            'phone_number': lesson_info['phone_number'],
            'name': lesson_info['name'],
            'lesson': lesson_info['lesson'],
            'teacher': lesson_info['teacher'],
            'date_added': lesson_info['created_timestamp']  # Use lesson creation timestamp
        })

    return new_lessons


def sync_new_lessons_to_teachers_sheet(mongo_stats_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sync newly created lessons to the teachers payment tracking sheet.

    Process:
    1. Extract new lessons from MongoDB stats result
    2. Check if each lesson already exists in MongoDB teacher_payments collection
    3. If not exists, add to Google Sheets and MongoDB

    Args:
        mongo_stats_result: Result dict from load_mongo_stats containing new_lessons_created

    Returns:
        Dict with statistics about synced lessons
    """
    if not TEACHERS_SHEET_ID:
        print("⚠ TEACHERS_SHEET_ID not configured - skipping teachers sheet sync")
        return {
            'lessons_synced': 0,
            'duplicates_skipped': 0,
            'errors': 0
        }

    print(f"\n{'='*60}")
    print("Starting Teachers Sheet Sync")
    print(f"{'='*60}")

    # Initialize Google Sheets
    try:
        gc = init_google_sheets()
        if not gc:
            raise Exception("Failed to initialize Google Sheets connection")

        sheet = gc.open_by_key(TEACHERS_SHEET_ID).sheet1
    except Exception as e:
        print(f"✗ Error connecting to teachers sheet: {e}")
        import traceback
        traceback.print_exc()
        return {
            'lessons_synced': 0,
            'duplicates_skipped': 0,
            'errors': 1
        }

    # Get MongoDB connection
    mongo_conn = get_mongo_connection()
    teacher_payments_collection = mongo_conn.get_teacher_payments_collection()

    # Extract new lessons from stats
    new_lessons = extract_new_lessons_from_stats(mongo_stats_result)

    if not new_lessons:
        print("No new lessons to sync")
        print(f"{'='*60}\n")
        return {
            'lessons_synced': 0,
            'duplicates_skipped': 0,
            'errors': 0
        }

    print(f"Found {len(new_lessons)} new lessons to process")

    stats = {
        'lessons_synced': 0,
        'duplicates_skipped': 0,
        'errors': 0
    }

    rows_to_append = []

    for lesson_info in new_lessons:
        try:
            phone_number = lesson_info['phone_number']
            lesson_num = lesson_info['lesson']

            # Generate unique ID for deduplication
            payment_id = generate_teacher_payment_id(phone_number, lesson_num)

            # Check if this lesson already exists in MongoDB
            existing_payment = teacher_payments_collection.find_one({'payment_id': payment_id})

            if existing_payment:
                stats['duplicates_skipped'] += 1
                print(f"  ⚠ Skipping duplicate: {lesson_info['name']} - Lesson {lesson_num}")
                continue

            # Parse date_added timestamp
            date_added_str = lesson_info['date_added']

            # Prepare row for Google Sheets
            # Headers: Student Phone Number, Student Name, Lesson, Teacher, Paid, Date Added
            row = [
                phone_number,
                lesson_info['name'],
                lesson_num,
                lesson_info['teacher'],
                'FALSE',  # Always set Paid to FALSE initially
                date_added_str  # Keep in HH:MM, DD.MM.YYYY format
            ]

            rows_to_append.append(row)

            # Save to MongoDB for deduplication tracking
            teacher_payments_collection.update_one(
                {'payment_id': payment_id},
                {
                    '$set': {
                        'phone_number': phone_number,
                        'name': lesson_info['name'],
                        'lesson': lesson_num,
                        'teacher': lesson_info['teacher'],
                        'paid': False,
                        'date_added': date_added_str,
                        'updated_at': MongoDBConnection.get_current_timestamp()
                    },
                    '$setOnInsert': {
                        'created_at': MongoDBConnection.get_current_timestamp()
                    }
                },
                upsert=True
            )

            stats['lessons_synced'] += 1
            print(f"  ✓ Added: {lesson_info['name']} - Lesson {lesson_num} (Teacher: {lesson_info['teacher']})")

        except Exception as e:
            stats['errors'] += 1
            print(f"  ✗ Error processing lesson for {lesson_info.get('name', 'Unknown')}: {e}")
            import traceback
            traceback.print_exc()

    # Insert new rows at row 2 (pushing existing data down)
    if rows_to_append:
        try:
            num_new_rows = len(rows_to_append)

            # Step 1: Insert blank rows at row 2 to create space
            sheet.insert_rows(values=[[]] * num_new_rows, row=2)
            print(f"  → Created {num_new_rows} blank rows at row 2")

            # Step 2: Update the newly created rows with our data
            # Rows are now at positions 2, 3, 4, etc.
            start_row = 2
            end_row = 2 + num_new_rows - 1

            # Build the range string (e.g., "A2:F4" for 3 rows with 6 columns)
            num_columns = len(rows_to_append[0])
            end_column_letter = chr(ord('A') + num_columns - 1)
            range_name = f"A{start_row}:{end_column_letter}{end_row}"

            # Update the range with our data
            sheet.update(range_name, rows_to_append, value_input_option='USER_ENTERED')
            print(f"\n💾 Inserted {num_new_rows} rows at top of teachers sheet (row 2)")

        except Exception as e:
            print(f"✗ Error inserting rows to teachers sheet: {e}")
            import traceback
            traceback.print_exc()
            stats['errors'] += len(rows_to_append)
            stats['lessons_synced'] -= len(rows_to_append)

    print(f"\n{'='*60}")
    print(f"Teachers Sheet Sync Complete:")
    print(f"  Lessons synced: {stats['lessons_synced']}")
    print(f"  Duplicates skipped: {stats['duplicates_skipped']}")
    print(f"  Errors: {stats['errors']}")
    print(f"{'='*60}\n")

    return stats


if __name__ == '__main__':
    """
    Test the teachers sheet sync functionality.
    Usage: python -m src.etl.students_etl.load_teachers_sheet
    """
    # Example test data
    test_data = {
        'new_lessons_created': [
            {
                'phone_number': '972 55-660-2298',
                'name': 'Test Student',
                'lesson': '7',
                'teacher': 'Test Teacher',
                'created_timestamp': '14:30, 09.12.2025'
            }
        ]
    }

    print("Testing teachers sheet sync with sample data...")
    result = sync_new_lessons_to_teachers_sheet(test_data)
    print(f"Test result: {result}")
