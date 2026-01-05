import os
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

from src.sheets_connect import init_google_sheets
from src.etl.db.mongodb.mongo_handler import get_mongo_connection

# Load environment variables
load_dotenv()

SHEET_ID = os.getenv('SHEET_ID')
WORKSHEET_NAME = "Students"


def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse timestamp string to datetime object.
    Handles multiple formats:
    - '18:51, 12/4/2025' (24-hour format with M/D/YYYY)
    - '6:51 PM, 12/4/2025' (12-hour format with AM/PM)
    - 'HH:MM, DD.MM.YYYY' (24-hour format with D.M.YYYY)
    """
    formats = [
        '%H:%M, %m/%d/%Y',      # '18:51, 12/4/2025'
        '%I:%M %p, %m/%d/%Y',   # '6:51 PM, 12/4/2025'
        '%H:%M, %d.%m.%Y',      # 'HH:MM, DD.MM.YYYY'
    ]

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue

    # If none of the formats worked, raise an error
    print(f"Error parsing timestamp '{timestamp_str}': Does not match any known format")
    raise ValueError(f"Could not parse timestamp: {timestamp_str}")


def format_lessons_array(lessons: List[Dict[str, Any]]) -> str:
    """
    Format lessons array from MongoDB into a readable string for Google Sheets.

    Format: "L1(P:3,M:2), L2(P:5,M:1), L3(P:2,M:0)"
    Where P = practice_count, M = message_count

    Args:
        lessons: List of lesson dictionaries from MongoDB

    Returns:
        Formatted string representation
    """
    if not lessons:
        return ""

    # Sort lessons by lesson number
    try:
        sorted_lessons = sorted(lessons, key=lambda x: int(x.get('lesson', 0)))
    except (ValueError, TypeError):
        sorted_lessons = lessons

    formatted_parts = []
    for lesson in sorted_lessons:
        lesson_num = lesson.get('lesson', '?')

        # Skip lessons with empty lesson numbers
        if not lesson_num or lesson_num.strip() == '':
            continue

        practice_count = lesson.get('practice_count', 0)
        message_count = lesson.get('message_count', 0)

        formatted_parts.append(f"L{lesson_num}(P:{practice_count},M:{message_count})")

    return ", ".join(formatted_parts)


def update_practice_dates(transformed_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Update Google Sheets with the latest practice date for students who practiced.
    Updates three columns:
    - last_practice: Date in DD/MM/YYYY format
    - new_practice: TRUE boolean indicator for new practice submission
    - lesson_progress: Formatted lessons array from MongoDB (e.g., "L1(P:3,M:2), L2(P:5,M:1)")

    Only updates students who have practice records in the current batch.

    Args:
        transformed_records: List of transformed records from ETL

    Returns:
        Dictionary with update statistics
    """
    if not transformed_records:
        print("No records to update in Google Sheets")
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 0
        }
    
    print(f"{'='*60}")
    print(f"Updating Google Sheets with practice dates")
    print(f"{'='*60}")
    
    # Initialize Google Sheets connection
    try:
        client = init_google_sheets()
        if not client:
            raise Exception("Failed to initialize Google Sheets client")
        
        spreadsheet = client.open_by_key(SHEET_ID)
        sheet = spreadsheet.worksheet(WORKSHEET_NAME)
    except Exception as e:
        print(f"✗ Failed to connect to Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 1
        }
    
    # Filter only practice records and get the latest practice per student
    student_practices = {}
    
    for record in transformed_records:
        if record['message_type'] == 'practice':
            phone_number = record['phone_number']
            current_timestamp_str = record['current_timestamp']
            
            # Parse timestamp string to datetime for proper comparison
            try:
                current_timestamp = parse_timestamp(current_timestamp_str)
            except Exception as e:
                print(f"⚠ Could not parse timestamp '{current_timestamp_str}': {e}")
                continue
            
            # Keep only the latest practice per student
            if phone_number not in student_practices:
                student_practices[phone_number] = {
                    'record': record,
                    'timestamp': current_timestamp
                }
            else:
                existing_timestamp = student_practices[phone_number]['timestamp']
                if current_timestamp > existing_timestamp:
                    student_practices[phone_number] = {
                        'record': record,
                        'timestamp': current_timestamp
                    }
    
    if not student_practices:
        print("No practice records found in this batch")
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 0
        }
    
    print(f"Found {len(student_practices)} students with practice records")
    
    # Get all data from sheet (assuming headers in row 1)
    try:
        all_data = sheet.get_all_values()
        headers = all_data[0] if all_data else []
        rows = all_data[1:] if len(all_data) > 1 else []
    except Exception as e:
        print(f"✗ Failed to read sheet data: {e}")
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 1
        }
    
    # Validate headers
    expected_headers = ['phone_number', 'name', 'lesson', 'last_practice', 'new_practice', 'lesson_progress']
    missing_headers = [h for h in expected_headers if h not in headers]

    if missing_headers:
        print(f"✗ Sheet headers don't match expected format")
        print(f"  Missing headers: {missing_headers}")
        print(f"  Expected: {expected_headers}")
        print(f"  Found: {headers}")
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 1
        }

    # Find column indices
    phone_col_idx = headers.index('phone_number')
    last_practice_col_idx = headers.index('last_practice')
    new_practice_col_idx = headers.index('new_practice')  # Column I for TRUE/FALSE indicator
    lesson_progress_col_idx = headers.index('lesson_progress')  # Column M for lessons array
    
    # Statistics
    stats = {
        'students_updated': 0,
        'students_not_found': 0,
        'errors': 0
    }
    
    # Connect to MongoDB to fetch lessons array for each student
    mongo_conn = None
    student_data_from_mongo = {}

    try:
        mongo_conn = get_mongo_connection()
        stats_collection = mongo_conn.get_students_stats_collection()

        # Fetch all student records from MongoDB
        all_students = stats_collection.find({})
        for student in all_students:
            phone_number = student.get('phone_number', '')
            student_data_from_mongo[phone_number] = {
                'lessons': student.get('lessons', [])
            }

        print(f"✓ Fetched {len(student_data_from_mongo)} students from MongoDB")
    except Exception as e:
        print(f"⚠ Could not fetch student data from MongoDB: {e}")
        import traceback
        traceback.print_exc()
        # Continue without MongoDB data - lesson_progress will be empty

    # Build a map of phone numbers to row indices
    phone_to_row = {}
    for idx, row in enumerate(rows):
        if len(row) > phone_col_idx:
            phone_number = row[phone_col_idx].strip()
            phone_to_row[phone_number] = idx + 2  # +2 because: 0-indexed to 1-indexed, plus header row
    
    # Update each student's last practice date
    updates = []
    
    for phone_number, practice_data in student_practices.items():
        try:
            practice_record = practice_data['record']
            practice_timestamp = practice_data['timestamp']
            
            if phone_number not in phone_to_row:
                print(f"⚠ Student not found in sheet: {practice_record['name']} ({phone_number})")
                stats['students_not_found'] += 1
                continue
            
            # Get row number (1-indexed for Google Sheets)
            row_num = phone_to_row[phone_number]

            # Format as DD/MM/YYYY (date only)
            practice_date = practice_timestamp.strftime('%d/%m/%Y')

            # Prepare cell updates for last_practice, new_practice, and lesson_progress columns
            # Update last_practice column (date)
            last_practice_cell = f"{chr(65 + last_practice_col_idx)}{row_num}"
            updates.append({
                'range': last_practice_cell,
                'values': [[practice_date]]
            })

            # Update new_practice column (TRUE indicator)
            new_practice_cell = f"{chr(65 + new_practice_col_idx)}{row_num}"
            updates.append({
                'range': new_practice_cell,
                'values': [[True]]  # Boolean TRUE
            })

            # Update lesson_progress column (lessons array from MongoDB)
            lesson_progress_text = ""
            if phone_number in student_data_from_mongo:
                lessons_array = student_data_from_mongo[phone_number].get('lessons', [])
                lesson_progress_text = format_lessons_array(lessons_array)

            lesson_progress_cell = f"{chr(65 + lesson_progress_col_idx)}{row_num}"
            updates.append({
                'range': lesson_progress_cell,
                'values': [[lesson_progress_text]]
            })

            print(f"✓ Queued update: {practice_record['name']} ({phone_number}) - {practice_date} at {last_practice_cell}, TRUE at {new_practice_cell}, Progress: {lesson_progress_text}")
            stats['students_updated'] += 1
            
        except Exception as e:
            print(f"✗ Error processing {phone_number}: {e}")
            import traceback
            traceback.print_exc()
            stats['errors'] += 1
    
    # Update lesson_progress for ALL students in the sheet (not just those with new practice)
    print(f"\nUpdating lesson_progress for all students in sheet...")
    lesson_progress_updates = []
    students_with_progress = 0

    for phone_number, row_num in phone_to_row.items():
        try:
            # Skip if already processed in the practice updates above
            if phone_number in student_practices:
                continue

            # Get lessons array from MongoDB
            if phone_number in student_data_from_mongo:
                lessons_array = student_data_from_mongo[phone_number].get('lessons', [])
                lesson_progress_text = format_lessons_array(lessons_array)

                if lesson_progress_text:  # Only update if there's actual progress data
                    lesson_progress_cell = f"{chr(65 + lesson_progress_col_idx)}{row_num}"
                    lesson_progress_updates.append({
                        'range': lesson_progress_cell,
                        'values': [[lesson_progress_text]]
                    })
                    students_with_progress += 1
        except Exception as e:
            print(f"✗ Error updating lesson_progress for {phone_number}: {e}")

    print(f"✓ Queued {students_with_progress} lesson_progress updates for students without new practice")

    # Combine all updates
    all_updates = updates + lesson_progress_updates

    # Batch update all cells at once
    if all_updates:
        try:
            sheet.batch_update(all_updates)
            print(f"✓ Successfully updated {len(all_updates)} cells in Google Sheets")
        except Exception as e:
            print(f"✗ Failed to batch update Google Sheets: {e}")
            import traceback
            traceback.print_exc()
            stats['errors'] += len(all_updates)
            stats['students_updated'] = 0

    print(f"{'='*60}")
    print(f"Google Sheets update complete:")
    print(f"  Students with new practice: {stats['students_updated']}")
    print(f"  Students with lesson_progress updated: {students_with_progress}")
    print(f"  Students not found in sheet: {stats['students_not_found']}")
    print(f"  Errors: {stats['errors']}")
    print(f"{'='*60}")

    return stats