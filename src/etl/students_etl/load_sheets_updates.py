import os
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

from src.sheets_connect import init_google_sheets

# Load environment variables
load_dotenv()

SHEET_ID = os.getenv('SHEET_ID')
WORKSHEET_NAME = "main"


def update_practice_dates(transformed_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Update Google Sheets with the latest practice date for students who practiced.
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
            current_timestamp = record['current_timestamp']
            
            # Keep only the latest practice per student
            if phone_number not in student_practices:
                student_practices[phone_number] = record
            else:
                existing_timestamp = student_practices[phone_number]['current_timestamp']
                if current_timestamp > existing_timestamp:
                    student_practices[phone_number] = record
    
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
    expected_headers = ['phone_number', 'name', 'lesson', 'last_practice']
    if not all(h in headers for h in expected_headers):
        print(f"✗ Sheet headers don't match expected format")
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
    
    # Statistics
    stats = {
        'students_updated': 0,
        'students_not_found': 0,
        'errors': 0
    }
    
    # Build a map of phone numbers to row indices
    phone_to_row = {}
    for idx, row in enumerate(rows):
        if len(row) > phone_col_idx:
            phone_number = row[phone_col_idx].strip()
            phone_to_row[phone_number] = idx + 2  # +2 because: 0-indexed to 1-indexed, plus header row
    
    # Update each student's last practice date
    updates = []
    
    for phone_number, practice_record in student_practices.items():
        try:
            if phone_number not in phone_to_row:
                print(f"⚠ Student not found in sheet: {practice_record['name']} ({phone_number})")
                stats['students_not_found'] += 1
                continue
            
            # Get row number (1-indexed for Google Sheets)
            row_num = phone_to_row[phone_number]
            
            # Extract date only (no timestamp) in DD-MM-YYYY format
            practice_datetime = practice_record['current_timestamp']
            
            # Convert to datetime object if it isn't already
            if isinstance(practice_datetime, datetime):
                date_obj = practice_datetime
            elif isinstance(practice_datetime, str):
                # Parse string to datetime
                # Format is: "HH:MM, DD.MM.YYYY"
                try:
                    # Split by comma to separate time and date
                    if ',' in practice_datetime:
                        time_part, date_part = practice_datetime.split(',')
                        date_part = date_part.strip()  # "DD.MM.YYYY"
                        time_part = time_part.strip()  # "HH:MM"
                        
                        # Parse the date part only
                        date_obj = datetime.strptime(date_part, '%d.%m.%Y')
                    else:
                        # Try other common formats
                        if 'T' in practice_datetime:
                            date_obj = datetime.fromisoformat(practice_datetime.replace('Z', '+00:00'))
                        elif ' ' in practice_datetime:
                            date_obj = datetime.strptime(practice_datetime.split('.')[0], '%Y-%m-%d %H:%M:%S')
                        else:
                            date_obj = datetime.strptime(practice_datetime, '%Y-%m-%d')
                except Exception as parse_error:
                    print(f"  ⚠ Could not parse datetime '{practice_datetime}': {parse_error}")
                    continue
            else:
                print(f"  ⚠ Unexpected timestamp type: {type(practice_datetime)}")
                continue
            
            # Format as DD-MM-YYYY (date only)
            practice_date = date_obj.strftime('%d/%m/%Y')
            
            # Prepare cell update (column D is index 3, which is last_practice_col_idx)
            cell_address = f"{chr(65 + last_practice_col_idx)}{row_num}"  # Convert to A1 notation
            updates.append({
                'range': cell_address,
                'values': [[practice_date]]
            })
            
            print(f"✓ Queued update: {practice_record['name']} ({phone_number}) - {practice_date} at {cell_address}")
            stats['students_updated'] += 1
            
        except Exception as e:
            print(f"✗ Error processing {phone_number}: {e}")
            stats['errors'] += 1
    
    # Batch update all cells at once
    if updates:
        try:
            sheet.batch_update(updates)
            print(f"Successfully updated {len(updates)} cells in Google Sheets")
        except Exception as e:
            print(f"Failed to batch update Google Sheets: {e}")
            stats['errors'] += len(updates)
            stats['students_updated'] = 0
    
    print(f"{'='*60}")
    print(f"Google Sheets update complete:")
    print(f"  Students updated: {stats['students_updated']}")
    print(f"  Students not found in sheet: {stats['students_not_found']}")
    print(f"  Errors: {stats['errors']}")
    print(f"{'='*60}")
    
    return stats