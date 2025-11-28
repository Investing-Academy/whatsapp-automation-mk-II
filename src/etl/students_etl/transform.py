import re
import os
from dotenv import load_dotenv
from typing import Dict, List, Optional
from datetime import datetime
from src.sheets_connect import init_google_sheets

# Load environment variables
load_dotenv()

# Get search words from environment and parse them
practice_search_word = os.getenv("PRACTICE_WORDS", "")
message_search_word = os.getenv("MESSAGE_WORDS", "")
sheet_id = os.getenv("SHEET_ID")

# Parse the strings into lists
PRACTICE_WORDS = [word.strip().strip('"').strip("'") 
                  for word in practice_search_word.strip('[]').split(',') 
                  if word.strip()]
MESSAGE_WORDS = [word.strip().strip('"').strip("'") 
                 for word in message_search_word.strip('[]').split(',') 
                 if word.strip()]


def load_student_data() -> tuple[Dict[str, Dict], Dict[str, Dict]]:
    """
    Load student data from Google Sheets
    Sheet columns: A=phone number, B=name, C=status, E=teacher
    
    Returns:
        tuple: (phone_to_data dict, name_to_data dict)
    """
    try:
        client = init_google_sheets()
        if not client:
            print("Failed to initialize Google Sheets client")
            return {}, {}
        
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet("main")
        
        # Get all values
        all_values = worksheet.get_all_values()
        
        print(f"Total rows in sheet: {len(all_values)}")
        print(f"First row (header): {all_values[0] if all_values else 'Empty'}")
        
        phone_to_data = {}
        name_to_data = {}
        
        # Skip header row
        for idx, row in enumerate(all_values[1:], start=2):
            if not row or len(row) < 2:
                continue
                
            phone = row[0].strip() if len(row) > 0 and row[0] else ""
            name = row[1].strip() if len(row) > 1 and row[1] else ""
            status = row[2].strip() if len(row) > 2 and row[2] else ""
            teacher = row[4].strip() if len(row) > 4 and row[4] else ""
            
            # Debug: Print first few rows
            if idx <= 5:
                print(f"Row {idx}: phone='{phone}', name='{name}', status='{status}', teacher='{teacher}'")
            
            # Clean phone number
            if phone:
                cleaned_phone = clean_phone_number(phone)
                student_data = {
                    'phone': cleaned_phone,
                    'name': name,
                    'lesson': status,
                    'teacher': teacher
                }
                phone_to_data[cleaned_phone] = student_data
                
                # Also map by name for reverse lookup
                if name:
                    name_to_data[name.lower()] = student_data
        
        print(f"Loaded {len(phone_to_data)} students from Google Sheets")
        print(f"Sample phone numbers in mapping: {list(phone_to_data.keys())[:3]}")
        return phone_to_data, name_to_data
    
    except Exception as e:
        print(f"Error loading Google Sheets data: {e}")
        import traceback
        traceback.print_exc()
        return {}, {}


def clean_phone_number(phone: str) -> str:
    """
    Clean and normalize phone numbers.
    - Removes Unicode direction marks, spaces, symbols.
    - Detects Israeli numbers and formats: 972 52-299-1474
    - For other countries: returns +<digits> (E.164 style)
    """
    # Remove direction marks, whitespace, and invisible characters
    cleaned = re.sub(r'[\u2066\u2069\u200e\u200f\s]', '', phone)
    
    # Fix "+" if inserted between characters like "+ 972"
    cleaned = cleaned.replace("+", "")
    
    # Keep only digits
    digits = re.sub(r'\D', "", cleaned)
    
    # If empty → return empty
    if not digits:
        return ""
    
    # Local IL number (10 digits starting with 05X)
    if len(digits) == 10 and digits.startswith("05"):
        intl = "972" + digits[1:]
        return f"{intl[:3]} {intl[3:5]}-{intl[5:8]}-{intl[8:]}"
    
    # Already international Israeli (starts with 972)
    if digits.startswith("972") and len(digits) == 12:
        return f"{digits[:3]} {digits[3:5]}-{digits[5:8]}-{digits[8:]}"
    
    return digits


def contains_keyword(text: str, keywords: List[str]) -> bool:
    """Check if text contains any of the keywords"""
    text_lower = text.lower()
    return any(keyword.lower() in text_lower for keyword in keywords)


def extract_student_info(sender: str, phone_to_data: Dict, name_to_data: Dict) -> Dict:
    """
    Extract student information from sender
    If sender is phone - add name, if sender is name - add phone
    
    Returns:
        Dict with phone, name, lesson, and teacher
    """
    # Try as phone number first
    cleaned_phone = clean_phone_number(sender)
    
    print(f"Looking up sender: '{sender}' -> cleaned: '{cleaned_phone}'")
    
    if cleaned_phone in phone_to_data:
        print(f"  Found in phone_to_data!")
        return phone_to_data[cleaned_phone]
    
    # Try as name
    sender_lower = sender.lower().strip()
    if sender_lower in name_to_data:
        print(f"  Found in name_to_data!")
        return name_to_data[sender_lower]
    
    print(f"  Not found in sheets data")
    # Not found - return basic info
    return {
        'phone': cleaned_phone if cleaned_phone else sender,
        'name': sender if not cleaned_phone else "Unknown",
        'lesson': "Unknown",
        'teacher': "Unknown"
    }


def process_messages(messages: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Process messages and create two types of outputs:
    1. Student stats updates (for student_stats collection)
    2. Message history (for message_history collection)
    
    Returns:
        Dict with two keys:
        - 'stats_updates': List of student stat updates
        - 'message_history': List of full message records
    """
    # Load student data from Google Sheets
    phone_to_data, name_to_data = load_student_data()
    
    # DEBUG: Print keywords being used
    print(f"\n=== Keyword Configuration ===")
    print(f"Practice keywords: {PRACTICE_WORDS}")
    print(f"Message keywords: {MESSAGE_WORDS}")
    
    # Dictionary to aggregate stats per student
    student_stats = {}
    message_history = []
    
    practice_count = 0
    message_count = 0
    unmatched_count = 0
    
    for idx, msg in enumerate(messages):
        # DEBUG: Print first few messages
        if idx < 3:
            print(f"\n--- Processing message {idx + 1} ---")
            print(f"Sender: {msg['sender']}")
            print(f"Text: {msg['text'][:100]}...")
        
        # Get student info from Google Sheets
        student_info = extract_student_info(msg['sender'], phone_to_data, name_to_data)
        phone = student_info['phone']
        current_lesson = student_info['lesson']
        timestamp = msg['timestamp']
        
        # Initialize student stats if first time seeing this student
        if phone not in student_stats:
            student_stats[phone] = {
                'phone_number': phone,
                'name': student_info['name'],
                'teacher': student_info['teacher'],
                'current_lesson': current_lesson,
                'total_messages': 0,
                'last_message_date': None,
                'practices': {},  # Temporary dict for easy lookup
                'updated_at': timestamp
            }
        
        # Determine message type and update counters
        msg_type = None
        
        if contains_keyword(msg['text'], PRACTICE_WORDS):
            msg_type = "practice"
            practice_count += 1
            if idx < 3:
                print(f"✓ Matched as PRACTICE")
            
            # Update practice counter for current lesson
            lesson_key = current_lesson
            if lesson_key not in student_stats[phone]['practices']:
                student_stats[phone]['practices'][lesson_key] = {
                    'lesson': current_lesson,
                    'count': 0,
                    'first_practice': timestamp,
                    'last_practice': timestamp
                }
            
            # Increment practice count
            student_stats[phone]['practices'][lesson_key]['count'] += 1
            student_stats[phone]['practices'][lesson_key]['last_practice'] = timestamp
            
        elif contains_keyword(msg['text'], MESSAGE_WORDS):
            msg_type = "message"
            message_count += 1
            
            # Increment message counter
            student_stats[phone]['total_messages'] += 1
            student_stats[phone]['last_message_date'] = timestamp
            if idx < 3:
                print(f"✓ Matched as MESSAGE")
        else:
            unmatched_count += 1
            if idx < 3:
                print(f"✗ No keyword match - message IGNORED")
        
        # Update timestamp
        student_stats[phone]['updated_at'] = timestamp
        
        # Add to message history (for message_history collection)
        if msg_type:
            message_history.append({
                'phone_number': phone,
                'name': student_info['name'],
                'teacher': student_info['teacher'],
                'lesson': current_lesson,
                'message_category': msg_type,
                'content': msg['text'],
                'timestamp': timestamp
            })
    
    # Convert practices dict to list for MongoDB
    stats_updates = []
    for phone, stats in student_stats.items():
        # Convert practices dict to list with practice_count key
        practice_entries = []
        for entry in stats['practices'].values():
            practice_entries.append({
                'lesson': entry['lesson'],
                'practice_count': entry['count'],
                'first_practice': entry['first_practice'],
                'last_practice': entry['last_practice']
            })
        stats['practices'] = practice_entries

        # Ensure top-level 'timestamp' is always set for downstream consumers
        stats['timestamp'] = stats['updated_at']
        stats_updates.append(stats)
    
    print(f"\n=== Processing Summary ===")
    print(f"Total messages received: {len(messages)}")
    print(f"Total students processed: {len(student_stats)}")
    print(f"Practice messages: {practice_count}")
    print(f"General messages: {message_count}")
    print(f"Unmatched messages (ignored): {unmatched_count}")
    print(f"Total messages for history: {len(message_history)}")
    
    
    return {
        'stats_updates': stats_updates,
        'message_history': message_history
    }


def get_stats_upsert_operations(stats_updates: List[Dict]) -> List[Dict]:
    """
    Generate MongoDB upsert operations for student_stats collection.
    This will either update existing student docs or create new ones.
    
    Usage with pymongo:
        from pymongo import UpdateOne
        operations = [
            UpdateOne(
                {'phone_number': op['filter']['phone_number']},
                op['update'],
                upsert=True
            )
            for op in get_stats_upsert_operations(stats_updates)
        ]
        db.student_stats.bulk_write(operations)
    
    Returns:
        List of operation dictionaries
    """
    operations = []
    
    for stats in stats_updates:
        phone = stats['phone_number']
        
        # For each practice lesson, we need to either:
        # 1. Increment count if lesson exists
        # 2. Add new lesson entry if it doesn't exist
        
        operation = {
            'filter': {'phone_number': phone},
            'update': {
                '$set': {
                    'name': stats['name'],
                    'teacher': stats['teacher'],
                    'current_lesson': stats['current_lesson'],
                    'updated_at': stats['updated_at']
                },
                '$inc': {
                    'total_messages': stats['total_messages']
                },
                '$setOnInsert': {
                    'created_at': stats['updated_at']
                }
            }
        }
        
        # Update last_message_date if exists
        if stats.get('last_message_date'):
            operation['update']['$set']['last_message_date'] = stats['last_message_date']
        
        # Handle practices array updates
        # Note: This is a simplified approach. For production, you might want
        # to use a more sophisticated update that handles concurrent updates better
        for practice in stats['practices']:
            lesson = practice['lesson']
            # This will need to be handled in your database layer
            # as MongoDB array updates are complex
            operation['update'].setdefault('$push', {})
            operation['update']['practices_to_merge'] = stats['practices']
        
        operations.append(operation)
    
    return operations