"""
Manual Student Data Insertion Utility

This script allows you to manually insert student documents into MongoDB.
You provide the core fields, and the script handles uniq_id generation and structure.

Usage:
    python manual_insert_student.py

Required fields you'll enter:
- Phone number (e.g., "972 55-660-2298")
- Name (e.g., "John Doe")
- Current lesson (e.g., "7")
- Lesson data (lesson number, teacher, practice_count, message_count, first_practice, last_practice)
"""

import hashlib
import sys
from datetime import datetime
from dotenv import load_dotenv

from src.etl.db.mongodb.mongo_handler import get_mongo_connection

# Load environment variables
load_dotenv()


def generate_uniq_id(phone_number: str, name: str) -> str:
    """Generate a unique ID by hashing phone number and name."""
    combined = f"{phone_number}_{name}"
    return hashlib.md5(combined.encode()).hexdigest()


def get_current_timestamp() -> str:
    """Get current timestamp in the format HH:MM, DD.MM.YYYY"""
    return datetime.now().strftime('%H:%M, %d.%m.%Y')


def validate_timestamp(timestamp: str) -> bool:
    """Validate timestamp format HH:MM, DD.MM.YYYY"""
    try:
        datetime.strptime(timestamp, '%H:%M, %d.%m.%Y')
        return True
    except ValueError:
        return False


def get_input(prompt: str, required: bool = True, validator=None) -> str:
    """Get user input with optional validation."""
    while True:
        value = input(prompt).strip()

        if not value and not required:
            return ""

        if not value and required:
            print("⚠ This field is required. Please enter a value.")
            continue

        if validator and not validator(value):
            print("⚠ Invalid format. Please try again.")
            continue

        return value


def add_lesson_interactive() -> dict:
    """Interactively collect lesson data from user."""
    print("\n" + "="*60)
    print("LESSON DATA ENTRY")
    print("="*60)

    lesson = get_input("Lesson number (e.g., 1, 2, 7): ")
    teacher = get_input("Teacher name: ")
    practice_count = get_input("Practice count (number): ")
    message_count = get_input("Message count (number): ")

    print("\nTimestamp format: HH:MM, DD.MM.YYYY (e.g., 14:30, 09.12.2025)")
    first_practice = get_input("First practice timestamp: ", validator=validate_timestamp)
    last_practice = get_input("Last practice timestamp: ", validator=validate_timestamp)

    try:
        practice_count = int(practice_count)
        message_count = int(message_count)
    except ValueError:
        print("⚠ Practice count and message count must be numbers!")
        return None

    return {
        'lesson': lesson,
        'teacher': teacher,
        'practice_count': practice_count,
        'message_count': message_count,
        'first_practice': first_practice,
        'last_practice': last_practice
    }


def create_student_document(phone_number: str, name: str, current_lesson: str, lessons: list) -> dict:
    """Create a complete student document with all required fields."""
    uniq_id = generate_uniq_id(phone_number, name)
    current_time = get_current_timestamp()

    # Find the last practice and message timestamps from lessons
    last_practice_timedate = None
    last_message_timedate = None

    if lessons:
        # Get the last lesson's last_practice as the student's last_practice_timedate
        last_practice_timedate = lessons[-1]['last_practice']
        # For messages, we'll use the last lesson's last_practice as well
        # (adjust this logic if you have specific message timestamps)
        last_message_timedate = lessons[-1]['last_practice']

    document = {
        'uniq_id': uniq_id,
        'phone_number': phone_number,
        'name': name,
        'current_lesson': current_lesson,
        'last_message_timedate': last_message_timedate or current_time,
        'last_practice_timedate': last_practice_timedate or current_time,
        'lessons': lessons,
        'created_at': current_time,
        'updated_at': current_time
    }

    return document


def print_document_preview(document: dict):
    """Print a preview of the document to be inserted."""
    print("\n" + "="*60)
    print("DOCUMENT PREVIEW")
    print("="*60)
    print(f"uniq_id: {document['uniq_id']}")
    print(f"phone_number: {document['phone_number']}")
    print(f"name: {document['name']}")
    print(f"current_lesson: {document['current_lesson']}")
    print(f"last_message_timedate: {document['last_message_timedate']}")
    print(f"last_practice_timedate: {document['last_practice_timedate']}")
    print(f"created_at: {document['created_at']}")
    print(f"updated_at: {document['updated_at']}")
    print(f"\nLessons ({len(document['lessons'])}):")
    for lesson in document['lessons']:
        print(f"  Lesson {lesson['lesson']}:")
        print(f"    Teacher: {lesson['teacher']}")
        print(f"    Practice count: {lesson['practice_count']}")
        print(f"    Message count: {lesson['message_count']}")
        print(f"    First practice: {lesson['first_practice']}")
        print(f"    Last practice: {lesson['last_practice']}")
    print("="*60)


def main():
    """Main function to run the manual insertion utility."""
    print("="*60)
    print("MANUAL STUDENT DATA INSERTION UTILITY")
    print("="*60)
    print("\nThis utility will guide you through creating a new student document.")
    print("You can add one or multiple lessons for the student.\n")

    # Get basic student information
    print("STUDENT INFORMATION")
    print("-"*60)
    phone_number = get_input("Phone number (e.g., '972 55-660-2298'): ")
    name = get_input("Student name: ")
    current_lesson = get_input("Current lesson number: ")

    # Collect lesson data
    lessons = []
    while True:
        lesson_data = add_lesson_interactive()

        if lesson_data is None:
            print("⚠ Skipping lesson due to invalid data.")
            continue

        lessons.append(lesson_data)

        add_more = get_input("\nAdd another lesson? (y/n): ", required=False)
        if add_more.lower() not in ['y', 'yes']:
            break

    if not lessons:
        print("⚠ No lessons added. Cannot create student document without lessons.")
        sys.exit(1)

    # Create the document
    document = create_student_document(phone_number, name, current_lesson, lessons)

    # Show preview
    print_document_preview(document)

    # Confirm insertion
    confirm = get_input("\nInsert this document into MongoDB? (y/n): ")
    if confirm.lower() not in ['y', 'yes']:
        print("❌ Insertion cancelled.")
        sys.exit(0)

    # Insert into MongoDB
    try:
        mongo_conn = get_mongo_connection()
        stats_collection = mongo_conn.get_students_stats_collection()

        # Check if student already exists
        existing = stats_collection.find_one({'uniq_id': document['uniq_id']})
        if existing:
            print(f"\n⚠ WARNING: Student with uniq_id '{document['uniq_id']}' already exists!")
            print(f"   Name: {existing.get('name')}")
            print(f"   Phone: {existing.get('phone_number')}")

            overwrite = get_input("\nOverwrite existing document? (y/n): ")
            if overwrite.lower() not in ['y', 'yes']:
                print("❌ Insertion cancelled.")
                sys.exit(0)

            # Update existing document
            result = stats_collection.replace_one(
                {'uniq_id': document['uniq_id']},
                document
            )
            print(f"\n✓ Successfully updated existing student document!")
            print(f"  Modified count: {result.modified_count}")
        else:
            # Insert new document
            result = stats_collection.insert_one(document)
            print(f"\n✓ Successfully inserted new student document!")
            print(f"  Inserted ID: {result.inserted_id}")

        print(f"  uniq_id: {document['uniq_id']}")
        print(f"  Name: {document['name']}")
        print(f"  Lessons: {len(document['lessons'])}")

    except Exception as e:
        print(f"\n✗ Failed to insert document: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Operation cancelled by user.")
        sys.exit(0)
