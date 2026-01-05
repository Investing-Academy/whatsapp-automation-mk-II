"""
Student Performance Classification Module

Calculates performance metrics and classifications for students based on:
- Practice efficiency (practice_count vs cohort average)
- Learning speed (lesson_time_days vs cohort average)

Classifications:
- Star: Low practice count + Fast time (natural talent)
- High Runner: High practice count + Fast time (hard worker)
- Normal: Average or slower performance
- Insufficient Data: Cohort size < 10 students

See starts.md for complete specification.
"""

import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import Counter

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from dotenv import load_dotenv
load_dotenv()

from src.etl.db.mongodb.mongo_handler import get_mongo_connection, MongoDBConnection

# Configuration
MINIMUM_COHORT_SIZE = 10  # Minimum students required for meaningful averages
TOTAL_LESSONS = 18

# Chapter definitions
CHAPTER_A_LESSONS = list(range(1, 7))    # Lessons 1-6
CHAPTER_B_LESSONS = list(range(7, 12))   # Lessons 7-11
LESSON_12 = [12]                          # Lesson 12 (standalone)
CHAPTER_C_LESSONS = list(range(13, 19))  # Lessons 13-18


def calculate_lesson_time_days(first_practice: str, last_practice: str) -> int:
    """
    Calculate the number of days between first and last practice for a lesson.

    Args:
        first_practice: Timestamp string in format "HH:MM, DD.MM.YYYY"
        last_practice: Timestamp string in format "HH:MM, DD.MM.YYYY"

    Returns:
        Number of days (minimum 1 if same day)
    """
    if not first_practice or not last_practice:
        return 0

    try:
        first_dt = MongoDBConnection.parse_timestamp(first_practice)
        last_dt = MongoDBConnection.parse_timestamp(last_practice)

        # Calculate days difference
        days_diff = (last_dt - first_dt).days

        # Minimum 1 day (same-day completion = 1 day)
        return max(1, days_diff)
    except Exception as e:
        print(f"⚠ Error calculating lesson time: {e}")
        return 0


def get_cohort_stats_for_lesson(lesson_num: int, all_students: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate cohort statistics for a specific lesson.

    Args:
        lesson_num: Lesson number (1-18)
        all_students: List of all student documents from MongoDB

    Returns:
        Dictionary with:
        - cohort_size: Number of students who completed this lesson
        - cohort_avg_practice: Average practice_count
        - cohort_avg_time_days: Average lesson_time_days
    """
    completed_students = []

    for student in all_students:
        lessons = student.get('lessons', [])

        # Find this specific lesson
        lesson_obj = next((l for l in lessons if str(l.get('lesson')) == str(lesson_num)), None)

        if not lesson_obj:
            continue

        # Check if lesson is completed (has both first_practice and last_practice)
        first_practice = lesson_obj.get('first_practice')
        last_practice = lesson_obj.get('last_practice')

        if not first_practice or not last_practice:
            continue

        # Calculate lesson time
        lesson_time = calculate_lesson_time_days(first_practice, last_practice)
        practice_count = lesson_obj.get('practice_count', 0)

        if lesson_time > 0:  # Valid completion
            completed_students.append({
                'practice_count': practice_count,
                'lesson_time_days': lesson_time
            })

    cohort_size = len(completed_students)

    if cohort_size == 0:
        return {
            'cohort_size': 0,
            'cohort_avg_practice': 0,
            'cohort_avg_time_days': 0
        }

    # Calculate averages
    avg_practice = sum(s['practice_count'] for s in completed_students) / cohort_size
    avg_time = sum(s['lesson_time_days'] for s in completed_students) / cohort_size

    return {
        'cohort_size': cohort_size,
        'cohort_avg_practice': round(avg_practice, 2),
        'cohort_avg_time_days': round(avg_time, 2)
    }


def classify_student_for_lesson(
    student_practice: int,
    student_time: int,
    cohort_avg_practice: float,
    cohort_avg_time: float,
    cohort_size: int
) -> str:
    """
    Classify student performance for a specific lesson.

    Args:
        student_practice: Student's practice_count for this lesson
        student_time: Student's lesson_time_days for this lesson
        cohort_avg_practice: Cohort average practice_count
        cohort_avg_time: Cohort average lesson_time_days
        cohort_size: Number of students in cohort

    Returns:
        Classification: "star" | "high_runner" | "normal" | "insufficient_data"
    """
    # Check minimum cohort size
    if cohort_size < MINIMUM_COHORT_SIZE:
        return "insufficient_data"

    # Star: Low practice + Fast time
    if student_practice < cohort_avg_practice and student_time < cohort_avg_time:
        return "star"

    # High Runner: High practice + Fast time
    if student_practice >= cohort_avg_practice and student_time < cohort_avg_time:
        return "high_runner"

    # Normal: Everything else
    return "normal"


def calculate_overall_classification(classifications: List[str]) -> str:
    """
    Calculate overall student classification based on most common classification.

    Args:
        classifications: List of lesson classifications (e.g., ["star", "high_runner", "normal"])

    Returns:
        Most common classification (tie-breaker: star > high_runner > normal)
    """
    if not classifications:
        return "normal"

    # Count classifications
    counts = Counter(classifications)

    # Remove insufficient_data from consideration
    if "insufficient_data" in counts:
        del counts["insufficient_data"]

    if not counts:
        return "normal"

    # Get most common
    most_common = counts.most_common()

    # If tie, use priority: star > high_runner > normal
    max_count = most_common[0][1]
    tied_classifications = [c for c, count in most_common if count == max_count]

    if "star" in tied_classifications:
        return "star"
    elif "high_runner" in tied_classifications:
        return "high_runner"
    else:
        return "normal"


def calculate_chapter_summary(
    lessons: List[Dict[str, Any]],
    chapter_lesson_nums: List[int],
    current_lesson: int
) -> Dict[str, Any]:
    """
    Calculate performance summary for a specific chapter.

    Args:
        lessons: Student's lessons array
        chapter_lesson_nums: List of lesson numbers in this chapter
        current_lesson: Student's current lesson number

    Returns:
        Chapter summary with classification counts and overall classification
    """
    chapter_classifications = []
    stars = 0
    high_runners = 0
    normal = 0
    completed_lessons = 0

    for lesson_num in chapter_lesson_nums:
        # Only count lessons student has progressed past
        if current_lesson <= lesson_num:
            continue

        # Find lesson object
        lesson_obj = next((l for l in lessons if str(l.get('lesson')) == str(lesson_num)), None)

        if not lesson_obj:
            continue

        classification = lesson_obj.get('classification')

        if not classification or classification == "insufficient_data":
            continue

        completed_lessons += 1
        chapter_classifications.append(classification)

        if classification == "star":
            stars += 1
        elif classification == "high_runner":
            high_runners += 1
        elif classification == "normal":
            normal += 1

    # Calculate chapter overall classification
    chapter_classification = calculate_overall_classification(chapter_classifications) if chapter_classifications else None

    return {
        'classification': chapter_classification,
        'stars': stars,
        'high_runners': high_runners,
        'normal': normal,
        'completed_lessons': completed_lessons
    }


def calculate_performance_for_student(
    student: Dict[str, Any],
    cohort_stats: Dict[int, Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calculate all performance metrics for a single student.

    Args:
        student: Student document from MongoDB
        cohort_stats: Dictionary mapping lesson_num -> cohort statistics

    Returns:
        Updated student document with performance fields
    """
    current_lesson_str = student.get('current_lesson', '0')
    try:
        current_lesson = int(current_lesson_str)
    except (ValueError, TypeError):
        current_lesson = 0

    lessons = student.get('lessons', [])
    all_classifications = []
    stars_count = 0
    high_runners_count = 0
    normal_count = 0
    total_lessons_classified = 0

    # Process each lesson
    for lesson_obj in lessons:
        lesson_num_str = lesson_obj.get('lesson', '0')

        try:
            lesson_num = int(lesson_num_str)
        except (ValueError, TypeError):
            continue

        # Only calculate if student has progressed past this lesson
        if current_lesson <= lesson_num:
            lesson_obj['lesson_time_days'] = None
            lesson_obj['classification'] = None
            lesson_obj['cohort_avg_practice'] = None
            lesson_obj['cohort_avg_time_days'] = None
            lesson_obj['cohort_size'] = None
            continue

        # Check if lesson is completed
        first_practice = lesson_obj.get('first_practice')
        last_practice = lesson_obj.get('last_practice')

        if not first_practice or not last_practice:
            lesson_obj['lesson_time_days'] = None
            lesson_obj['classification'] = None
            lesson_obj['cohort_avg_practice'] = None
            lesson_obj['cohort_avg_time_days'] = None
            lesson_obj['cohort_size'] = None
            continue

        # Calculate lesson time
        lesson_time = calculate_lesson_time_days(first_practice, last_practice)
        lesson_obj['lesson_time_days'] = lesson_time

        # Get cohort stats
        cohort = cohort_stats.get(lesson_num, {})
        cohort_avg_practice = cohort.get('cohort_avg_practice', 0)
        cohort_avg_time = cohort.get('cohort_avg_time_days', 0)
        cohort_size = cohort.get('cohort_size', 0)

        lesson_obj['cohort_avg_practice'] = cohort_avg_practice
        lesson_obj['cohort_avg_time_days'] = cohort_avg_time
        lesson_obj['cohort_size'] = cohort_size

        # Classify student
        student_practice = lesson_obj.get('practice_count', 0)
        classification = classify_student_for_lesson(
            student_practice,
            lesson_time,
            cohort_avg_practice,
            cohort_avg_time,
            cohort_size
        )

        lesson_obj['classification'] = classification

        # Update counts
        if classification != "insufficient_data":
            all_classifications.append(classification)
            total_lessons_classified += 1

            if classification == "star":
                stars_count += 1
            elif classification == "high_runner":
                high_runners_count += 1
            elif classification == "normal":
                normal_count += 1

    # Calculate overall classification
    overall_classification = calculate_overall_classification(all_classifications)

    # Calculate chapter summaries
    chapter_a_summary = calculate_chapter_summary(lessons, CHAPTER_A_LESSONS, current_lesson)
    chapter_b_summary = calculate_chapter_summary(lessons, CHAPTER_B_LESSONS, current_lesson)
    lesson_12_summary = calculate_chapter_summary(lessons, LESSON_12, current_lesson)
    chapter_c_summary = calculate_chapter_summary(lessons, CHAPTER_C_LESSONS, current_lesson)

    # Handle lesson 12 specially (standalone)
    if current_lesson > 12:
        lesson_12_obj = next((l for l in lessons if str(l.get('lesson')) == '12'), None)
        lesson_12_completed = lesson_12_obj is not None and lesson_12_obj.get('classification') is not None
        lesson_12_summary['completed'] = lesson_12_completed
    else:
        lesson_12_summary['completed'] = False

    # Create performance summary
    performance_summary = {
        'overall_classification': overall_classification,
        'total_lessons_classified': total_lessons_classified,
        'stars_count': stars_count,
        'high_runners_count': high_runners_count,
        'normal_count': normal_count,
        'chapter_a': chapter_a_summary,
        'chapter_b': chapter_b_summary,
        'lesson_12': lesson_12_summary,
        'chapter_c': chapter_c_summary
    }

    student['performance_summary'] = performance_summary

    return student


def calculate_all_student_performance() -> Dict[str, Any]:
    """
    Calculate performance classifications for all students.

    This function:
    1. Fetches all students from MongoDB
    2. Calculates cohort statistics for each lesson (1-18)
    3. Classifies each student for each completed lesson
    4. Calculates overall and chapter-level summaries
    5. Updates MongoDB with performance data

    Returns:
        Dictionary with statistics about the calculation
    """
    print(f"{'='*60}")
    print(f"Calculating Student Performance Classifications")
    print(f"{'='*60}")

    # Connect to MongoDB
    try:
        mongo_conn = get_mongo_connection()
        stats_collection = mongo_conn.get_students_stats_collection()
    except Exception as e:
        print(f"✗ Failed to connect to MongoDB: {e}")
        return {
            'students_processed': 0,
            'lessons_classified': 0,
            'errors': 1
        }

    # Fetch all students
    try:
        all_students = list(stats_collection.find({}))
        print(f"✓ Fetched {len(all_students)} students from MongoDB")
    except Exception as e:
        print(f"✗ Failed to fetch students: {e}")
        return {
            'students_processed': 0,
            'lessons_classified': 0,
            'errors': 1
        }

    if not all_students:
        print("No students found in database")
        return {
            'students_processed': 0,
            'lessons_classified': 0,
            'errors': 0
        }

    # Calculate cohort statistics for all lessons
    print(f"\nCalculating cohort statistics for {TOTAL_LESSONS} lessons...")
    cohort_stats = {}

    for lesson_num in range(1, TOTAL_LESSONS + 1):
        cohort = get_cohort_stats_for_lesson(lesson_num, all_students)
        cohort_stats[lesson_num] = cohort

        if cohort['cohort_size'] > 0:
            print(f"  Lesson {lesson_num}: {cohort['cohort_size']} students, "
                  f"avg practice={cohort['cohort_avg_practice']}, "
                  f"avg time={cohort['cohort_avg_time_days']} days")

    print(f"✓ Cohort statistics calculated for all lessons")

    # Process each student
    print(f"\nClassifying students...")
    students_processed = 0
    total_lessons_classified = 0
    errors = 0

    for student in all_students:
        try:
            # Calculate performance
            updated_student = calculate_performance_for_student(student, cohort_stats)

            # Update MongoDB
            uniq_id = student.get('uniq_id')
            if not uniq_id:
                print(f"⚠ Student missing uniq_id: {student.get('name')}")
                errors += 1
                continue

            # Update document
            stats_collection.update_one(
                {'uniq_id': uniq_id},
                {
                    '$set': {
                        'lessons': updated_student['lessons'],
                        'performance_summary': updated_student['performance_summary'],
                        'updated_at': mongo_conn.get_current_timestamp()
                    }
                }
            )

            students_processed += 1
            classified = updated_student['performance_summary']['total_lessons_classified']
            total_lessons_classified += classified

            # Print student summary
            summary = updated_student['performance_summary']
            print(f"  ✓ {student.get('name')} ({student.get('current_lesson')}): "
                  f"{summary['overall_classification']} "
                  f"(S:{summary['stars_count']}, HR:{summary['high_runners_count']}, N:{summary['normal_count']})")

        except Exception as e:
            print(f"✗ Error processing student {student.get('name')}: {e}")
            import traceback
            traceback.print_exc()
            errors += 1

    print(f"\n{'='*60}")
    print(f"Performance Calculation Complete:")
    print(f"  Students processed: {students_processed}")
    print(f"  Total lessons classified: {total_lessons_classified}")
    print(f"  Errors: {errors}")
    print(f"{'='*60}")

    return {
        'students_processed': students_processed,
        'lessons_classified': total_lessons_classified,
        'errors': errors
    }


if __name__ == '__main__':
    """
    Run performance calculation as a standalone script.
    """
    calculate_all_student_performance()
