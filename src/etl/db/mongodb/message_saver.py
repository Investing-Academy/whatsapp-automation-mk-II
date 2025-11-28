import hashlib
from datetime import datetime
from typing import List, Dict, Optional, Any
from src.etl.db.mongodb.mongo_handler import get_mongo_connection


class MessageSaver:
    """
    Student stats saver - saves only student statistics documents to MongoDB.
    Raw messages are not stored, only aggregated student statistics.
    
    NOTE: This class does NOT use or create a 'messages' collection.
    Only 'student_stats' collection is used.
    """

    def __init__(self):
        """Initialize connection and setup indexes for student_stats collection only"""
        self.mongo = get_mongo_connection()
        
        # Only get student stats collection - NO messages collection
        students_db = self.mongo.get_students_database()
        self.stats_collection = students_db['student_stats']
        self.collection = self.stats_collection  # backwards compatibility for legacy code
        
        # Explicitly ensure we're NOT using a messages collection
        # If messages collection exists, it should be dropped manually
        
        self._setup_indexes()
    
    def _setup_indexes(self):
        """
        Create indexes for student stats collection only
        """
        try:
            self.stats_collection.create_index("phone_number", unique=False, name="phone_unique_idx")
            self.stats_collection.create_index("current_lesson", name="lesson_idx")
            self.stats_collection.create_index("uniq_id", unique=True, name="uniq_id_idx")
            print("✓ All indexes created/verified for student_stats collection")
        except Exception as e:
            print(f"Could not create indexes: {e}")
    
    @staticmethod
    def generate_uniq_id(phone_number: str, name: str) -> str:
        """
        Generate a uniq_id for a student (sha256 hash of lowercased phone+name, first 32 chars)
        """
        base = f"{str(phone_number).lower()}|{str(name).lower()}"
        return hashlib.sha256(base.encode()).hexdigest()[:32]
    
    def save_message(self, message_data: Dict, update_stats: bool = True) -> Dict:
        """
        Update student stats only (messages are NOT saved to MongoDB)
        
        This method only updates the student_stats collection.
        No messages collection is used or created.
        """
        try:
            if update_stats:
                self._update_student_stats(message_data)
                return {
                    'success': True,
                    'action': 'stats_updated',
                    'message': 'Student stats updated'
                }
            return {
                'success': True,
                'action': 'skipped',
                'message': 'Stats update disabled'
            }
        except Exception as e:
            return {
                'success': False,
                'action': 'error',
                'message': f"Error updating stats: {str(e)}"
            }
    
    def _update_student_stats(self, message_data: Dict):
        """
        Update student statistics based on message (uses uniq_id and new schema)
        Only updates student_stats collection - no messages are stored
        """
        try:
            phone = message_data['phone_number']
            name = message_data.get('name', 'Unknown')
            uniq_id = self.generate_uniq_id(phone, name)
            message_category = message_data.get('message_category')
            lesson = message_data.get('lesson', 'Unknown')
            teacher = message_data.get('teacher', 'Unknown')
            timestamp = message_data['timestamp']
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp)
                except Exception:
                    pass
            
            # Migration: update old docs missing uniq_id, rename legacy keys
            legacy_found = False
            existing = self.stats_collection.find_one({'uniq_id': uniq_id})
            if not existing:
                # Fallback: try phone_number for legacy docs
                existing = self.stats_collection.find_one({'phone_number': phone})
                if existing and 'uniq_id' not in existing:
                    print(f"Migrating document for phone {phone}: adding uniq_id...")
                    self.stats_collection.update_one({"_id": existing["_id"]}, {"$set": {"uniq_id": uniq_id}})
                    legacy_found = True
            # Also migrate legacy practices array to lessons array with practice_count key
            if existing and 'practices' in existing:
                lessons = []
                for p in existing.get('practices', []):
                    lessons.append({
                        'lesson': p.get('lesson'),
                        'teacher': p.get('teacher', 'Unknown'),
                        'practice_count': p.get('practice_count') or p.get('count', 0),
                        'first_practice': p.get('first_practice'),
                        'last_practice': p.get('last_practice')
                    })
                self.stats_collection.update_one({'_id': existing['_id']}, {"$set": {"lessons": lessons}, "$unset": {"practices": ""}})
                existing['lessons'] = lessons
            # Now proceed with upsert/update logic
            filter_query = {"uniq_id": uniq_id}
            if existing:
                update_doc = {
                    '$set': {
                        'name': name,
                        'current_lesson': lesson,
                        'updated_at': timestamp,
                        'uniq_id': uniq_id  # ensure always set
                    }
                }
                lesson_entries = existing.get('lessons', [])
                lesson_names = [p.get('lesson') for p in lesson_entries]

                def add_lesson_entry(practice_count, first_ts, last_ts):
                    update_doc.setdefault('$push', {})['lessons'] = {
                        'lesson': lesson,
                        'teacher': teacher,
                        'practice_count': practice_count,
                        'first_practice': first_ts,
                        'last_practice': last_ts
                    }

                if message_category == 'practice':
                    if lesson in lesson_names:
                        idx = lesson_names.index(lesson)
                        update_doc.setdefault('$inc', {})[f'lessons.{idx}.practice_count'] = 1
                        update_doc['$set'][f'lessons.{idx}.last_practice'] = timestamp
                        # set first practice if missing
                        if not lesson_entries[idx].get('first_practice'):
                            update_doc['$set'][f'lessons.{idx}.first_practice'] = timestamp
                    else:
                        add_lesson_entry(1, timestamp, timestamp)
                elif message_category == 'message':
                    update_doc.setdefault('$inc', {})['total_messages'] = 1
                    update_doc['$set']['last_message_date'] = timestamp

                    # ensure lesson entry exists even for general messages (new lesson detected from sheets)
                    if lesson and lesson not in lesson_names:
                        add_lesson_entry(0, None, None)

                self.stats_collection.update_one(filter_query, update_doc)
            else:
                # Create new student stats with requested structure (exactly as per README)
                stat_doc = {
                    'uniq_id': uniq_id,
                    'phone_number': phone,
                    'name': name,
                    'current_lesson': lesson,
                    'total_messages': 1 if message_category == 'message' else 0,
                    'last_message_date': timestamp if message_category == 'message' else None,
                    'lessons': [],
                    'created_at': timestamp,
                    'updated_at': timestamp
                }
                if message_category == 'practice':
                    stat_doc['lessons'] = [{
                        'lesson': lesson,
                        'teacher': teacher,
                        'practice_count': 1,
                        'first_practice': timestamp,
                        'last_practice': timestamp
                    }]
                elif lesson and lesson != 'Unknown':
                    stat_doc['lessons'] = [{
                        'lesson': lesson,
                        'teacher': teacher,
                        'practice_count': 0,
                        'first_practice': None,
                        'last_practice': None
                    }]
                self.stats_collection.insert_one(stat_doc)
        except Exception as e:
            print(f"Warning: Could not update student stats: {e}")
            # Don't fail the message save if stats update fails
    
    def save_messages_batch(self, messages: List[Dict], update_stats: bool = True) -> Dict:
        """
        Update student stats for multiple messages (messages are NOT saved to MongoDB)
        Only student_stats collection is updated - no messages collection is used.
        
        Args:
            messages: List of message dictionaries
            update_stats: Whether to update student stats (default: True)
            
        Returns:
            Dict with summary:
                - total: int (total messages processed)
                - updated: int (stats updated)
                - skipped: int (skipped)
                - errors: int (failed)
                - details: List[Dict] (individual results)
        """
        results = {
            'total': len(messages),
            'updated': 0,
            'skipped': 0,
            'errors': 0,
            'details': []
        }
        
        for message in messages:
            result = self.save_message(message, update_stats=update_stats)
            results['details'].append(result)
            
            if result['action'] == 'stats_updated':
                results['updated'] += 1
            elif result['action'] == 'skipped':
                results['skipped'] += 1
            elif result['action'] == 'error':
                results['errors'] += 1
        
        return results

    def upsert_student_stats(self, stats_updates: List[Dict]) -> Dict:
        """
        Upsert student statistics documents based on aggregated data from transform layer.
        Only updates student_stats collection.
        """
        summary = {'processed': len(stats_updates), 'upserts': 0, 'errors': 0}

        for stats in stats_updates:
            try:
                phone = stats['phone_number']
                name = stats.get('name', 'Unknown')
                uniq_id = self.generate_uniq_id(phone, name)
                teacher = stats.get('teacher', 'Unknown')
                current_lesson = stats.get('current_lesson', 'Unknown')
                updated_at = stats.get('updated_at')
                last_message_date = stats.get('last_message_date')
                lessons_delta = stats.get('practices', [])
                total_messages_delta = stats.get('total_messages', 0)

                existing = self.stats_collection.find_one({'uniq_id': uniq_id})

                # Build lesson map from existing data
                lessons_map = {}
                if existing:
                    for lesson_doc in existing.get('lessons', []):
                        lesson_name = lesson_doc.get('lesson')
                        if lesson_name:
                            lessons_map[lesson_name] = lesson_doc

                # Merge delta lessons
                for entry in lessons_delta:
                    lesson_name = entry.get('lesson')
                    if not lesson_name:
                        continue
                    practice_delta = entry.get('practice_count', 0)
                    entry_teacher = entry.get('teacher', teacher)
                    first_practice = entry.get('first_practice')
                    last_practice = entry.get('last_practice')

                    if lesson_name in lessons_map:
                        lesson_doc = lessons_map[lesson_name]
                        lesson_doc['practice_count'] = lesson_doc.get('practice_count', 0) + practice_delta
                        if first_practice and (not lesson_doc.get('first_practice') or first_practice < lesson_doc.get('first_practice')):
                            lesson_doc['first_practice'] = first_practice
                        if last_practice and (not lesson_doc.get('last_practice') or last_practice > lesson_doc.get('last_practice')):
                            lesson_doc['last_practice'] = last_practice
                        if entry_teacher and entry_teacher != 'Unknown':
                            lesson_doc['teacher'] = entry_teacher
                    else:
                        lessons_map[lesson_name] = {
                            'lesson': lesson_name,
                            'teacher': entry_teacher,
                            'practice_count': practice_delta,
                            'first_practice': first_practice,
                            'last_practice': last_practice
                        }

                # Ensure the current lesson exists (even if no practice yet)
                if current_lesson and current_lesson not in lessons_map:
                    lessons_map[current_lesson] = {
                        'lesson': current_lesson,
                        'teacher': teacher,
                        'practice_count': 0,
                        'first_practice': None,
                        'last_practice': None
                    }

                lessons_list = list(lessons_map.values())

                if existing:
                    update_payload: Dict[str, Any] = {
                        'name': name,
                        'current_lesson': current_lesson or existing.get('current_lesson', 'Unknown'),
                        'lessons': lessons_list,
                        'updated_at': updated_at
                    }

                    if total_messages_delta:
                        update_payload['total_messages'] = existing.get('total_messages', 0) + total_messages_delta
                        if last_message_date:
                            update_payload['last_message_date'] = last_message_date
                    elif last_message_date:
                        update_payload['last_message_date'] = last_message_date

                    self.stats_collection.update_one({'_id': existing['_id']}, {'$set': update_payload})
                else:
                    new_doc = {
                        'uniq_id': uniq_id,
                        'phone_number': phone,
                        'name': name,
                        'current_lesson': current_lesson,
                        'total_messages': total_messages_delta,
                        'last_message_date': last_message_date,
                        'lessons': lessons_list,
                        'created_at': updated_at,
                        'updated_at': updated_at
                    }
                    self.stats_collection.insert_one(new_doc)

                summary['upserts'] += 1
            except Exception as exc:
                summary['errors'] += 1
                print(f"Warning: Could not upsert stats for {stats.get('phone_number')}: {exc}")

        return summary
    
    # ========== QUERY METHODS - STUDENT STATS ONLY ==========
    
    def get_student_stats(self, phone_number: str) -> Optional[Dict]:
        """Get statistics for a specific student from student_stats collection"""
        return self.stats_collection.find_one({'phone_number': phone_number})
    
    def get_all_student_stats(self, limit: Optional[int] = None) -> List[Dict]:
        """Get stats for all students from student_stats collection"""
        cursor = self.stats_collection.find().sort('updated_at', -1)
        
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)
    
    def get_students_by_teacher(self, teacher: str) -> List[Dict]:
        """Get all students for a specific teacher (searches in lessons array)"""
        return list(self.stats_collection.find({'lessons.teacher': teacher}))
    
    def get_students_by_lesson(self, lesson: str) -> List[Dict]:
        """Get all students currently on a specific lesson"""
        return list(self.stats_collection.find({'current_lesson': lesson}))
    
    def get_students_with_low_practice(self, lesson: str, max_count: int = 3) -> List[Dict]:
        """
        Get students who have practiced less than max_count times in a specific lesson
        
        Args:
            lesson: The lesson to check
            max_count: Maximum practice count threshold (default: 3)
        """
        all_students = list(self.stats_collection.find({'current_lesson': lesson}))
        
        low_practice_students = []
        for student in all_students:
            # Find practice count for this lesson
            practice_count = 0
            for lesson_entry in student.get('lessons', []):
                if lesson_entry['lesson'] == lesson:
                    practice_count = lesson_entry['practice_count']
                    break
            
            if practice_count < max_count:
                student['practice_count'] = practice_count
                low_practice_students.append(student)
        
        return low_practice_students
    
    def get_stats_summary(self) -> Dict:
        """Get overall statistics from student_stats collection"""
        total_students = self.stats_collection.count_documents({})
        
        # Aggregate total practices and messages across all students
        pipeline = [
            {'$unwind': '$lessons'},
            {'$group': {
                '_id': None,
                'total_practices': {'$sum': '$lessons.practice_count'}
            }}
        ]
        practice_result = list(self.stats_collection.aggregate(pipeline))
        total_practices = practice_result[0]['total_practices'] if practice_result else 0
        
        # Get total messages from student stats
        total_messages = sum(
            doc.get('total_messages', 0) 
            for doc in self.stats_collection.find({}, {'total_messages': 1})
        )
        
        return {
            'total_students': total_students,
            'total_messages': total_messages,
            'total_practice_count': total_practices
        }
    
    # ========== UTILITY METHODS ==========
    
    def recalculate_all_stats(self) -> Dict:
        """
        Recalculate all student stats (not applicable since messages are not stored)
        This method is kept for backwards compatibility but does nothing
        
        NOTE: Messages are NOT stored in MongoDB, only student_stats exist.
        """
        print("Note: Messages are not stored in MongoDB, so stats recalculation is not applicable.")
        return {
            'students_recalculated': 0,
            'total_messages_processed': 0,
            'message': 'Messages are not stored in MongoDB'
        }
    
    def close(self):
        """Close MongoDB connection"""
        self.mongo.close()