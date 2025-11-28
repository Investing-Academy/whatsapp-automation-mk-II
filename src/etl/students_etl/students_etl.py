from datetime import datetime
from src.etl.students_etl.transform import process_messages
from src.etl.db.mongodb.message_saver import MessageSaver
from src.etl.students_etl.students_load import update_sheets_from_mongo
from src.etl.db.mongodb.mongo_handler import get_mongo_connection


def log_students_run(stats_processed, stats_upserted, sheets_updated, run_timestamp, total_run_time, success=True, error_message=None):
    """
    Log students ETL run statistics to logger_stats collection
    
    Args:
        stats_processed: Number of student stats processed
        stats_upserted: Number of stats actually upserted to DB
        sheets_updated: Number of Google Sheets rows updated
        run_timestamp: Timestamp when the run started
        total_run_time: Total execution time in seconds
        success: Whether the run was successful
        error_message: Error message if run failed
    """
    try:
        mongo = get_mongo_connection()
        logger_collection = mongo.get_logger_stats_collection()
        
        log_entry = {
            "source": "students_etl",
            "log_level": "info" if success else "error",
            "timestamp": run_timestamp,
            "stats_processed": stats_processed,
            "stats_upserted": stats_upserted,
            "sheets_updated": sheets_updated,
            "total_run_time": round(total_run_time, 2),
            "success": success,
            "error_message": error_message,
            "metadata": {
                "process": "student_stats_update",
                "run_date": run_timestamp.strftime("%Y-%m-%d"),
                "run_time": run_timestamp.strftime("%H:%M:%S")
            }
        }
        
        logger_collection.insert_one(log_entry)
        print(f"✓ Logged students run: {stats_upserted} stats, {sheets_updated} sheets in {total_run_time:.2f}s")
        
    except Exception as e:
        print(f"⚠ Could not log to logger_stats: {e}")


def run_students_etl(students_messages):
    """
    Run the students ETL process with logging to logger_stats.
    
    Args:
        students_messages: List of student messages to process
    """
    run_timestamp = datetime.now()
    start_time = datetime.now()
    
    stats_processed = 0
    stats_upserted = 0
    sheets_updated = 0
    
    try:
        # Transform
        processed = process_messages(students_messages)
        stats_updates = processed['stats_updates']
        stats_processed = len(stats_updates)
        print(f"Processed student stats for {stats_processed} students")

        # Load (stats upsert only)
        saver = MessageSaver()
        stats_result = saver.upsert_student_stats(stats_updates)
        stats_upserted = stats_result['upserts']
        
        # Print results
        print("=" * 60 + "\n" + "STUDENT STATS SAVE RESULTS" + "\n" + "=" * 60 )
        print(f"Total stats processed: {stats_result['processed']}")
        print(f"✓ Upserts applied:     {stats_result['upserts']}")
        print(f"✗ Errors:             {stats_result['errors']}")
        print("=" * 60)
        
        # Update Google Sheets with latest practice dates
        if stats_result['upserts'] > 0:
            print("Updating Google Sheets with latest practice dates...")
            sheets_results = update_sheets_from_mongo()
            
            if sheets_results:
                sheets_updated = sheets_results.get('updates_needed', 0)
                print(f"{len(stats_updates)} students scanned")
                print(f"✓ Sheets updated: {sheets_updated} students")
        else:
            print("⊘ No stats changes - skipping Sheets update")
        
        # Calculate total run time
        total_run_time = (datetime.now() - start_time).total_seconds()
        
        # Log successful run
        log_students_run(
            stats_processed=stats_processed,
            stats_upserted=stats_upserted,
            sheets_updated=sheets_updated,
            run_timestamp=run_timestamp,
            total_run_time=total_run_time,
            success=True
        )
        
        print(f"✓ ETL Complete: {stats_upserted} stats updated in {total_run_time:.2f}s")
        
        return stats_result
        
    except Exception as e:
        # Calculate run time even on error
        total_run_time = (datetime.now() - start_time).total_seconds()
        
        # Log failed run
        log_students_run(
            stats_processed=stats_processed,
            stats_upserted=stats_upserted,
            sheets_updated=sheets_updated,
            run_timestamp=run_timestamp,
            total_run_time=total_run_time,
            success=False,
            error_message=str(e)
        )
        
        print("=" * 60)
        print("STUDENT STATS SAVE FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        raise