from src.etl.extract import open_whatsapp
from src.etl.transform import process_messages
from src.etl.db.mongodb.message_saver import MessageSaver

def run_etl():
    # Extract
    messages = open_whatsapp()
    if messages == 0:
        print("========================")
        print("failed to read messages")
        print("========================")
        return
    else:
        # Transform
        messages = process_messages(messages)
        print(f"Processed {len(messages)} messages")
        
        # Load (with deduplication)
        try:
            saver = MessageSaver()
            results = saver.save_messages_batch(messages)
            
            # Print results
            print("=" * 60)
            print("DATABASE SAVE RESULTS")
            print(f"Total processed:     {results['total']}")
            print(f"✓ New messages:      {results['inserted']}")
            print(f"⊘ Duplicates skipped: {results['skipped']}")
            print(f"✗ Errors:            {results['errors']}")
            print("=" * 60)

            
            return results
            
        except Exception as e:
            print("DATABASE SAVE FAILED")
            print(f"Error: {e}")

            raise
