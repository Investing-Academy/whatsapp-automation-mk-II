import re
from datetime import datetime
from src.etl.db.mongodb.mongo_handler import get_mongo_connection

def parse_whatsapp_timestamp(timestamp_str):
    """
    Parse WhatsApp timestamp string to datetime object.
    Returns datetime object or None if parsing fails.
    """
    if not timestamp_str:
        return None
    
    try:
        # If it's already a datetime object
        if isinstance(timestamp_str, datetime):
            return timestamp_str
        
        # Try ISO format first
        try:
            return datetime.fromisoformat(timestamp_str)
        except:
            pass
        
        # Try common date-time formats
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except:
                continue
        
        # If just time (like "10:30"), assume today's date
        time_formats = ["%H:%M", "%I:%M %p"]
        for fmt in time_formats:
            try:
                time_obj = datetime.strptime(timestamp_str, fmt)
                today = datetime.now().date()
                return datetime.combine(today, time_obj.time())
            except:
                continue
        
        return None
        
    except Exception as e:
        print(f"Warning: Could not parse timestamp '{timestamp_str}': {e}")
        return None


def get_last_run_timestamp():
    """
    Get the last run timestamp from MongoDB.
    Returns datetime object or None if no previous run.
    """
    try:
        mongo = get_mongo_connection()
        collection = mongo.get_sales_last_run_collection()
        
        # Find the document for sales leads ETL
        doc = collection.find_one({"identifier": "sales_leads_etl"})
        
        if doc and "last_run_timestamp" in doc:
            last_timestamp = doc["last_run_timestamp"]
            
            # Parse if it's a string
            if isinstance(last_timestamp, str):
                last_timestamp = parse_whatsapp_timestamp(last_timestamp)
            
            print(f"Last run timestamp: {last_timestamp}")
            return last_timestamp
        else:
            print(f"No previous run found - processing all messages")
            return None
            
    except Exception as e:
        print(f"Warning: Could not get last run timestamp: {e}")
        return None


def save_last_run_timestamp(timestamp):
    """
    Save the last run timestamp to MongoDB.
    Overwrites the previous timestamp.
    """
    try:
        mongo = get_mongo_connection()
        collection = mongo.get_sales_last_run_collection()
        
        # Convert datetime to ISO string for storage
        if isinstance(timestamp, datetime):
            timestamp_str = timestamp.isoformat()
        else:
            timestamp_str = str(timestamp)
        
        # Update or insert the document
        result = collection.update_one(
            {"identifier": "sales_leads_etl"},
            {
                "$set": {
                    "last_run_timestamp": timestamp_str,
                    "updated_at": datetime.now().isoformat()
                }
            },
            upsert=True
        )
        
        print(f"💾 Saved last run timestamp: {timestamp_str}")
        return True
        
    except Exception as e:
        print(f"Error saving last run timestamp: {e}")
        return False


def filter_new_messages(messages, last_run_timestamp):
    """
    Filter messages to only include those newer than last_run_timestamp.
    """
    if not last_run_timestamp:
        # No previous run, return all messages
        return messages
    
    new_messages = []
    
    for msg in messages:
        msg_timestamp = parse_whatsapp_timestamp(msg.get("timestamp", ""))
        
        if msg_timestamp and msg_timestamp > last_run_timestamp:
            new_messages.append(msg)
    
    print(f"Filtered messages: {len(messages)} total → {len(new_messages)} new")
    
    return new_messages


def extract_lead_info(text):
    """
    Extract lead information from WhatsApp message text.
    Returns dict with lead data or None if format not found.
    """
    # Check if message contains the key identifier
    if "מקור:" not in text:
        return None
    
    lead_data = {
        "מקור": None,
        "שם": None,
        "מייל": None,
        "טלפון": None,
        "raw_text": text
    }
    
    # Extract each field using regex
    patterns = {
        "מקור": r"מקור:\s*([^\n]+?)(?:\s+(?:שם|מייל|טלפון)|$)",
        "שם": r"שם:\s*([^\n]+?)(?:\s+(?:טלפון|מייל|מקור)|$)",
        "מייל": r"מייל:\s*([^\s]+?)(?:\s+(?:מקור|שם|טלפון)|$)",
        "טלפון": r"טלפון:\s*([^\s]+?)(?:\s+(?:מייל|מקור|שם)|$)"
    }
    
    for field, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            lead_data[field] = match.group(1).strip()
    
    # Only return if we found at least the name (שם)
    if lead_data["שם"]:
        return lead_data
    
    return None


def process_sales_messages(messages):
    """
    Process sales messages and extract lead information.
    Filters out messages already processed in previous runs.
    """
    print(f"{'='*60}")
    print(f"SALES LEADS PROCESSING")
    print(f"{'='*60}")
    
    # Get last run timestamp
    last_run_timestamp = get_last_run_timestamp()
    
    # Filter messages to only new ones
    new_messages = filter_new_messages(messages, last_run_timestamp)
    
    if not new_messages:
        print(f"✓ No new messages to process")
        print(f"{'='*60}")
        return []
    
    leads = []
    latest_timestamp = None
    
    for msg in new_messages:
        text = msg.get("text", "")
        
        # Try to extract lead info
        lead_info = extract_lead_info(text)
        
        if lead_info:
            # Add metadata from original message
            lead_info["sender"] = msg.get("sender", "Unknown")
            lead_info["timestamp"] = msg.get("timestamp", "")
            lead_info["extracted_at"] = datetime.now().isoformat()
            
            leads.append(lead_info)
            print(f"✓ Lead extracted: {lead_info['שם']} from {lead_info['מקור']}")
            
            # Track the latest timestamp
            msg_timestamp = parse_whatsapp_timestamp(msg.get("timestamp", ""))
            if msg_timestamp:
                if not latest_timestamp or msg_timestamp > latest_timestamp:
                    latest_timestamp = msg_timestamp
    
    # Save the latest timestamp for next run
    if latest_timestamp:
        save_last_run_timestamp(latest_timestamp)
    
    print(f"{'='*60}")
    print(f"SALES LEADS EXTRACTION SUMMARY")
    print(f"{'='*60}")
    print(f"Total messages scanned: {len(messages)}")
    print(f"New messages processed: {len(new_messages)}")
    print(f"Leads found: {len(leads)}")
    print(f"{'='*60}")
    
    return leads


def format_single_lead_for_sheets(lead):
    """
    Format a single lead according to the column structure:
    B: timestamp
    C: name (שם)
    D: phone (טלפון)
    E: email (מייל)
    F: source (מקור)
    """
    return [
        lead.get("timestamp", ""),
        lead.get("שם", ""),
        lead.get("טלפון", ""),
        lead.get("מייל", ""),
        lead.get("מקור", "")
    ]


def format_leads_for_sheets(leads):
    """
    Format all leads for batch Google Sheets insertion.
    """
    return [format_single_lead_for_sheets(lead) for lead in leads]