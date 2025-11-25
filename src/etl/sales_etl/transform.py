import re
from datetime import datetime

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
    
    # Extract each field using regex with more specific patterns
    # The key change: use (?:\s|$) to stop at whitespace OR end of string
    # Also added word boundaries to prevent capturing beyond the field
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
    """
    leads = []
    
    for msg in messages:
        text = msg.get("text", "")
        
        # Try to extract lead info
        lead_info = extract_lead_info(text)
        
        if lead_info:
            # Add metadata from original message
            lead_info["sender"] = msg.get("sender", "Unknown")
            lead_info["timestamp"] = msg.get("timestamp", "")
            
            # Parse timestamp to datetime if possible
            try:
                # Assuming timestamp format like "10:30" or "10:30 AM"
                lead_info["extracted_at"] = datetime.now().isoformat()
            except:
                lead_info["extracted_at"] = datetime.now().isoformat()
            
            leads.append(lead_info)
            print(f"✓ Lead extracted: {lead_info['שם']} from {lead_info['מקור']}")
    
    print(f"\n{'='*60}")
    print(f"SALES LEADS EXTRACTION SUMMARY")
    print(f"{'='*60}")
    print(f"Total messages scanned: {len(messages)}")
    print(f"Leads found: {len(leads)}")
    print(f"{'='*60}\n")
    
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