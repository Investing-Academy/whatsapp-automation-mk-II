import os
from dotenv import load_dotenv
from src.etl.sales_etl.load import upload_leads_to_sheets
from src.etl.sales_etl.transform import process_sales_messages
from src.sheets_connect import init_google_sheets

# Load environment variables once at module level
load_dotenv()

def get_sales_worksheet():
    """
    Get the sales worksheet connection.
    Separated for reusability and testing.
    """
    sheet_id = os.getenv("SALES_SHEET_ID")
    
    if not sheet_id:
        raise ValueError("SALES_SHEET_ID not found in environment variables")
    
    client = init_google_sheets()
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.worksheet("main")
    
    print(f"✓ Connected to sales spreadsheet")
    return worksheet


def run_sales_etl(sales_messages):
    """
    Run the complete ETL pipeline for sales leads.
    """
    if not sales_messages:
        print("⚠ No sales messages to process")
        return {"success": 0, "errors": ["No messages provided"]}
    
    try:
        # Connect to worksheet
        worksheet = get_sales_worksheet()
        
        # Process leads
        leads = process_sales_messages(sales_messages)
        
        if not leads:
            print("⚠ No leads extracted from messages")
            return {"success": 0, "errors": ["No valid leads found"]}
        
        # Upload to sheets
        result = upload_leads_to_sheets(worksheet, leads)
        
        print(f"✓ ETL Complete: {result['success']} leads uploaded")
        return result
        
    except Exception as e:
        print(f"✗ ETL Failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": 0, "errors": [str(e)]}

    