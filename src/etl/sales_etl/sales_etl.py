from datetime import datetime, timedelta
from src.etl.sales_etl.load import upload_leads_to_sheets
from src.etl.sales_etl.transform import process_sales_messages, format_leads_for_sheets  


def run_sales_etl(sales_messages, use_test_data=False):
    """
    Run the sales ETL process.
    
    Args:
        sales_messages: List of sales messages to process
        use_test_data: If True, uses test messages instead of real data
    """
    try:        
        # Process leads / Transform
        leads = process_sales_messages(sales_messages)
        
        if not leads:
            print("⚠ No leads extracted from messages")
            return {"success": 0, "errors": ["No valid leads found"]}
        
        # Format leads for sheets (do this once here)
        formatted_leads = format_leads_for_sheets(leads)
        
        # Upload to sheets (pass already formatted leads)
        result = upload_leads_to_sheets(formatted_leads)
        
        print(f"✓ ETL Complete: {result['success']} leads uploaded")
        return result
        
    except Exception as e:
        print(f"✗ ETL Failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": 0, "errors": [str(e)]} 