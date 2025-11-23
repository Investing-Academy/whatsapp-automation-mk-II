from src.etl.extract import open_whatsapp
from src.etl.transform import process_messages


def run_etl():
    messages = open_whatsapp()
    if messages != 0:
        messages = process_messages(messages)
        print(messages)
    else:
        print("failed to read messages")