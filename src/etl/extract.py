from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from dotenv import load_dotenv
import time
import os


def open_whatsapp_browser():
    """Open WhatsApp Web one time and return driver + wait"""
    chrome_options = Options()
    chrome_options.add_argument("--disable-notifications")

    # Persistent session
    user_data_dir = os.path.join(os.getcwd(), "whatsapp_session")
    chrome_options.add_argument(f"user-data-dir={user_data_dir}")

    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 30)

    print("Opening WhatsApp Web…")
    driver.get("https://web.whatsapp.com")

    # Check login
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#side [role="textbox"][contenteditable="true"]')
            )
        )
        print("Already logged in! Session restored.")
        time.sleep(2)
    except TimeoutException:
        print("Scan the QR code to login...")
        time.sleep(15)

    print("WhatsApp Web is ready.")
    return driver, wait



def open_group(driver, wait, group_name):
    """Open a WhatsApp group by name"""
    print(f"--- Opening group: {group_name} ---")

    # Focus search box
    search_box = wait.until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '#side [role="textbox"][contenteditable="true"]')
        )
    )
    search_box.click()
    search_box.send_keys(Keys.CONTROL, 'a')
    search_box.send_keys(Keys.BACK_SPACE)
    search_box.send_keys(group_name)

    # Select first result
    first_result = None
    try:
        results = WebDriverWait(driver, 0.5).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, 'div[data-testid="cell-frame-container"]')
            )
        )
        if results:
            first_result = results[0]
    except TimeoutException:
        pass

    if not first_result:
        try:
            results = WebDriverWait(driver, 0.5).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, '//div[@role="listbox"]//div[@role="option"]')
                )
            )
            if results:
                first_result = results[0]
        except TimeoutException:
            pass

    if first_result:
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});", first_result
        )
        driver.execute_script("arguments[0].click();", first_result)
    else:
        ActionChains(driver).send_keys(Keys.ARROW_DOWN).send_keys(Keys.ENTER).perform()

    # Wait for messages to appear after opening group
    try:
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-pre-plain-text]'))
        )
    except TimeoutException:
        print("⚠ No messages found in group")

    print(f"Group opened: {group_name}")



def _extract_message_data(message_elements):
    """Extract data from message elements"""
    data = []
    for msg in message_elements:
        try:
            meta = msg.get_attribute("data-pre-plain-text")
            if meta:
                meta = meta.strip("[]")
                timestamp, sender = meta.split("] ")[0], meta.split("] ")[1].replace(":", "")
            else:
                timestamp, sender = "?", "?"

            # WhatsApp Web 2025+ message text selector
            text_elems = msg.find_elements(
                By.CSS_SELECTOR,
                'span[dir="ltr"], span[dir="rtl"]'
            )

            text = " ".join(t.text for t in text_elems).strip()

            data.append({
                "sender": sender,
                "timestamp": timestamp,
                "text": text
            })
        except Exception as e:
            print(f"⚠ Error extracting message: {e}")

    print(f"{len(data)} messages read.")
    return data


def read_messages(driver, message_count):
    """Read last N WhatsApp messages using scroll-based loading without sleep"""
    print(f"Reading last {message_count} messages...")

    # Try multiple selectors for the conversation panel
    panel = None
    panel_selectors = [
        "div[data-testid='conversation-panel-body']",
        "div[data-testid='conversation-panel-messages']",
        "#main div[role='application']",
        "#main [data-testid*='conversation']",
        "div.copyable-area",
        "#main div[tabindex='-1'][role='application']"
    ]

    for selector in panel_selectors:
        try:
            panel = driver.find_element(By.CSS_SELECTOR, selector)
            print(f"✓ Found conversation panel using: {selector}")
            break
        except:
            continue

    if not panel:
        print("⚠ Could not find conversation panel with any selector")
        print("⚠ Attempting to scroll using alternative method...")
        # Try to find any scrollable element
        try:
            panel = driver.find_element(By.CSS_SELECTOR, "#main")
            print("✓ Using #main element for scrolling")
        except:
            print("⚠ Could not find scrollable element, reading visible messages only")
            messages = driver.find_elements(By.CSS_SELECTOR, '[data-pre-plain-text]')
            last_messages = messages[-message_count:] if len(messages) >= message_count else messages
            return _extract_message_data(last_messages)

    max_scroll_attempts = 15
    previous_count = 0
    stable_count = 0
    target_messages = message_count

    # Get initial count
    messages = driver.find_elements(By.CSS_SELECTOR, '[data-pre-plain-text]')
    print(f"Initial message count: {len(messages)}")

    for attempt in range(max_scroll_attempts):
        # Scroll to top to trigger loading older messages
        try:
            driver.execute_script("arguments[0].scrollTop = 0", panel)
        except Exception as e:
            print(f"⚠ Scroll error: {e}")

        # Count current messages
        messages = driver.find_elements(By.CSS_SELECTOR, '[data-pre-plain-text]')
        current_count = len(messages)

        print(f"  Scroll attempt {attempt + 1}: Found {current_count} messages (target: {target_messages})")

        # Check if message count has stabilized
        if current_count == previous_count:
            stable_count += 1
            if stable_count >= 3:  # Count stable for 3 consecutive checks
                print(f"✓ Message count stabilized at {current_count}")
                break
        else:
            stable_count = 0  # Reset stability counter

        # Check if we have enough messages
        if current_count >= target_messages:
            print(f"✓ Reached target: {current_count} >= {target_messages}")
            break

        previous_count = current_count

        # Scroll multiple times with different techniques
        try:
            # Method 1: Scroll to top
            driver.execute_script("arguments[0].scrollTop = 0", panel)
            # Method 2: Scroll up by pixels
            driver.execute_script("arguments[0].scrollBy(0, -1000)", panel)
            # Method 3: Scroll to very top
            driver.execute_script("arguments[0].scrollTo(0, 0)", panel)
        except Exception as e:
            print(f"⚠ Error during aggressive scrolling: {e}")

    # Get final message list
    messages = driver.find_elements(By.CSS_SELECTOR, '[data-pre-plain-text]')
    last_messages = messages[-message_count:] if len(messages) >= message_count else messages

    return _extract_message_data(last_messages)


def run_multi_group_reader():
    load_dotenv()

    STUDENTS = os.getenv("STUDENTS_GROUP")
    #SALES = os.getenv("SALES_TEAM_GROUP")  # Disabled - sales ETL not running
    MESSAGE_COUNT = int(os.getenv("MESSAGE_COUNT"))

    driver, wait = open_whatsapp_browser()

    try:
        # --- Group 1: Students ---
        open_group(driver, wait, STUDENTS)
        students_messages = read_messages(driver, MESSAGE_COUNT)
        print(students_messages)

        # --- Group 2: Sales (DISABLED) ---
        #open_group(driver, wait, SALES)
        #sales_messages = read_messages(driver, MESSAGE_COUNT)

        print("=== Done Extracting Messages ===")
        return {
            "students": students_messages,
            "sales": []  # Empty list since sales ETL is disabled
        }

    finally:
        driver.quit()