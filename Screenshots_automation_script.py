import pyautogui
import time
from datetime import datetime, timedelta

def get_previous_workday(current_date):
    """Returns the previous date, skipping weekends."""
    date = current_date - timedelta(days=1)
    while date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        date -= timedelta(days=1)
    return date

def automate_flow(start_date_mmdd, iterations=10):
    # Determine the starting year (using current year)
    current_year = datetime.now().year
    
    try:
        # Parse the starting date
        current_date = datetime.strptime(f"{current_year}{start_date_mmdd}", "%Y%m%d")
    except ValueError:
        print("Invalid date format. Please use MMDD.")
        return

    # If the starting date is a weekend, move to the previous workday
    while current_date.weekday() >= 5:
        current_date -= timedelta(days=1)

    print("Starting automation in 3 seconds...")
    time.sleep(3)

    # 1. Alt + Tab (Only once at the start)
    print("Performing Alt+Tab...")
    pyautogui.hotkey('alt', 'tab')
    
    # Wait a bit for the window to become active
    time.sleep(1) 

    for i in range(iterations):
        date_str = current_date.strftime("%m%d")
        print(f"[{i+1}/{iterations}] Processing date: {date_str}")

        # 2. Type Date
        pyautogui.write(date_str, interval=0.05)
        time.sleep(0.5)

        # 3. Tab * 4
        for _ in range(4):
            pyautogui.press('tab')
            time.sleep(0.1)

        # 4. Type same Date
        pyautogui.write(date_str, interval=0.05)
        time.sleep(0.5)

        # 5. Tab * 4
        for _ in range(4):
            pyautogui.press('tab')
            time.sleep(0.1)

        # 6. Press Enter
        pyautogui.press('enter')

        # 7. Wait 3 sec
        print("Waiting 3 seconds...")
        time.sleep(8)

        # 8. Win + Prt Sc (Take screenshot)
        print("Taking screenshot...")
        pyautogui.keyDown('win')
        pyautogui.press('printscreen')
        pyautogui.keyUp('win')
        
        # Short pause after screenshot to let it save
        time.sleep(1) 

        # 9. Shift + Tab * 8
        for _ in range(8):
            pyautogui.hotkey('shift', 'tab')
            time.sleep(0.1)

        # Update date for the next iteration (previous day, skipping weekends)
        current_date = get_previous_workday(current_date)
        
        # Short pause before the next loop
        time.sleep(0.5) 
        
    print("Automation complete!")

if __name__ == "__main__":
    # --- Configuration ---
    # Put your starting date here in MMDD format
    START_DATE = "0504" 
    
    # Set how many days backward you want to process
    ITERATIONS = 50
    
    automate_flow(START_DATE, ITERATIONS)
