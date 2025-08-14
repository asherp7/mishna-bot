import os
import sys
import pandas as pd
import requests
from datetime import datetime

mishnahs_per_perek = [('ברכות', [5, 8, 6, 7, 5, 8, 5, 8, 5]), ('פאה', [6, 8, 8, 11, 8, 11, 8, 9]), ('דמאי', [4, 5, 6, 7, 11, 12, 8]), ('כלאים', [9, 11, 7, 9, 8, 9, 8, 6, 10]), ('שביעית', [8, 10, 10, 10, 9, 6, 7, 11, 9, 9]), ('תרומות', [10, 6, 9, 13, 9, 6, 7, 12, 7, 12, 10]), ('מעשרות', [8, 8, 10, 6, 8]), ('מעשר שני', [7, 10, 13, 12, 15]), ('חלה', [9, 8, 10, 11]), ('ערלה', [9, 17, 9]), ('ביכורים', [11, 11, 12, 5]), ('שבת', [11, 7, 6, 2, 4, 10, 4, 7, 7, 6, 6, 6, 7, 4, 3, 8, 8, 3, 6, 5, 3, 6, 5, 5]), ('ערובין', [10, 6, 9, 11, 9, 10, 11, 11, 4, 15]), ('פסחים', [7, 8, 8, 9, 10, 6, 13, 8, 11, 9]), ('שקלים', [7, 5, 4, 9, 6, 6, 7, 8]), ('יומא', [8, 7, 11, 6, 7, 8, 5, 9]), ('סוכה', [11, 9, 15, 10, 8]), ('ביצה', [10, 10, 8, 7, 7]), ('ראש השנה', [9, 8, 9, 9]), ('תענית', [7, 10, 9, 8]), ('מגילה', [11, 6, 6, 10]), ('מועד קטן', [10, 5, 9]), ('חגיגה', [8, 7, 8]), ('יבמות', [4, 10, 10, 13, 6, 6, 6, 6, 6, 9, 7, 6, 13, 9, 10, 7]), ('כתובות', [10, 10, 9, 12, 9, 7, 10, 8, 9, 6, 6, 4, 11]), ('נדרים', [4, 5, 11, 8, 6, 10, 9, 7, 10, 8, 12]), ('נזיר', [7, 10, 7, 7, 7, 11, 4, 2, 5]), ('סוטה', [9, 6, 8, 5, 5, 4, 8, 7, 15]), ('גיטין', [6, 7, 8, 9, 9, 7, 9, 10, 10]), ('קידושין', [10, 10, 13, 14]), ('בבא קמא', [4, 6, 11, 9, 7, 6, 7, 7, 12, 10]), ('בבא מציעא', [8, 11, 12, 12, 11, 8, 11, 9, 13, 6]), ('בבא בתרא', [6, 14, 8, 9, 11, 8, 4, 8, 10, 8]), ('סנהדרין', [6, 5, 8, 5, 5, 6, 11, 7, 6, 6, 6]), ('מכות', [10, 8, 16]), ('שבועות', [7, 5, 11, 13, 5, 7, 8, 6]), ('עדויות', [14, 10, 12, 12, 7, 3, 9, 7]), ('עבודה זרה', [9, 7, 10, 12, 12]), ('אבות', [18, 16, 18, 22, 23, 11]), ('הוריות', [5, 7, 8]), ('זבחים', [4, 5, 6, 6, 8, 7, 6, 12, 7, 8, 8, 6, 8, 10]), ('מנחות', [4, 5, 7, 5, 9, 7, 6, 7, 9, 9, 9, 5, 11]), ('חולין', [7, 10, 7, 7, 5, 7, 6, 6, 8, 4, 2, 5]), ('בכורות', [7, 9, 4, 10, 6, 12, 7, 10, 8]), ('ערכין', [4, 6, 5, 4, 6, 5, 5, 7, 8]), ('תמורה', [6, 3, 5, 4, 6, 5, 6]), ('כריתות', [7, 6, 10, 3, 8, 9]), ('מעילה', [4, 9, 8, 6, 5, 6]), ('תמיד', [4, 5, 9, 3, 6, 4, 3]), ('מדות', [9, 6, 8, 7, 4]), ('קינים', [4, 5, 6]), ('כלים', [9, 8, 8, 4, 11, 4, 6, 11, 8, 8, 9, 8, 8, 8, 6, 8, 17, 9, 10, 7, 3, 10, 5, 17, 9, 9, 12, 10, 8, 4, 254, 30]), ('אוהלות', [8, 7, 7, 3, 7, 7, 6, 6, 16, 7, 9, 8, 6, 7, 10, 5, 5, 10]), ('נגעים', [6, 5, 8, 11, 5, 8, 5, 10, 3, 10, 12, 7, 12, 13]), ('פרה', [4, 5, 11, 4, 9, 5, 12, 11, 9, 6, 9, 11]), ('טהרות', [9, 8, 8, 13, 9, 10, 9, 9, 9, 8]), ('מקואות', [8, 10, 4, 5, 6, 11, 7, 5, 7, 8]), ('נידה', [7, 7, 7, 7, 9, 14, 5, 4, 11, 8]), ('מכשירין', [6, 11, 8, 10, 11, 8]), ('זבים', [6, 4, 3, 7, 12]), ('טבול יום', [5, 8, 6, 7]), ('ידים', [5, 4, 5, 8]), ('עוקצים', [6, 10, 12])]

hebrew_numbers = [
    "א",
    "ב",
    "ג",
    "ד",
    "ה",
    "ו",
    "ז",
    "ח",
    "ט",
    "י",
    "יא",
    "יב",
    "יג",
    "יד",
    "טו",
    "טז",
    "יז",
    "יח",
    "יט",
    "כ",
    "כא",
    "כב",
    "כג",
    "כד",
    "כה",
    "כו",
    "כז",
    "כח",
    "כט",
    "ל"
]

def setup_configuration():
    """
    Loads configuration from environment variables and validates them.
    Exits the script if any required variables are missing or invalid.
    """
    required_env_vars = [
        'TELEGRAM_BOT_TOKEN',
        'TELEGRAM_CHAT_ID',
    ]
    
    config = {}
    config['START_DATE'] = datetime.strptime("2025-08-10", '%Y-%m-%d').date()
    config['START_MASECHET'] = "מעילה"
    config['START_PEREK'] = 1
    config['START_MISHNA'] = 2
    missing_vars = [var for var in required_env_vars if os.environ.get(var) is None]

    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    try:
        config['TELEGRAM_BOT_TOKEN'] = os.environ.get('TELEGRAM_BOT_TOKEN')
        config['TELEGRAM_CHAT_ID'] = os.environ.get('TELEGRAM_CHAT_ID')
        return config
    except (ValueError, TypeError) as e:
        print(f"Error: Invalid format in environment variables. Please check dates and numbers. Details: {e}")
        sys.exit(1)

def load_mishna_data(data):
    """
    Loads Mishna data from a list of typles, where each tuple is a pair: (masechet name, list of number of mishnahs in each perek of the masechet), 
    and creates a flat, ordered list
    of all Mishnayot in Shas.
    """
    all_mishnayot = []
    for masechet, perakim in data:
        for perek, num_mishnayot in enumerate(perakim, 1):
            for mishna in range(1, int(num_mishnayot) + 1):
                all_mishnayot.append((masechet.strip(), int(perek), mishna))
    return all_mishnayot

def get_todays_mishnayot(all_mishnayot, schedule_config):
    """
    Calculates the two Mishnayot to be learned today based on the start date.
    """
    start_mishna_tuple = (
        schedule_config['START_MASECHET'],
        schedule_config['START_PEREK'],
        schedule_config['START_MISHNA']
    )

    try:
        start_index = all_mishnayot.index(start_mishna_tuple)
    except ValueError:
        print(f"Error: Start Mishna not found in the data: {start_mishna_tuple}")
        return None

    days_passed = (datetime.now().date() - schedule_config['START_DATE']).days
    
    if days_passed < 0:
        days_passed = 0

    mishna_offset = days_passed * 2
    total_mishnayot = len(all_mishnayot)
    mishna1_index = (start_index + mishna_offset) % total_mishnayot
    mishna2_index = (start_index + mishna_offset + 1) % total_mishnayot

    return all_mishnayot[mishna1_index], all_mishnayot[mishna2_index]

def format_message(mishna1, mishna2, start_date):
    """
    Formats the message to be sent to Telegram.
    """
    masechet1, perek1, num1 = mishna1
    masechet2, perek2, num2 = mishna2

    header = "משניות לעילוי נשמת אלישע לוי לוינשטרן 🕯️\n\n"

    if masechet1 == masechet2 and perek1 == perek2:
        message = f"           <b>{masechet1}</b> פרק <b>{hebrew_numbers[perek1-1]}</b> משניות <b>{hebrew_numbers[num1-1]}</b> - <b>{hebrew_numbers[num2-1]}</b>"
    else:
        message = (
        f"           <b>{masechet1}</b> פרק <b>{hebrew_numbers[perek1-1]}</b> משנה <b>{hebrew_numbers[num1-1]}</b>"
        f"           <b>{masechet2}</b> פרק <b>{hebrew_numbers[perek2-1]}</b> משנה <b>{hebrew_numbers[num2-1]}</b>"
        )
        
    return header + message

def send_telegram_message(message, token, chat_id):
    """
    Sends a message to the configured Telegram chat using the bot token.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {'chat_id': chat_id, 'text': message, 'parse_mode': 'HTML'}

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print("Message sent successfully to Telegram!")
    except requests.exceptions.RequestException as e:
        print(f"Error sending message to Telegram: {e}")

def main():
    """Main function to run the script."""
    print("Starting the daily Mishna script...")
    
    config = setup_configuration()
    
    all_mishnayot = load_mishna_data(mishnahs_per_perek)
    if not all_mishnayot:
        sys.exit(1)

    todays_mishnayot_tuple = get_todays_mishnayot(all_mishnayot, config)
    if not todays_mishnayot_tuple:
        sys.exit(1)

    mishna1, mishna2 = todays_mishnayot_tuple
    # print(f"Today's Mishnayot: {mishna1} and {mishna2}")

    message = format_message(mishna1, mishna2, config['START_DATE'])
    send_telegram_message(message, config['TELEGRAM_BOT_TOKEN'], config['TELEGRAM_CHAT_ID'])
    
    print("Script finished.")

if __name__ == "__main__":
    main()
