import os
import sys
import pandas as pd
import requests
from datetime import datetime

mishnahs_per_perek = [('Berachot', [5, 8, 6, 7, 5, 8, 5, 8, 5]), ('Peah', [6, 8, 8, 11, 8, 11, 8, 9]), ('Damai', [4, 5, 6, 7, 11, 12, 8]), ('Kilaim', [9, 11, 7, 9, 8, 9, 8, 6, 10]), ('Sheviit', [8, 10, 10, 10, 9, 6, 7, 11, 9, 9]), ('Terumot', [10, 6, 9, 13, 9, 6, 7, 12, 7, 12, 10]), ('Maserot', [8, 8, 10, 6, 8]), ('Maser Sheni', [7, 10, 13, 12, 15]), ('Chalah', [9, 8, 10, 11]), ('Orlah', [9, 17, 9]), ('Bikurim', [11, 11, 12, 5]), ('Shabbat', [11, 7, 6, 2, 4, 10, 4, 7, 7, 6, 6, 6, 7, 4, 3, 8, 8, 3, 6, 5, 3, 6, 5, 5]), ('Eruvin', [10, 6, 9, 11, 9, 10, 11, 11, 4, 15]), ('Psachim', [7, 8, 8, 9, 10, 6, 13, 8, 11, 9]), ('Shekalim', [7, 5, 4, 9, 6, 6, 7, 8]), ('Yoma', [8, 7, 11, 6, 7, 8, 5, 9]), ('Sukkah', [11, 9, 15, 10, 8]), ('Beitzah', [10, 10, 8, 7, 7]), ('Rosh HaShanah', [9, 8, 9, 9]), ('Taanit', [7, 10, 9, 8]), ('Megilah', [11, 6, 6, 10]), ('Moed Kattan', [10, 5, 9]), ('Chaggigah', [8, 7, 8]), ('Yevamot', [4, 10, 10, 13, 6, 6, 6, 6, 6, 9, 7, 6, 13, 9, 10, 7]), ('Ketubot', [10, 10, 9, 12, 9, 7, 10, 8, 9, 6, 6, 4, 11]), ('Nedarim', [4, 5, 11, 8, 6, 10, 9, 7, 10, 8, 12]), ('Nazir', [7, 10, 7, 7, 7, 11, 4, 2, 5]), ('Sotah', [9, 6, 8, 5, 5, 4, 8, 7, 15]), ('Gittin', [6, 7, 8, 9, 9, 7, 9, 10, 10]), ('Kidushin', [10, 10, 13, 14]), ('Bava Kamma', [4, 6, 11, 9, 7, 6, 7, 7, 12, 10]), ('Bava Metzia', [8, 11, 12, 12, 11, 8, 11, 9, 13, 6]), ('Bava Batra', [6, 14, 8, 9, 11, 8, 4, 8, 10, 8]), ('Sanhedrin', [6, 5, 8, 5, 5, 6, 11, 7, 6, 6, 6]), ('Makot', [10, 8, 16]), ('Shevuot', [7, 5, 11, 13, 5, 7, 8, 6]), ('Eduyot', [14, 10, 12, 12, 7, 3, 9, 7]), ('Avodah Zara', [9, 7, 10, 12, 12]), ('Avot', [18, 16, 18, 22, 23, 11]), ('Horyot', [5, 7, 8]), ('Zevachim', [4, 5, 6, 6, 8, 7, 6, 12, 7, 8, 8, 6, 8, 10]), ('Menachot', [4, 5, 7, 5, 9, 7, 6, 7, 9, 9, 9, 5, 11]), ('Chullin', [7, 10, 7, 7, 5, 7, 6, 6, 8, 4, 2, 5]), ('Bechorot', [7, 9, 4, 10, 6, 12, 7, 10, 8]), ('Erchin', [4, 6, 5, 4, 6, 5, 5, 7, 8]), ('Temurah', [6, 3, 5, 4, 6, 5, 6]), ('Keritot', [7, 6, 10, 3, 8, 9]), ('Meilah', [4, 9, 8, 6, 5, 6]), ('Tamid', [4, 5, 9, 3, 6, 4, 3]), ('Middot', [9, 6, 8, 7, 4]), ('Kinnim', [4, 5, 6]), ('Kelim', [9, 8, 8, 4, 11, 4, 6, 11, 8, 8, 9, 8, 8, 8, 6, 8, 17, 9, 10, 7, 3, 10, 5, 17, 9, 9, 12, 10, 8, 4, 254, 30]), ('Ohalot', [8, 7, 7, 3, 7, 7, 6, 6, 16, 7, 9, 8, 6, 7, 10, 5, 5, 10]), ('Negaim', [6, 5, 8, 11, 5, 8, 5, 10, 3, 10, 12, 7, 12, 13]), ('Parah', [4, 5, 11, 4, 9, 5, 12, 11, 9, 6, 9, 11]), ('Taharot', [9, 8, 8, 13, 9, 10, 9, 9, 9, 8]), ('Mikvaot', [8, 10, 4, 5, 6, 11, 7, 5, 7, 8]), ('Niddah', [7, 7, 7, 7, 9, 14, 5, 4, 11, 8]), ('Machshirin', [6, 11, 8, 10, 11, 8]), ('Zavim', [6, 4, 3, 7, 12]), ('Tevul Yom', [5, 8, 6, 7]), ('Yadayim', [5, 4, 5, 8]), ('Uktzim', [6, 10, 12])]

def setup_configuration():
    """
    Loads configuration from environment variables and validates them.
    Exits the script if any required variables are missing or invalid.
    """
    required_env_vars = [
        'TELEGRAM_BOT_TOKEN',
        'TELEGRAM_CHAT_ID',
        'XLSX_FILE',
        'START_DATE',
        'START_MASECHET',
        'START_PEREK',
        'START_MISHNA'
    ]
    
    config = {}
    missing_vars = [var for var in required_env_vars if os.environ.get(var) is None]

    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    try:
        config['TELEGRAM_BOT_TOKEN'] = os.environ.get('TELEGRAM_BOT_TOKEN')
        config['TELEGRAM_CHAT_ID'] = os.environ.get('TELEGRAM_CHAT_ID')
        config['START_DATE'] = datetime.strptime(os.environ.get('START_DATE'), '%Y-%m-%d').date()
        config['START_MASECHET'] = os.environ.get('START_MASECHET')
        config['START_PEREK'] = int(os.environ.get('START_PEREK'))
        config['START_MISHNA'] = int(os.environ.get('START_MISHNA'))
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

    header = "📜 Daily Mishna Reminder 📜\n\n"

    if masechet1 == masechet2 and perek1 == perek2:
        message = f"Today's learning is:\n<b>{masechet1}, Perek {perek1}, Mishnayot {num1}-{num2}</b>"
    else:
        message = (
            f"Today's learning is:\n"
            f"1. <b>{masechet1}, Perek {perek1}, Mishna {num1}</b>\n"
            f"2. <b>{masechet2}, Perek {perek2}, Mishna {num2}</b>"
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
    print(todays_mishnayot_tuple)

    mishna1, mishna2 = todays_mishnayot_tuple
    print(f"Today's Mishnayot: {mishna1} and {mishna2}")

    message = format_message(mishna1, mishna2, config['START_DATE'])
    send_telegram_message(message, config['TELEGRAM_BOT_TOKEN'], config['TELEGRAM_CHAT_ID'])
    
    print("Script finished.")

if __name__ == "__main__":
    main()
