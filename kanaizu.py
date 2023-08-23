import os
import re
import json
import pandas as pd
from aiogram import Bot, Dispatcher, types
from concurrent.futures import ProcessPoolExecutor

TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'
PATH = 'PATH_TO_YOUR_FILES'
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
executor = ProcessPoolExecutor(max_workers=4)

stats = {'files_processed': 0, 'errors': 0}

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    keyboard = types.InlineKeyboardMarkup()
    btn1 = types.InlineKeyboardButton('Поиск', callback_data='search') 
    btn2 = types.InlineKeyboardButton('Статистика', callback_data='stats') 
    keyboard.add(btn1, btn2)
    await message.answer('Выберите действие:', reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data == 'search')
async def process_callback_search(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, 'Введите текст для поиска:')

@dp.callback_query_handler(lambda c: c.data == 'stats')
async def process_callback_stats(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, f"Обработано файлов: {stats['files_processed']}, ошибок: {stats['errors']}")

@dp.message_handler()
async def handle_message(message: types.Message):
    text = message.text
    chat_id = message.chat.id
    for filename in os.listdir(PATH):
        try:
            if filename.endswith(('.csv', '.txt', '.json')):
                future = executor.submit(search_in_file, filename, text)
                future.add_done_callback(lambda x: send_results(x.result(), chat_id))
        except Exception as e:
            print(f"Error processing file {filename}: {e}")
            stats['errors'] += 1

async def send_results(results, chat_id):
    for result in results:
        await bot.send_message(chat_id, result)

def search_in_file(filename, text):
    results = []
    try:
        if filename.endswith('.csv'):
            df = pd.read_csv(os.path.join(PATH, filename))
            process_file(df, filename, results, text)
        elif filename.endswith('.txt'):
            with open(os.path.join(PATH, filename), 'r') as f:
                lines = f.readlines()
                process_file(lines, filename, results, text)
        elif filename.endswith('.json'):
            with open(os.path.join(PATH, filename), 'r') as f:
                data = json.load(f)
                process_file(data, filename, results, text)
        stats['files_processed'] += 1
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
        stats['errors'] += 1
    return results

def process_file(data, filename, results, text):
    if isinstance(data, pd.DataFrame):
        for i, row in data.iterrows():
            if re.search(text, str(row.to_string()), re.IGNORECASE):
                result = f"Found in CSV ({filename}), row {i+1}:\n\n"
                for column in data.columns:
                    result += f"{column}: {row[column]}\n"
                results.append(result)
    elif isinstance(data, list):
        for i, line in enumerate(data):
            if re.search(text, line, re.IGNORECASE):
                result = f"Found in TXT ({filename}), line {i+1}:\n{line}"
                results.append(result)
    elif isinstance(data, dict):
        for key, value in data.items():
            if re.search(text, str(value), re.IGNORECASE):
                result = f"Found in JSON ({filename}), key {key}:\n{value}"
                results.append(result)

if __name__ == '__main__':
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)