#!/usr/bin/env python


import logging
import sqlite3
import json

from pathlib import Path
from typing import Optional

from telegram import Update, ForceReply, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, CallbackContext
from telegram.helpers import escape_markdown


# Enable logging
logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] <%(name)s> %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('apscheduler.scheduler').setLevel(logging.WARNING)
logging.getLogger('telegram.ext.Application').setLevel(logging.WARNING)

db = {}



async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user.mention_markdown_v2()

    text = f'Привет, {user}\\!\n'
    text += 'Я кое\\-что знаю про автомобили KIA Sportage первого поколения\\. '
    text += 'Назовите мне идентификационный номер автомобиля \\(VIN\\) и я попробую найти о нём информацию\\.\n'

    await update.message.reply_markdown_v2(text)

def check_ru_vin(vin):
    global db

    cursor = db['VIN_RU']
    result = None
    if cursor:
        cursor.execute('SELECT * FROM VIN_RU WHERE VINRU = ?', (vin, ))
        result = cursor.fetchall()

    if not result or len(result) < 1:
        return 'Очень жаль, но поиск не дал результатов\\.'

    fields = {
        'ENGINE': 'Номер двигателя',
        'PDATE': 'Дата производства',
        'SDATE': 'Дата продажи',
        'MOD': 'Модификация',
        'TRIM': 'Код комплектации',
        'PAINT': 'Цвет кузова'
    }

    text = [f'Результаты поиска VIN *{vin}*:']
    index = 1
    for row in result:
        if index > 1:
            text.append('')

        if len(row['VINKR']) > 0:
            text.append(f'{index}\\. `{row["VINKR"]}`')
        else:
            text.append(f'{index}\\. _НОМЕР НЕ НАЙДЕН_')

        for field, description in fields.items():
            if not row[field]:
                continue
            value = escape_markdown(row[field], 2)
            text.append(f'{description}: *{value}*')

        index += 1

    return '\n'.join(text)

def fetch_cat_record(catalogue, table, key, value):
    global db
    cursor = db[catalogue] if catalogue in db else None
    if cursor:
        cursor.execute(f'SELECT * FROM {table} WHERE {key} = ? LIMIT 1', (value, ))
        result = cursor.fetchall()
    if result and len(result) > 0:
        return result[0]
    return None

def get_media(catalogue: str, group: str):
    media = []
    for i in range(1, 4):
        path = Path(f'GROUP/{catalogue}/{group}{i}1.png')
        if path.is_file():
            try:
                media.append(InputMediaPhoto(open(path, 'rb')))
            except Exception:
                pass
    return media

def check_pno(pno):
    global db

    text = ''
    media = []
    catalogue = 'GENKFM002A'
    cursor = db[catalogue] if catalogue in db else None
    result = None
    if cursor:
        cursor.execute('SELECT * FROM MDBCDMPF WHERE CDPTNO = ? LIMIT 1', (pno, ))
        result = cursor.fetchall()
    if result and len(result) > 0:
        part = result[0]
        group = fetch_cat_record(catalogue, 'MDBGNMPF', 'GNGRNO', part["CDGRNO"])
        partName = fetch_cat_record(catalogue, 'MDBPNCPF', 'PNPNCD', part["CDPNCD"])
        text = f'Информация о запчасти *{part["CDPTNO"]}*:\n'

        media = get_media(catalogue, part["CDGRNO"])

        if partName:
            text += f'Наименование: *{escape_markdown(partName["PNLGEG"], 2)}* \\({escape_markdown(partName["PNLGRU"], 2)}\\)\n'

        text += f'Номер на схеме: *{part["CDKEY1"]}*\n'

        if group:
            text += f'Группа: {group["GNGRNO"]} \\- *{escape_markdown(group["GNLGEG"], 2)}* \\({escape_markdown(group["GNLGRU"], 2)}\\)\n'

        partCount = part["CDCQTY"].lstrip('0')
        if partCount:
            text += f'Количество: *{partCount}*\n'

        if part["CDREMK"]:
            text += f'Заметка: *{escape_markdown(part["CDREMK"], 2)}*\n'


    return {'text' : text, 'media' : media}

def list_group(group: str):
    global db

    text = ''
    media = []
    catalogue = 'GENKFM002A'
    cursor = db[catalogue] if catalogue in db else None
    parts = None
    if not cursor:
        return text
    cursor.execute('SELECT * FROM MDBCDMPF WHERE CDGRNO = ?', (group, ))
    parts = cursor.fetchall()
    if not parts or len(parts) < 1:
        return text
    processed_parts = {}
    for part in parts:
        key = int(part["CDKEY1"])
        if key in processed_parts:
            processed_parts[key] = f'*{key}* _несколько вариантов_\n'
            continue
        partName = fetch_cat_record(catalogue, 'MDBPNCPF', 'PNPNCD', part["CDPNCD"])
        partCount = part["CDCQTY"].lstrip('0')
        if partCount and partCount != '1':
            partCount = f' \\(*{partCount}* шт\\.\\)'
        else:
            partCount = ''
        processed_parts[key] = f'*{key}* `/p {part["CDPTNO"]}` {partCount}\n'

    for key in sorted(processed_parts):
        text += processed_parts[key]

    return text

def check_group(cmd: str, pattern: str):
    global db

    result = {'text': '', 'media': None}

    catalogue = 'GENKFM002A'
    cursor = db[catalogue] if catalogue in db else None
    if not cursor:
        return result

    cursor.execute('SELECT * FROM MDBGNMPF WHERE GNGRNO = ?', (pattern, ))
    groups = cursor.fetchall()
    if not groups or len(groups) < 1:
        pattern_ru = pattern.lower()
        cursor.execute('SELECT * FROM MDBGNMPF WHERE GNLGEG LIKE ? OR GNLGRU LIKE ?', (f'%{pattern}%', f'%{pattern_ru}%', ))
        groups = cursor.fetchall()

    lower_bound = 4

    if not groups or len(groups) < 1:
        return result
    elif len(groups) == 1:
        group = groups[0]
        text = f'Группа: {group["GNGRNO"]} \\- *{escape_markdown(group["GNLGEG"], 2)}* \\({escape_markdown(group["GNLGRU"], 2)}\\)\n'
        text += list_group(group["GNGRNO"])
        result = {'text' : text, 'media' : get_media(catalogue, group["GNGRNO"])}
    elif len(groups) <= lower_bound:
        text = 'Обнаружено несколько групп запчастей:\n'
        media = []
        for group in groups:
            gmedia = get_media(catalogue, group["GNGRNO"])
            picture_numbers = ''
            if len(gmedia) == 1:
                picture_numbers = f'\\(рис\\. *{len(media) + 1}*\\) '
            elif len(gmedia) > 1:
                start = len(media) + 1
                end = len(media) + len(gmedia)
                picture_numbers = f'\\(рис\\. *{start} \\- {end}*\\) '
            text += f'`{cmd} {group["GNGRNO"]}` {picture_numbers}\\- {escape_markdown(group["GNLGEG"], 2)} \\({escape_markdown(group["GNLGRU"], 2)}\\)\n'
            media += gmedia
        result = {'text' : text, 'media' : media}
    else:
        text = 'Обнаружено несколько групп запчастей\\.\n'
        text += f'Их больше *{lower_bound}*\\, поэтому я выведу их списком без рисунков:\n'
        for group in groups:
            text += f'`{cmd} {group["GNGRNO"]}` \\- {escape_markdown(group["GNLGEG"], 2)} \\({escape_markdown(group["GNLGRU"], 2)}\\)\n'
        result['text'] = text

    return result

async def on_vin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    user = update.effective_user.mention_markdown_v2()

    vin = ''
    text = ''
    if update.message.text:
        vin = update.message.text[5:].strip().upper()

    if vin:
        if (len(vin) == 17) and ('JA' in vin):
            text = check_ru_vin(vin)

            logger.info(f'Пользователь {user} запросил VIN {vin}')
        else:
            text = 'Идентификационный номер автомобиля \\(VIN\\) должен состоять ровно из *17* символов и содержать бувы *JA*\\. Например:\n'
            text += '/vin X4XJA563000000000\n'
            logger.info(f'Пользователь {user} отправил странный запрос: {vin[0:20]}')
    else:
        text = 'Вы забыли написать идентификационный номер автомобиля \\(VIN\\)\\. Попробуйте вот так:\n'
        text += '/vin X4XJA563000000000\n'
        logger.info(f'Пользователь {user} отправил пустой запрос')

    if text:
        await update.message.reply_markdown_v2(text)

async def on_part(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    user = update.effective_user.mention_markdown_v2()

    cmd = ''
    pno = ''
    text = ''
    media = []
    if update.message.text:
        arguments = update.message.text.split(' ', 1)
        cmd = arguments[0]
        if len(arguments) > 1:
            pno = arguments[1].strip().upper()[:20]
    if pno:
        result = check_pno(pno)
        text = result['text']
        media = result['media']
    else:
        text = f'Вы забыли написать номер запчасти\\. Попробуйте вот так:\n{cmd} 0K95412205\n'
        logger.info(f'Пользователь {user} отправил пустой номер запчасти')

    if media:
        await update.message.reply_media_group(media, caption=text, parse_mode=ParseMode.MARKDOWN_V2)
    elif text:
        await update.message.reply_markdown_v2(text)
    else:
        await update.message.reply_markdown_v2('Запчасть с таким номером не найдена\\.')

async def on_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    user = update.effective_user.mention_markdown_v2()

    cmd = ''
    pattern = ''
    text = ''
    media = []
    lower_bound = 4
    if update.message.text:
        arguments = update.message.text.split(' ', 1)
        cmd = arguments[0]
        if len(arguments) > 1:
            pattern = arguments[1].strip().upper()[:64]
    if len(pattern) >= lower_bound:
        result = check_group(cmd, pattern)
        text = result['text']
        media = result['media']
    else:
        text = f'Вы забыли написать наименование группы или оно слишком короткое \\(меньше {lower_bound} символов\\)\\.\nПримеры запросов:\n'
        text += f'`{cmd} 0900A`\n'
        text += f'`{cmd} valve`\n'
        text += f'`{cmd} усилитель`\n'
        logger.info(f'Пользователь {user} отправил пустой номер группы')

    if media:
        await update.message.reply_media_group(media, caption=text, parse_mode=ParseMode.MARKDOWN_V2)
    elif text:
        await update.message.reply_markdown_v2(text)
    else:
        await update.message.reply_markdown_v2('Не удалось найти ни одной группы по вашему запросу\\.')

async def on_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = 'Доступные команды:\n\n'
    text += '/vin - получить информацию об автомобиле по VIN-номеру.\n'
    text += '/part (или /p) - получить информацию о запчасти.\n'
    await update.message.reply_text(text)

async def error_handler(update: Optional[object], context: CallbackContext):
    logging.getLogger('telegram.ext.Application').error(context.error.message)

def row_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def main() -> None:
    global db

    settings_file_name = Path(__file__).with_suffix('.json')
    with open(settings_file_name, 'r') as fp:
        try:
            settings = json.load(fp)
        except:
            settings = {'api_token': ''}

    db['VIN_RU'] = None
    db['EPC'] = None

    for key in db.keys():
        connection = sqlite3.connect(f'{key}.db', check_same_thread=False)
        if connection:
            connection.row_factory = row_factory
            db[key] = connection.cursor()

    if db['EPC']:
        for row in db['EPC'].execute('SELECT * FROM MDBCATPF WHERE CMGRTY = ?', ('FM', )):
            name = row['CMBPNO']
            connection = sqlite3.connect(f'PC/{name}.db', check_same_thread=False)
            if connection:
                connection.row_factory = row_factory
                db[name] = connection.cursor()

    application = Application.builder().token(settings['api_token']).build()
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler('start', on_start))
    application.add_handler(CommandHandler('help', on_help))
    application.add_handler(CommandHandler('vin', on_vin))
    application.add_handler(CommandHandler(['p', 'part'], on_part))
    application.add_handler(CommandHandler(['g', 'group'], on_group))

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
