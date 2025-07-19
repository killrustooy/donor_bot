import asyncio
import logging
import pandas as pd
import re
import os
import time
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from typing import Union
from aiogram.exceptions import TelegramBadRequest

logging.basicConfig(level=logging.INFO)
bot = Bot(token='8104630789:AAGAZ-ITfW3F0Rtno-h8iFUIiKqkxl1gqu0') 
dp = Dispatcher()
FILE_PUT = "donors.xlsx"
DONORS_SHEET_NAME = "Sheet1" # <-- Указываем правильное имя листа

# ID чата, куда пересылаются вопросы
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "-1002709368305"))
ADMIN_IDS = [1214800918] # <-- Возвращаем ID администраторов

# Словари для опросов и лимитов
questions_map: dict[int, int] = {}
last_question_time: dict[int, float] = {}

# --- Главное меню ---
menu_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Вопрос организаторам"), KeyboardButton(text="Личный кабинет")],
        [KeyboardButton(text="Информация о донорстве"), KeyboardButton(text="Записаться на донацию")]
    ],
    resize_keyboard=True
)

# Универсальная кнопка «Главное меню»
back_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Главное меню")]],
    resize_keyboard=True
)

# Кнопка для запроса номера телефона (показывается при старте)
knopka_dlya_nomera = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Отправить мой номер 📱", request_contact=True)]],
    resize_keyboard=True
)

# --- Админ-меню (Reply Keyboards) ---
admin_main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Управление донорами 👥")],
        [KeyboardButton(text="Управление мероприятиями 📅")],
    ],
    resize_keyboard=True
)

admin_donors_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Добавить донора ➕"), KeyboardButton(text="Редактировать донора ✍️")],
        [KeyboardButton(text="Скачать базу доноров 📋")],
        [KeyboardButton(text="⬅️ Назад в главное меню")],
    ],
    resize_keyboard=True
)

admin_events_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Создать мероприятие ➕")],
        [KeyboardButton(text="Загрузить статистику ДД 📈")],
        [KeyboardButton(text="⬅️ Назад в главное меню")],
    ],
    resize_keyboard=True
)


# --- Состояния ---

class SostoyaniyaRegistracii(StatesGroup):
    ozhidanie_soglasiya = State()
    ozhidanie_fio = State()
    ozhidanie_kategorii = State()
    ozhidanie_gruppy = State()
    podtverzhdenie_fio = State()

# --- Состояния для ДД ---
class SostoyaniyaDD(StatesGroup):
    vybor_daty = State()
    prichina_neyavki = State()

# --- Состояния вопросов ---
class SostoyaniyaVoprosa(StatesGroup):
    ozhidanie_voprosa = State()

# --- Состояния для Админки ---
class AdminAddDonor(StatesGroup):
    awaiting_fio = State()
    awaiting_category = State()
    awaiting_group = State()
    awaiting_phone = State()
    confirmation = State()

class AdminEditDonor(StatesGroup):
    awaiting_phone_to_find = State()
    choosing_field_to_edit = State()
    awaiting_new_value = State()

class AdminCreateEvent(StatesGroup):
    awaiting_date = State()
    awaiting_center = State()
    awaiting_external_link = State()
    confirmation = State()

class AdminUploadStats(StatesGroup):
    awaiting_file = State()


# Кнопки для согласия
knopki_soglasiya = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принимаю условия", callback_data="soglasen")]
    ]
)

# Кнопки для выбора категории
knopki_kategoriy = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Студент", callback_data="kategoriya_student")],
        [InlineKeyboardButton(text="Сотрудник", callback_data="kategoriya_sotrudnik")],
        [InlineKeyboardButton(text="Внешний донор", callback_data="kategoriya_vneshniy")]
    ]
)

# Кнопки для подтверждения ФИО
knopki_podtverzhdeniya_fio = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Да, это я", callback_data="fio_verno")],
        [InlineKeyboardButton(text="Нет, это не я", callback_data="fio_neverno")]
    ]
)

# Кнопки для опроса о причине неявки
knopki_oprosa_neyavki = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Медотвод (болезнь)", callback_data="opros_medotvod")],
        [InlineKeyboardButton(text="Личные причины", callback_data="opros_lichnye")],
        [InlineKeyboardButton(text="Передумал(а)", callback_data="opros_peredumal")],
    ]
)

# --- Клавиатуры для инфо-разделов ---
knopki_info = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="О донорстве крови", callback_data="info_krov")],
        [InlineKeyboardButton(text="О донорстве костного мозга", callback_data="info_kostniy_mozg")],
        [InlineKeyboardButton(text="О донациях в МИФИ", callback_data="info_mifi")]
    ]
)

# Кнопка для связи с организаторами, которую мы покажем при конфликте
knopka_svyazi_konflikt = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="✍️ Написать организаторам", callback_data="ask_question_inline")]
    ]
)

# Кнопка «Назад» для инфо-разделов
knopka_info_back = InlineKeyboardMarkup(
    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="info_back")]]
)

# --- Функции для работы с Excel ---

async def process_stats_file(file_path, bot, chat_id):
    """Обрабатывает загруженный Excel файл со статистикой и обновляет данные доноров."""
    try:
        df_stats = pd.read_excel(file_path)
        required_columns = ['ФИО', 'Дата', 'ЦК'] # ИСПРАВЛЕНО: ожидаем колонку 'ЦК'
        if not all(col in df_stats.columns for col in required_columns):
            await bot.send_message(chat_id, f"<b>Ошибка!</b>\nВ загруженном файле отсутствуют необходимые колонки. "
                                            f"Убедитесь, что есть столбцы: {', '.join(required_columns)}", parse_mode="HTML")
            return

        all_sheets = pd.read_excel(FILE_PUT, sheet_name=None)
        if DONORS_SHEET_NAME not in all_sheets:
            await bot.send_message(chat_id, f"Ошибка: лист '{DONORS_SHEET_NAME}' не найден в основной базе.")
            return

        df_donors = all_sheets[DONORS_SHEET_NAME]
        
        # Подготовка колонок для статистики, если их нет
        stats_cols = {
            'Кол-во Гаврилова': 0, 'Кол-во ФМБА': 0,
            'Дата последней донации Гаврилова': '', 'Дата последней донации ФМБА': ''
        }
        for col, default in stats_cols.items():
            if col not in df_donors.columns:
                df_donors[col] = default

        # Приведение к нижнему регистру для надежного сравнения
        df_donors['ФИО_lower'] = df_donors['ФИО'].str.strip().str.lower()
        
        updated_count = 0
        not_found_donors = []

        for _, row in df_stats.iterrows():
            fio = row['ФИО'].strip().lower()
            donor_indices = df_donors.index[df_donors['ФИО_lower'] == fio].tolist()

            if not donor_indices:
                not_found_donors.append(row['ФИО'])
                continue
            
            donor_index = donor_indices[0]
            updated_count += 1
            
            center_name = row['ЦК'].lower() # ИСПРАВЛЕНО: читаем из колонки 'ЦК'
            try:
                donation_date = pd.to_datetime(row['Дата'], dayfirst=False).strftime('%d.%m.%Y')
            except ValueError:
                logging.warning(f"Неверный формат даты для {row['ФИО']}: {row['Дата']}")
                continue

            if 'гаврилова' in center_name:
                current_donations = df_donors.loc[donor_index, 'Кол-во Гаврилова']
                # Проверяем, не является ли значение NaN (пустой ячейкой)
                if pd.isna(current_donations):
                    current_donations = 0
                df_donors.loc[donor_index, 'Кол-во Гаврилова'] = int(current_donations) + 1
                df_donors.loc[donor_index, 'Дата последней донации Гаврилова'] = donation_date
            elif 'фмба' in center_name:
                current_donations = df_donors.loc[donor_index, 'Кол-во ФМБА']
                # Проверяем, не является ли значение NaN (пустой ячейкой)
                if pd.isna(current_donations):
                    current_donations = 0
                df_donors.loc[donor_index, 'Кол-во ФМБА'] = int(current_donations) + 1
                df_donors.loc[donor_index, 'Дата последней донации ФМБА'] = donation_date

        df_donors = df_donors.drop(columns=['ФИО_lower'])
        all_sheets[DONORS_SHEET_NAME] = df_donors

        with pd.ExcelWriter(FILE_PUT, engine='openpyxl') as writer:
            for sheet_name, data in all_sheets.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)

        # Формирование отчета
        report = f"✅ Статистика успешно обработана!\n\n" \
                 f"Всего строк в файле: {len(df_stats)}\n" \
                 f"Обновлено доноров: {updated_count}\n"
        
        if not_found_donors:
            report += f"\n❗️Не найдено доноров ({len(not_found_donors)}):\n" + "\n".join(not_found_donors)
        
        await bot.send_message(chat_id, report)

    except Exception as e:
        await bot.send_message(chat_id, f"❌ Произошла критическая ошибка при обработке файла: {e}")
        logging.error(f"Ошибка при обработке файла статистики: {e}", exc_info=True)


def dobavit_sobytie(date: str, center: str, link: str):
    """Добавляет новое мероприятие в лист 'events', сохраняя все остальные листы."""
    try:
        try:
            all_sheets = pd.read_excel(FILE_PUT, sheet_name=None)
            df_events = all_sheets.get('events')
            if df_events is None:
                # Если лист 'events' отсутствует, создаем его с нужными колонками
                df_events = pd.DataFrame(columns=['id', 'date', 'center', 'is_active', 'reg_link_external'])
        except FileNotFoundError:
            # Если файл вообще не найден, создаем его с нуля
            sozdat_listy_v_excel()
            all_sheets = pd.read_excel(FILE_PUT, sheet_name=None)
            df_events = all_sheets['events']

        # Генерируем новый ID
        new_id = df_events['id'].max() + 1 if not df_events.empty else 1
        
        new_event = {
            'id': new_id,
            'date': date,
            'center': center,
            'is_active': True, # Новые мероприятия по умолчанию активны
            'reg_link_external': link
        }
        
        # Используем pd.concat для добавления новой строки
        df_events = pd.concat([df_events, pd.DataFrame([new_event])], ignore_index=True)
        all_sheets['events'] = df_events
        
        with pd.ExcelWriter(FILE_PUT, engine='openpyxl') as writer:
            for sheet_name, data in all_sheets.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
                
    except Exception as e:
        logging.error(f"Критическая ошибка при добавлении мероприятия в Excel: {e}")

def update_donor_data(phone_to_find: str, column_to_update: str, new_value: str) -> bool:
    """Находит донора по номеру телефона и обновляет данные в указанной колонке, сохраняя все листы."""
    try:
        all_sheets = pd.read_excel(FILE_PUT, engine='openpyxl', sheet_name=None)
        if DONORS_SHEET_NAME not in all_sheets:
            logging.error(f"Лист '{DONORS_SHEET_NAME}' не найден в Excel файле при обновлении.")
            return False

        df = all_sheets[DONORS_SHEET_NAME]
        
        df['Телефон_norm'] = df['Телефон'].astype(str).str.replace(r'[^\d]', '', regex=True)
        phone_to_find_norm = re.sub(r'[^\d]', '', phone_to_find)

        user_index = df.index[df['Телефон_norm'] == phone_to_find_norm].tolist()

        if not user_index:
            logging.warning(f"Донор с телефоном {phone_to_find} не найден для обновления.")
            return False

        df.loc[user_index[0], column_to_update] = new_value
        df = df.drop(columns=['Телефон_norm'])
        all_sheets[DONORS_SHEET_NAME] = df
        
        with pd.ExcelWriter(FILE_PUT, engine='openpyxl') as writer:
            for sheet_name, data in all_sheets.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
        return True

    except (FileNotFoundError, ValueError) as e:
        logging.error(f"Ошибка при обновлении данных донора: {e}")
        return False

def nayti_usera_po_telegram_id(telegram_id):
    """Ищет пользователя по ID телеграма в листе 'donors'."""
    try:
        df = pd.read_excel(FILE_PUT, engine='openpyxl', sheet_name=DONORS_SHEET_NAME)
        if 'Телеграм' not in df.columns:
            return None
        df['Телеграм'] = df['Телеграм'].astype(str).str.replace(r'\.0$', '', regex=True)
        match = df[df['Телеграм'] == str(telegram_id)]
        if not match.empty:
            return match.iloc[0]
    except (FileNotFoundError, ValueError):
        return None
    return None

def nayti_usera_po_nomeru(nomer_telefona):
    """Ищет пользователя по номеру телефона в листе 'donors'."""
    try:
        df = pd.read_excel(FILE_PUT, engine='openpyxl', sheet_name=DONORS_SHEET_NAME)
        df['Телефон_norm'] = df['Телефон'].astype(str).str.replace(r'[^\d]', '', regex=True)
        nomer_telefona_norm = re.sub(r'[^\d]', '', nomer_telefona)
        user = df[df['Телефон_norm'] == nomer_telefona_norm]
        if not user.empty:
            return user.iloc[0]
    except (FileNotFoundError, ValueError):
        return None
    return None

def obnovit_telegram_id(nomer_telefona, telegram_id):
    """Обновляет Telegram ID для донора, сохраняя все листы в Excel."""
    try:
        all_sheets = pd.read_excel(FILE_PUT, sheet_name=None)
        if DONORS_SHEET_NAME not in all_sheets:
            logging.error(f"Лист '{DONORS_SHEET_NAME}' не найден при обновлении telegram_id.")
            return

        df = all_sheets[DONORS_SHEET_NAME]
        df['Телефон_norm'] = df['Телефон'].astype(str).str.replace(r'[^\d]', '', regex=True)
        nomer_telefona_norm = re.sub(r'[^\d]', '', nomer_telefona)
        idx_list = df.index[df['Телефон_norm'] == nomer_telefona_norm].tolist()
        
        if idx_list:
            if 'Телеграм' not in df.columns:
                df['Телеграм'] = None
            df.loc[idx_list[0], 'Телеграм'] = str(telegram_id)
        
        df = df.drop(columns=['Телефон_norm'])
        all_sheets[DONORS_SHEET_NAME] = df

        with pd.ExcelWriter(FILE_PUT, engine='openpyxl') as writer:
            for sheet_name, data in all_sheets.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
    except (FileNotFoundError, ValueError) as e:
        logging.error(f"Ошибка при обновлении telegram_id: {e}")

def dobavit_usera(dannie):
    """Добавляет пользователя в лист 'donors', сохраняя все остальные листы."""
    try:
        try:
            all_sheets = pd.read_excel(FILE_PUT, sheet_name=None)
            df = all_sheets.get(DONORS_SHEET_NAME)
            if df is None:
                # Если лист не найден, создаем пустой DataFrame с нужными колонками
                df = pd.DataFrame(columns=['ФИО', 'Группа', 'Телефон', 'Телеграм', 'Кол-во Гаврилова', 'Кол-во ФМБА', 'Дата последней донации Гаврилова', 'Дата последней донации ФМБА'])
        except FileNotFoundError:
            # Если файл не найден, создаем его с нуля
            sozdat_listy_v_excel()
            all_sheets = pd.read_excel(FILE_PUT, sheet_name=None)
            df = all_sheets[DONORS_SHEET_NAME]

        # Добавляем недостающие колонки статистики, если их нет
        stats_cols = {'Кол-во Гаврилова': 0, 'Кол-во ФМБА': 0, 'Дата последней донации Гаврилова': None, 'Дата последней донации ФМБА': None}
        for col, default_val in stats_cols.items():
            if col not in df.columns:
                df[col] = default_val

        # Добавляем нового пользователя, заполняя статистику по умолчанию
        new_user_data = {
            'ФИО': dannie.get('ФИО'), 'Группа': dannie.get('Группа'), 'Телефон': dannie.get('Телефон'),
            'Телеграм': str(dannie.get('Телеграм')) if dannie.get('Телеграм') else None,
            'Кол-во Гаврилова': 0, 'Кол-во ФМБА': 0,
            'Дата последней донации Гаврилова': None, 'Дата последней донации ФМБА': None
        }
        
        noviy_user = pd.DataFrame([new_user_data])
        df = pd.concat([df, noviy_user], ignore_index=True)
        all_sheets[DONORS_SHEET_NAME] = df
        
        with pd.ExcelWriter(FILE_PUT, engine='openpyxl') as writer:
            for sheet_name, data in all_sheets.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
                
    except Exception as e:
        logging.error(f"Критическая ошибка при добавлении пользователя в Excel: {e}")

# --- АДМИН-ПАНЕЛЬ ---

@dp.message(Command("admin"))
async def admin_menu_handler(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет доступа к этой команде.")
        return
    await message.answer("Добро пожаловать в админ-панель!", reply_markup=admin_main_kb)

@dp.message(F.text == "Управление донорами 👥")
async def admin_donors_menu_handler(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Меню управления донорами:", reply_markup=admin_donors_kb)

@dp.message(F.text == "⬅️ Назад в главное меню")
async def back_to_admin_main_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Главное меню админ-панели.", reply_markup=admin_main_kb)

# --- Управление мероприятиями (админ) ---

@dp.message(F.text == "Управление мероприятиями 📅")
async def admin_events_menu_handler(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Меню управления мероприятиями:", reply_markup=admin_events_kb)

@dp.message(F.text == "Скачать базу доноров 📋")
async def admin_download_db(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    if os.path.exists(FILE_PUT):
        try:
            document = types.FSInputFile(FILE_PUT)
            await message.answer_document(
                document,
                caption="✅ Актуальная база данных доноров."
            )
        except Exception as e:
            logging.error(f"Ошибка при отправке файла базы данных: {e}")
            await message.answer(f"❌ Произошла ошибка при отправке файла: {e}")
    else:
        await message.answer(f"❌ Файл базы данных `{FILE_PUT}` не найден.")

@dp.message(F.text == "Загрузить статистику ДД 📈", StateFilter(None))
async def admin_upload_stats_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminUploadStats.awaiting_file)
    await message.answer("Пожалуйста, загрузите Excel-файл со статистикой.\n\n"
                         "В файле должны быть колонки: '<b>ФИО</b>', '<b>Дата</b>' (в формате ММ/ДД/ГГГГ), '<b>ЦК</b>'.",
                         parse_mode="HTML", reply_markup=ReplyKeyboardRemove())

@dp.message(AdminUploadStats.awaiting_file, F.document)
async def admin_upload_stats_process(message: types.Message, state: FSMContext, bot: Bot):
    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        await message.answer("Пожалуйста, загрузите файл в формате Excel (.xlsx или .xls).")
        return

    await message.answer("Файл получен, начинаю обработку...")
    
    # Создаем временный путь для файла
    file_info = await bot.get_file(message.document.file_id)
    downloaded_file = await bot.download_file(file_info.file_path)
    
    temp_file_path = f"temp_{message.document.file_name}"
    with open(temp_file_path, 'wb') as new_file:
        new_file.write(downloaded_file.getvalue())

    # Обрабатываем файл и отправляем отчет
    await process_stats_file(temp_file_path, bot, message.chat.id)

    # Удаляем временный файл и выходим из состояния
    os.remove(temp_file_path)
    await state.clear()
    await message.answer("Вы вернулись в меню управления мероприятиями.", reply_markup=admin_events_kb)

@dp.message(AdminUploadStats.awaiting_file)
async def admin_upload_stats_wrong_input(message: types.Message):
    await message.answer("Пожалуйста, отправьте документ Excel или вернитесь в меню.")


@dp.message(F.text == "Создать мероприятие ➕", StateFilter(None))
async def admin_create_event_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminCreateEvent.awaiting_date)
    await message.answer("<b>Шаг 1: Введите дату мероприятия в формате ДД.ММ.ГГГГ</b>", parse_mode="HTML", reply_markup=ReplyKeyboardRemove())

@dp.message(AdminCreateEvent.awaiting_date)
async def admin_create_event_date(message: types.Message, state: FSMContext):
    date_text = message.text.strip()
    try:
        # Простая валидация формата
        time.strptime(date_text, "%d.%m.%Y")
    except ValueError:
        await message.answer("<b>Неверный формат даты.</b>\nПожалуйста, введите дату в формате ДД.ММ.ГГГГ (например, 25.10.2024).", parse_mode="HTML")
        return
    
    await state.update_data(date=date_text)
    await state.set_state(AdminCreateEvent.awaiting_center)
    await message.answer("<b>Шаг 2: Введите название центра крови (ЦК)</b>", parse_mode="HTML")

@dp.message(AdminCreateEvent.awaiting_center)
async def admin_create_event_center(message: types.Message, state: FSMContext):
    center_name = message.text.strip()
    if not center_name:
        await message.answer("Название центра не может быть пустым. Пожалуйста, введите название.")
        return
    
    await state.update_data(center=center_name)
    await state.set_state(AdminCreateEvent.awaiting_external_link)
    await message.answer("<b>Шаг 3: Введите ссылку для доп. регистрации внешних доноров</b>\n(Если такой ссылки нет, отправьте минус '-')", parse_mode="HTML")

@dp.message(AdminCreateEvent.awaiting_external_link)
async def admin_create_event_link(message: types.Message, state: FSMContext):
    link = message.text.strip()
    if link == '-':
        link = None
    elif not (link.startswith('http://') or link.startswith('https://')):
        await message.answer("<b>Неверный формат ссылки.</b>\nСсылка должна начинаться с http:// или https://. Если ссылки нет, отправьте минус '-'.", parse_mode="HTML")
        return
        
    await state.update_data(link=link)
    event_data = await state.get_data()
    
    confirmation_text = (
        "<b>Проверьте данные нового мероприятия:</b>\n\n"
        f"<b>Дата:</b> {event_data['date']}\n"
        f"<b>Центр крови:</b> {event_data['center']}\n"
        f"<b>Ссылка для внешних:</b> {event_data.get('link', 'Нет')}\n\n"
        "Создать мероприятие?"
    )
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, создать", callback_data="admin_event_confirm"),
        InlineKeyboardButton(text="❌ Нет, отменить", callback_data="admin_event_cancel")
    ]])
    await message.answer(confirmation_text, parse_mode="HTML", reply_markup=confirm_kb)
    await state.set_state(AdminCreateEvent.confirmation)

@dp.callback_query(AdminCreateEvent.confirmation, F.data == "admin_event_confirm")
async def admin_create_event_confirm(callback: types.CallbackQuery, state: FSMContext):
    event_data = await state.get_data()
    
    dobavit_sobytie(
        date=event_data.get('date'),
        center=event_data.get('center'),
        link=event_data.get('link')
    )
    
    await callback.message.edit_text(f"✅ Мероприятие <b>{event_data.get('date')}</b> в <b>{event_data.get('center')}</b> успешно создано.", parse_mode="HTML")
    await state.clear()
    await callback.message.answer("Вы в меню управления мероприятиями.", reply_markup=admin_events_kb)

@dp.callback_query(StateFilter("*"), F.data == "admin_event_cancel")
async def admin_create_event_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Создание мероприятия отменено.")
    await callback.message.answer("Вы в меню управления мероприятиями.", reply_markup=admin_events_kb)


# --- Добавление донора (админ) ---

@dp.message(F.text == "Добавить донора ➕", StateFilter(None))
async def admin_add_donor_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminAddDonor.awaiting_fio)
    await message.answer("<b>Шаг 1: Введите ФИО нового донора</b>", parse_mode="HTML", reply_markup=ReplyKeyboardRemove())

@dp.message(AdminAddDonor.awaiting_fio)
async def admin_add_fio(message: types.Message, state: FSMContext):
    fio = message.text.strip()
    if not (2 <= len(fio.split()) <= 3) or re.search(r'[^а-яА-ЯёЁ\s-]', fio):
        await message.answer("<b>Ошибка!</b>\nПожалуйста, введите ФИО (2 или 3 слова), используя только русские буквы.", parse_mode="HTML")
        return
    fio_krasivoe = " ".join([word.capitalize() for word in fio.split()])
    await state.update_data(fio=fio_krasivoe)
    await state.set_state(AdminAddDonor.awaiting_category)
    await message.answer("<b>Шаг 2: Выберите категорию донора</b>", parse_mode="HTML", reply_markup=knopki_kategoriy)

@dp.callback_query(AdminAddDonor.awaiting_category, F.data.startswith("kategoriya_"))
async def admin_add_category(callback: types.CallbackQuery, state: FSMContext):
    category_map = {"kategoriya_student": "Студент", "kategoriya_sotrudnik": "Сотрудник", "kategoriya_vneshniy": "Внешний донор"}
    category_key = callback.data.split('_')[1]
    category_name = category_map.get(f"kategoriya_{category_key}")
    
    await state.update_data(category_name=category_name)
    await callback.message.delete()

    if category_key == "student":
        await state.set_state(AdminAddDonor.awaiting_group)
        await callback.message.answer("<b>Шаг 3: Введите номер учебной группы (например, Б20-505)</b>", parse_mode="HTML")
    else:
        await state.set_state(AdminAddDonor.awaiting_phone)
        await callback.message.answer("<b>Шаг 3: Введите номер телефона донора (начиная с 8)</b>", parse_mode="HTML")

@dp.message(AdminAddDonor.awaiting_group)
async def admin_add_group(message: types.Message, state: FSMContext):
    gruppa = message.text.strip().upper()
    if not re.fullmatch(r'^[А-Я]\d{2}-\d{3}$', gruppa):
        await message.answer("<b>Неверный формат группы.</b> Пожалуйста, введите номер в формате X00-000.", parse_mode="HTML")
        return
    await state.update_data(group=gruppa)
    await state.set_state(AdminAddDonor.awaiting_phone)
    await message.answer("<b>Шаг 4: Введите номер телефона донора (начиная с 8)</b>", parse_mode="HTML")

@dp.message(AdminAddDonor.awaiting_phone)
async def admin_add_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.fullmatch(r'8[0-9]{10}', phone):
        await message.answer("<b>Неверный формат номера.</b>\nПожалуйста, введите номер в формате 8XXXXXXXXXX.", parse_mode="HTML")
        return
    
    await state.update_data(phone=phone)
    user_data = await state.get_data()
    
    gruppa_info = user_data.get('group', user_data.get('category_name'))
    
    confirmation_text = (
        "<b>Проверьте введенные данные:</b>\n\n"
        f"<b>ФИО:</b> {user_data['fio']}\n"
        f"<b>Группа/Категория:</b> {gruppa_info}\n"
        f"<b>Телефон:</b> {user_data['phone']}\n\n"
        "Все верно?"
    )
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, добавить", callback_data="admin_add_confirm"),
        InlineKeyboardButton(text="❌ Нет, отменить", callback_data="admin_add_cancel")
    ]])
    await message.answer(confirmation_text, parse_mode="HTML", reply_markup=confirm_kb)
    await state.set_state(AdminAddDonor.confirmation)

@dp.callback_query(AdminAddDonor.confirmation, F.data == "admin_add_confirm")
async def admin_add_confirm(callback: types.CallbackQuery, state: FSMContext):
    user_data = await state.get_data()
    gruppa_value = user_data.get('group', user_data.get('category_name'))
    
    zapis = {
        'ФИО': user_data.get('fio'),
        'Группа': gruppa_value,
        'Телефон': user_data.get('phone'),
        'Телеграм': None,
    }
    dobavit_usera(zapis)
    
    await callback.message.edit_text(f"✅ Донор <b>{user_data.get('fio')}</b> успешно добавлен.", parse_mode="HTML")
    await state.clear()
    await callback.message.answer("Вы в меню управления донорами.", reply_markup=admin_donors_kb)
    
@dp.callback_query(StateFilter("*"), F.data == "admin_add_cancel")
async def admin_add_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Добавление донора отменено.")
    await callback.message.answer("Вы в меню управления донорами.", reply_markup=admin_donors_kb)

# --- Редактирование донора (админ) ---

@dp.message(F.text == "Редактировать донора ✍️", StateFilter(None))
async def admin_edit_donor_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminEditDonor.awaiting_phone_to_find)
    await message.answer("Введите номер телефона донора, которого хотите найти (начиная с 8).", reply_markup=ReplyKeyboardRemove())

@dp.message(AdminEditDonor.awaiting_phone_to_find)
async def admin_edit_find_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.fullmatch(r'8[0-9]{10}', phone):
        await message.answer("<b>Неверный формат номера.</b>\nПожалуйста, введите номер в формате 8XXXXXXXXXX.", parse_mode="HTML")
        return
    
    user_data = nayti_usera_po_nomeru(phone)
    if user_data is None:
        await message.answer("Донор с таким номером не найден. Попробуйте еще раз или вернитесь в меню.", reply_markup=admin_donors_kb)
        await state.clear()
        return

    await state.update_data(phone_to_edit=phone, user_data=user_data.to_dict())
    
    info = (
        "<b>Найден донор:</b>\n\n"
        f"<b>ФИО:</b> {user_data.get('ФИО')}\n"
        f"<b>Группа/Категория:</b> {user_data.get('Группа')}\n"
        f"<b>Телефон:</b> {user_data.get('Телефон')}\n\n"
        "Что вы хотите изменить?"
    )
    edit_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Изменить ФИО", callback_data="edit_field_ФИО")],
        [InlineKeyboardButton(text="Изменить Группу/Категорию", callback_data="edit_field_Группа")],
        [InlineKeyboardButton(text="Изменить Телефон", callback_data="edit_field_Телефон")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="edit_cancel")]
    ])
    await message.answer(info, parse_mode="HTML", reply_markup=edit_kb)
    await state.set_state(AdminEditDonor.choosing_field_to_edit)

@dp.callback_query(AdminEditDonor.choosing_field_to_edit, F.data.startswith("edit_field_"))
async def admin_edit_choose_field(callback: types.CallbackQuery, state: FSMContext):
    field = callback.data.split('_')[-1]
    await state.update_data(field_to_edit=field)
    await state.set_state(AdminEditDonor.awaiting_new_value)
    await callback.message.edit_text(f"Введите новое значение для поля '<b>{field}</b>':", parse_mode="HTML")

@dp.message(AdminEditDonor.awaiting_new_value)
async def admin_edit_get_new_value(message: types.Message, state: FSMContext):
    data = await state.get_data()
    field = data['field_to_edit']
    phone_to_find = data['phone_to_edit']
    new_value = message.text.strip()

    # Тут можно добавить валидацию для каждого поля
    if field == "Телефон" and not re.fullmatch(r'8[0-9]{10}', new_value):
        await message.answer("Неверный формат телефона. Введите 11 цифр, начиная с 8.")
        return
    if field == "Группа" and not (re.fullmatch(r'^[А-Я]\d{2}-\d{3}$', new_value.upper()) or new_value in ["Сотрудник", "Внешний донор"]):
         await message.answer("Неверный формат. Введите номер группы (X00-000) или категорию ('Сотрудник'/'Внешний донор').")
         return

    if update_donor_data(phone_to_find, field, new_value):
        await message.answer(f"✅ Данные для донора обновлены. Поле '<b>{field}</b>' теперь '<b>{new_value}</b>'.", parse_mode="HTML", reply_markup=admin_donors_kb)
    else:
        await message.answer("❌ Произошла ошибка при обновлении данных.", reply_markup=admin_donors_kb)
    
    await state.clear()

@dp.callback_query(StateFilter("*"), F.data == "edit_cancel")
async def admin_edit_cancel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Редактирование отменено.")
    await callback.message.answer("Вы в меню управления донорами.", reply_markup=admin_donors_kb)


# --- Обработчики команд и сообщений ---

@dp.message(Command("start"))
async def command_start(message: types.Message, state: FSMContext):
    await state.clear()
    user = nayti_usera_po_telegram_id(message.from_user.id)
    if user is not None:
        fio = user.get('ФИО', 'Донор')
        phone = user.get('Телефон', 'не указан')
        await state.update_data(
            fio=fio, 
            nomer_telefona=phone, 
            username=message.from_user.username,
            telegram_id=message.from_user.id
        )
        await message.answer(f"С возвращением, {fio}!", reply_markup=menu_kb)
    else:
        await message.answer(
            "Привет! Я бот для доноров. Чтобы начать, мне нужен твой номер телефона.",
            reply_markup=knopka_dlya_nomera
        )

@dp.message(F.contact)
async def contact_handler(message: types.Message, state: FSMContext):
    nomer_telefona = message.contact.phone_number.replace("+", "")
    telegram_id = message.from_user.id
    await state.update_data(nomer_telefona=nomer_telefona, username=message.from_user.username, telegram_id=telegram_id)
    await message.answer("Спасибо, номер получен!", reply_markup=ReplyKeyboardRemove())
    user = nayti_usera_po_nomeru(nomer_telefona)
    if user is not None:
        obnovit_telegram_id(nomer_telefona, telegram_id)
        fio = user['ФИО']
        await state.update_data(fio=fio)
        await message.answer(f"Привет! Ты - {fio}?", reply_markup=knopki_podtverzhdeniya_fio)
        await state.set_state(SostoyaniyaRegistracii.podtverzhdenie_fio)
    else:
        await message.answer(
            "Похоже, ты у нас впервые! Для продолжения, пожалуйста, ознакомься с "
            "[политикой обработки персональных данных](https://telegra.ph/POLITIKA-NIYAU-MIFI-V-OTNOSHENII-OBRABOTKI-PERSONALNYH-DANNYH-07-18) "
            "и нажми кнопку ниже.",
            parse_mode="Markdown",
            disable_web_page_preview=True, # отключаем превью, чтобы не было громоздко
            reply_markup=knopki_soglasiya
        )
        await state.set_state(SostoyaniyaRegistracii.ozhidanie_soglasiya)

# Обработчик нажатия на кнопку "Принимаю условия"
@dp.callback_query(SostoyaniyaRegistracii.ozhidanie_soglasiya, F.data == "soglasen")
async def obrabotchik_soglasiya(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Отлично! Теперь введи, пожалуйста, свои Фамилию Имя Отчество.")
    await state.set_state(SostoyaniyaRegistracii.ozhidanie_fio)

# Обработчик для подтверждения ФИО
@dp.callback_query(SostoyaniyaRegistracii.podtverzhdenie_fio)
async def obrabotchik_podtverzhdeniya_fio(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == 'fio_verno':
        await callback.message.edit_text("Отлично! Рад снова тебя видеть.")
        await callback.message.answer("Главное меню:", reply_markup=menu_kb)
        await state.clear()
    else:
        await callback.message.edit_text(
            "Этот номер телефона уже зарегистрирован на другого пользователя.\n"
            "Если это ваш номер, но ФИО указано неверно, обратитесь к организаторам, нажав на кнопку ниже. ",
            reply_markup=knopka_svyazi_konflikt
        )
        await state.clear()


# Обработчик который ловит ФИО
@dp.message(SostoyaniyaRegistracii.ozhidanie_fio)
async def obrabotchik_fio(message: types.Message, state: FSMContext):
    fio = message.text.strip()

    # Улучшенная валидация ФИО
    # 1. Проверка на количество слов (должно быть 2 или 3)
    if not (2 <= len(fio.split()) <= 3):
        await message.answer("Пожалуйста, введи полное Фамилию Имя Отчество (или Фамилию и Имя).")
        return

    # 2. Проверка на запрещенные символы (разрешены только русские буквы, пробелы и дефисы)
    if re.search(r'[^а-яА-ЯёЁ\s-]', fio):
        await message.answer("В ФИО могут быть только русские буквы, пробелы и дефис. Попробуй еще раз.")
        return

    # Приводим ФИО к красивому виду: "Иванов Иван-Петрович"
    fio_krasivoe = " ".join([word.capitalize() for word in fio.split()])

    await state.update_data(fio=fio_krasivoe)
    await message.answer("Спасибо! Теперь выбери свою категорию:", reply_markup=knopki_kategoriy)
    await state.set_state(SostoyaniyaRegistracii.ozhidanie_kategorii)

# Обработчик на случай, если пользователь пишет текстом вместо нажатия кнопки категории
@dp.message(SostoyaniyaRegistracii.ozhidanie_kategorii)
async def nepravilnaya_kategoriya(message: types.Message):
    await message.answer("Пожалуйста, выбери один из вариантов на кнопках ниже.")

# выбор категории
@dp.callback_query(SostoyaniyaRegistracii.ozhidanie_kategorii)
async def obrabotchik_kategorii(callback: types.CallbackQuery, state: FSMContext):
    kategoriya_eng = callback.data.split('_')[1] # вытаскиваем 'student', 'sotrudnik' или 'vneshniy'
    await state.update_data(kategoriya=kategoriya_eng)

    if kategoriya_eng == "student":
        await callback.message.edit_text("Понял. Теперь введи номер своей учебной группы.")
        await state.set_state(SostoyaniyaRegistracii.ozhidanie_gruppy)
    else:
        dannie_usera = await state.get_data()

        # переводим на русский для записи в файл
        kategorii_map = {
            'sotrudnik': 'Сотрудник',
            'vneshniy': 'Внешний донор'
        }
        kategoriya_rus = kategorii_map.get(dannie_usera.get('kategoriya'), 'Не указана')

        zapis = {
            'Телефон': dannie_usera.get('nomer_telefona'),
            'ФИО': dannie_usera.get('fio'),
            'Группа': kategoriya_rus,
            'Телеграм': dannie_usera.get('telegram_id'),
        }
        dobavit_usera(zapis)

        await callback.message.edit_text("Ты успешно зарегистрирован!")
        # переход в главное меню
        await state.clear()
        await bot.send_message(callback.from_user.id, "Главное меню:", reply_markup=menu_kb)

# обработчик, который ловит номер группы
@dp.message(SostoyaniyaRegistracii.ozhidanie_gruppy)
async def obrabotchik_gruppy(message: types.Message, state: FSMContext):
    gruppa = message.text.strip().upper()

    # Валидация формата группы (например, А22-501)
    if not re.match(r'^[А-Я]\d{2}-\d{3}$', gruppa):
        await message.answer("Неверный формат группы. Пожалуйста, введи номер в формате X00-000, например: А22-501")
        return

    await state.update_data(gruppa=gruppa)

    await message.answer("Отлично, регистрация почти завершена!")
    dannie_usera = await state.get_data()

    zapis = {
        'Телефон': dannie_usera.get('nomer_telefona'),
        'ФИО': dannie_usera.get('fio'),
        'Группа': dannie_usera.get('gruppa'),
        'Телеграм': dannie_usera.get('telegram_id'),
        # ... и другие колонки
    }
    dobavit_usera(zapis)

    await message.answer("Ты успешно зарегистрирован!")
    # и снова переход в главное меню
    await state.clear()
    await message.answer("Главное меню:", reply_markup=menu_kb)

# --- Информационные разделы ---
@dp.message(F.text == "Информация о донорстве")
async def info_section(message: types.Message):
    await message.answer("Выберите интересующий вас раздел:", reply_markup=knopki_info)

# Фильтр теперь более точный и не ловит "info_back"
@dp.callback_query(F.data.in_(["info_krov", "info_kostniy_mozg", "info_mifi"]))
async def send_info(callback: types.CallbackQuery):
    action = callback.data.removeprefix("info_")
    text = ""
    if action == "krov":
        text = (
            "**Требования к донорам:**\n"
            "- Возраст от 18 до 60 лет\n"
            "- Вес не менее 50 кг\n"
            "- Отсутствие противопоказаний\n\n"
            "**Подготовка к донации:**\n"
            "- За 48 часов не употреблять алкоголь\n"
            "- Накануне вечером легкий ужин\n"
            "- Утром легкий завтрак (сладкий чай, сухари)\n\n"
            "Полный список противопоказаний можно найти на сайте Центра Крови."
        )
    elif action == "kostniy_mozg":
        text = (
            "**Донорство костного мозга - это важно!**\n"
            "Это шанс спасти жизнь человека, больного лейкозом или другим заболеванием крови.\n\n"
            "**Как вступить в регистр:**\n"
            "1. Сдать пробирку крови (4 мл) на донорской акции.\n"
            "2. Заполнить анкету.\n\n"
            "**Процесс донации:**\n"
            "Процедура похожа на сдачу тромбоцитов и абсолютно безопасна."
        )
    elif action == "mifi":
        text = (
            "**Дни донора в НИЯУ МИФИ:**\n"
            "1. Зарегистрируйся в боте на ближайшую дату.\n"
            "2. Приходи в указанное время и место.\n"
            "3. Не забудь паспорт и хорошее настроение!\n\n"
            "**Ближайший День Донора:**\n"
            "Следите за анонсами!"
        )

    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=knopka_info_back)
    # Чтобы убрать часики на кнопке
    await callback.answer()

# Хендлер возврата из инфо-раздела
@dp.callback_query(F.data == "info_back")
async def info_back(callback: types.CallbackQuery):
    # Удаляем текущее сообщение с текстом
    await callback.message.delete()
    # И присылаем новое с выбором разделов
    await callback.message.answer("Выберите интересующий вас раздел:", reply_markup=knopki_info)
    await callback.answer()

# --- Личный кабинет ---
@dp.message(F.text == "Личный кабинет")
async def lichnyi_kabinet(message: types.Message):
    user_data = nayti_usera_po_telegram_id(message.from_user.id)
    if user_data is None:
        await message.answer("Хм, не нашел тебя в базе доноров. Пожалуйста, перезапусти бота командой /start, чтобы авторизоваться.")
        return

    fio = user_data.get('ФИО', 'Не указано')
    donations_gavrilova = user_data.get('Кол-во Гаврилова', 0)
    donations_fmba = user_data.get('Кол-во ФМБА', 0)
    total_donations = donations_gavrilova + donations_fmba

    last_donation_gavrilova = user_data.get('Дата последней донации Гаврилова')
    last_donation_fmba = user_data.get('Дата последней донации ФМБА')

    last_donation_date = None
    last_donation_center = ""

    # Преобразуем строки в даты, если они есть
    if pd.notna(last_donation_gavrilova):
        date_gavrilova = pd.to_datetime(last_donation_gavrilova)
    else:
        date_gavrilova = None

    if pd.notna(last_donation_fmba):
        date_fmba = pd.to_datetime(last_donation_fmba)
    else:
        date_fmba = None

    if date_gavrilova and date_fmba:
        if date_gavrilova > date_fmba:
            last_donation_date = date_gavrilova.strftime('%d.%m.%Y')
            last_donation_center = "в ЦК им. Гаврилова"
        else:
            last_donation_date = date_fmba.strftime('%d.%m.%Y')
            last_donation_center = "в ЦК ФМБА"
    elif date_gavrilova:
        last_donation_date = date_gavrilova.strftime('%d.%m.%Y')
        last_donation_center = "в ЦК им. Гаврилова"
    elif date_fmba:
        last_donation_date = date_fmba.strftime('%d.%m.%Y')
        last_donation_center = "в ЦК ФМБА"


    text = (
        f"👤 **Твой личный кабинет**\n\n"
        f"**ФИО:** {fio}\n"
        f"**Всего донаций:** {total_donations}\n"
    )

    if last_donation_date:
        text += f"**Последняя донация:** {last_donation_date} {last_donation_center}\n"
    else:
        text += "**Последняя донация:** Данных нет\n"

    text += "**В регистре ДКМ:** Нет данных"  # Этой колонки пока нет

    # Добавляем информацию о записях на ДД
    moy_zapisy = poluchit_registracii_usera(message.from_user.id)
    if moy_zapisy:
        text += "\n\n**Мои записи на Дни Донора:**\n"
        for zapis in moy_zapisy:
            sobytie = poluchit_sobytie_po_id(zapis['event_id'])
            if sobytie is not None:
                text += f"– {sobytie['date']} в {sobytie['center']}\n"
    else:
        text += "\n\nУ тебя нет активных записей на Дни Донора."

    await message.answer(text, parse_mode="Markdown")

# --- Регистрация на День Донора ---

@dp.message(F.text == "Записаться на донацию")
async def zapis_na_donaciyu(message: types.Message):
    user_data = nayti_usera_po_telegram_id(message.from_user.id)
    if user_data is None:
        await message.answer("Чтобы записаться, нужно сначала авторизоваться. Пожалуйста, перезапусти бота командой /start.")
        return
    
    sobytiya = poluchit_aktivnye_sobytiya()
    
    if sobytiya:
        knopki = []
        for sobytie in sobytiya:
            text_knopki = f"{sobytie['date']} - {sobytie['center']}"
            knopki.append([InlineKeyboardButton(text=text_knopki, callback_data=f"reg_dd_{sobytie['id']}")])
        
        klaviatura = InlineKeyboardMarkup(inline_keyboard=knopki)
        await message.answer("Открыта запись на Дни Донора! Выбери удобную дату и место:", reply_markup=klaviatura)
    else:
        await message.answer("К сожалению, сейчас нет активных записей на Дни Донора. Следи за анонсами!")

@dp.callback_query(F.data.startswith("reg_dd_"))
async def podtverdit_zapis_na_dd(callback: types.CallbackQuery):
    await callback.answer()
    event_id = int(callback.data.split('_')[-1])
    telegram_id = callback.from_user.id

    sobytie = poluchit_sobytie_po_id(event_id)
    if sobytie is None or not sobytie['is_active']:
        await callback.message.edit_text("Извини, запись на это мероприятие уже закрыта.")
        return

    if proverit_registraciyu_na_sobytie(telegram_id, event_id):
        await callback.message.edit_text("Ты уже записан на эту дату!")
        return

    user_data = nayti_usera_po_telegram_id(telegram_id)
    fio = user_data.get('ФИО', 'Неизвестный донор')
    category = user_data.get('Группа')

    dobavit_registraciyu(event_id, telegram_id, fio)
    
    text_confirmation = f"Отлично! Ты записан на донацию {sobytie['date']} в {sobytie['center']}."
    
    if category == 'Внешний донор':
        link = sobytie.get('reg_link_external')
        if link and pd.notna(link):
            text_confirmation += f"\n\n❗️**ВАЖНО:** Так как ты внешний донор, пройди доп. регистрацию по [этой ссылке]({link})."

    await callback.message.edit_text(text_confirmation, parse_mode="Markdown", disable_web_page_preview=True)

# --- Вопросы организаторам ---
@dp.message(F.text == "Вопрос организаторам")
async def start_question_text(message: types.Message, state: FSMContext):
    await message.answer("Напиши свой вопрос, и я передам его организаторам.", reply_markup=ReplyKeyboardRemove())
    await state.set_state(SostoyaniyaVoprosa.ozhidanie_voprosa)

@dp.message(SostoyaniyaVoprosa.ozhidanie_voprosa)
async def recieve_question(message: types.Message, state: FSMContext):
    dannie_usera = await state.get_data()
    phone = dannie_usera.get("nomer_telefona", "не указан")
    fio = dannie_usera.get("fio", "Не указано")
    username = dannie_usera.get("username") or message.from_user.username or "нет username"

    fwd_msg = await bot.forward_message(ADMIN_CHAT_ID, message.chat.id, message.message_id)
    questions_map[fwd_msg.message_id] = message.from_user.id
    await bot.send_message(ADMIN_CHAT_ID, f"Вопрос от {fio} (тел: {phone}, @{username})")

    await message.answer("Спасибо! Вопрос отправлен организаторам.", reply_markup=menu_kb)
    await state.clear()

# Ответ организатора пользователю
@dp.message(F.chat.id == ADMIN_CHAT_ID, F.reply_to_message)
async def answer_to_user(message: types.Message):
    user_id = None
    if message.reply_to_message:
        user_id = questions_map.get(message.reply_to_message.message_id)
        if not user_id and message.reply_to_message.forward_from:
            user_id = message.reply_to_message.forward_from.id
    
    if user_id:
        await bot.send_message(user_id, f"Ответ от организаторов:\n{message.text}")
        await message.answer("✅ Ответ отправлен пользователю.")
    else:
        await message.answer("⚠️ Не удалось определить пользователя для ответа. Возможно, бот был перезапущен.")

# --- Опрос о неявке (пока не используется автоматически) ---
@dp.callback_query(F.data.startswith("opros_"))
async def obrabotka_oprosa_neyavki(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    prichina = callback.data.split('_')[1]
    # В callback.message.reply_to_message.text мы можем хранить event_id, но это не очень надежно.
    # Для реального использования нужен будет более стабильный способ передать event_id.
    # Пока что это просто пример.
    # event_id = ... 
    # telegram_id = callback.from_user.id
    # obnovit_status_registracii(telegram_id, event_id, prichina)
    
    await callback.message.edit_text(f"Спасибо за твой ответ! Мы учтем это.")
    
# --- Функции для мероприятий ---

def sozdat_listy_v_excel():
    """Проверяет наличие файла donors.xlsx и листов 'events', 'registrations'. Создает их при отсутствии."""
    
    # Если файла нет, создаем его с нуля со всеми листами
    if not os.path.exists(FILE_PUT):
        with pd.ExcelWriter(FILE_PUT, engine='openpyxl') as writer:
            # Лист доноров
            df_donors = pd.DataFrame(columns=['ФИО', 'Группа', 'Телефон', 'Телеграм', 'Кол-во Гаврилова', 'Кол-во ФМБА', 'Дата последней донации Гаврилова', 'Дата последней донации ФМБА'])
            df_donors.to_excel(writer, sheet_name=DONORS_SHEET_NAME, index=False)
            
            # Лист мероприятий
            df_events = pd.DataFrame({
                'id': [1, 2], 'date': ['2023-10-27', '2023-10-28'],
                'center': ['Центр крови им. О.К. Гаврилова', 'Центр крови ФМБА'], 
                'is_active': [True, False], 
                'reg_link_external': ['https://it.mephi.ru/web/guest/den-donora', 'https://it.mephi.ru/web/guest/den-donora-2']
            })
            df_events.to_excel(writer, sheet_name='events', index=False)

            # Лист регистраций
            df_regs = pd.DataFrame(columns=['event_id', 'telegram_id', 'fio', 'status', 'prichina_neyavki'])
            df_regs.to_excel(writer, sheet_name='registrations', index=False)
        return

    # Если файл существует, дописываем только НЕДОСТАЮЩИЕ листы
    try:
        with pd.ExcelFile(FILE_PUT) as xls:
            existing_sheets = xls.sheet_names
    except Exception:
        existing_sheets = [] # На случай, если файл поврежден
        
    # Используем 'append' режим для добавления новых листов
    with pd.ExcelWriter(FILE_PUT, engine='openpyxl', mode='a', if_sheet_exists='error') as writer:
        if 'events' not in existing_sheets:
            df_events = pd.DataFrame({
                'id': [1, 2], 'date': ['2023-10-27', '2023-10-28'],
                'center': ['Центр крови им. О.К. Гаврилова', 'Центр крови ФМБА'], 
                'is_active': [True, False], 
                'reg_link_external': ['https://it.mephi.ru/web/guest/den-donora', 'https://it.mephi.ru/web/guest/den-donora-2']
            })
            df_events.to_excel(writer, sheet_name='events', index=False)

        if 'registrations' not in existing_sheets:
            df_regs = pd.DataFrame(columns=['event_id', 'telegram_id', 'fio', 'status', 'prichina_neyavki'])
            df_regs.to_excel(writer, sheet_name='registrations', index=False)

def poluchit_aktivnye_sobytiya():
    """Возвращает список всех активных мероприятий."""
    try:
        df = pd.read_excel(FILE_PUT, sheet_name='events')
        active_events = df[df['is_active'] == True]
        return active_events.to_dict('records')
    except (FileNotFoundError, ValueError): # ValueError если лист не найден
        return []

def poluchit_sobytie_po_id(event_id):
    try:
        df = pd.read_excel(FILE_PUT, sheet_name='events')
        event = df[df['id'] == event_id]
        if not event.empty:
            return event.iloc[0]
    except Exception:
        return None

def proverit_registraciyu_na_sobytie(telegram_id, event_id):
    """Проверяет, зарегистрирован ли уже пользователь на конкретное событие."""
    try:
        df = pd.read_excel(FILE_PUT, sheet_name='registrations')
        df['telegram_id'] = df['telegram_id'].astype(str)
        registration = df[(df['telegram_id'] == str(telegram_id)) & (df['event_id'] == event_id)]
        return not registration.empty
    except Exception:
        return False

def dobavit_registraciyu(event_id, telegram_id, fio):
    """Добавляет запись о регистрации в файл."""
    try:
        all_sheets = pd.read_excel(FILE_PUT, sheet_name=None)
        
        df_regs = all_sheets.get('registrations', pd.DataFrame(columns=['event_id', 'telegram_id', 'fio', 'status', 'prichina_neyavki']))

        new_reg = {'event_id': event_id, 'telegram_id': str(telegram_id), 'fio': fio, 'status': 'registered'}
        
        df_regs = pd.concat([df_regs, pd.DataFrame([new_reg])], ignore_index=True)
        
        all_sheets['registrations'] = df_regs
        with pd.ExcelWriter(FILE_PUT, engine='openpyxl') as writer:
            for sheet_name, data in all_sheets.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
    except FileNotFoundError:
        df_regs = pd.DataFrame([{'event_id': event_id, 'telegram_id': str(telegram_id), 'fio': fio, 'status': 'registered'}])
        with pd.ExcelWriter(FILE_PUT, engine='openpyxl') as writer:
            df_regs.to_excel(writer, sheet_name='registrations', index=False)

def obnovit_status_registracii(telegram_id, event_id, prichina):
    try:
        all_sheets = pd.read_excel(FILE_PUT, sheet_name=None)
        if 'registrations' not in all_sheets: return
        
        df_regs = all_sheets['registrations']
        mask = (df_regs['telegram_id'].astype(str) == str(telegram_id)) & (df_regs['event_id'] == event_id)
        idx = df_regs.index[mask].tolist()
        if idx:
            df_regs.loc[idx[0], 'status'] = 'not_attended'
            df_regs.loc[idx[0], 'prichina_neyavki'] = prichina
        all_sheets['registrations'] = df_regs
        with pd.ExcelWriter(FILE_PUT, engine='openpyxl') as writer:
            for sheet_name, data in all_sheets.items():
                data.to_excel(writer, sheet_name=sheet_name, index=False)
    except Exception:
        pass

def poluchit_registracii_usera(telegram_id):
    """Возвращает все активные регистрации пользователя."""
    try:
        df_regs = pd.read_excel(FILE_PUT, sheet_name='registrations')
        # Ищем только регистрации со статусом 'registered'
        user_regs = df_regs[(df_regs['telegram_id'].astype(str) == str(telegram_id)) & (df_regs['status'] == 'registered')]
        return user_regs.to_dict('records')
    except (FileNotFoundError, ValueError):
        return []

# --- Блокировка команд в группах ---
@dp.message(F.text.startswith("/"), F.chat.type.in_(["group", "supergroup"]))
async def ignore_group_commands(message: types.Message):
    return

# --- Переход в главное меню из любой точки ---
@dp.message(F.text.casefold() == "главное меню")
async def go_main_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=menu_kb)

# --- Запуск бота ---
async def main():
    sozdat_listy_v_excel()
    logging.info("Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот выключен") 
