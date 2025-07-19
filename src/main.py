import asyncio
import logging
import pandas as pd
import re
import os
import time
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from typing import Union
from aiogram.exceptions import TelegramBadRequest

logging.basicConfig(level=logging.INFO)
bot = Bot(token='8104630789:AAGAZ-ITfW3F0Rtno-h8iFUIiKqkxl1gqu0')
dp = Dispatcher()
FILE_PUT = "donors.xlsx"

# ID чата, куда пересылаются вопросы
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "-1002709368305"))
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

def nayti_usera_po_telegram_id(telegram_id):
    try:
        df = pd.read_excel(FILE_PUT, engine='openpyxl')
        if 'Телеграм' not in df.columns:
            return None
        df['Телеграм'] = df['Телеграм'].astype(str).str.replace(r'\.0$', '', regex=True)
        match = df[df['Телеграм'] == str(telegram_id)]
        if not match.empty:
            return match.iloc[0]
    except FileNotFoundError:
        return None
    return None

def nayti_usera_po_nomeru(nomer_telefona):
    try:
        baza_dannyh = pd.read_excel(FILE_PUT, engine='openpyxl')
        baza_dannyh['Телефон'] = baza_dannyh['Телефон'].astype(str).str.replace(r'\D', '', regex=True)
        user = baza_dannyh[baza_dannyh['Телефон'].str.contains(nomer_telefona, na=False)]
        if not user.empty:
            return user.iloc[0]
    except FileNotFoundError:
        return None
    return None

def obnovit_telegram_id(nomer_telefona, telegram_id):
    try:
        df = pd.read_excel(FILE_PUT, engine='openpyxl')
        df['Телефон_чистый'] = df['Телефон'].astype(str).str.replace(r'\D', '', regex=True)
        idx_list = df.index[df['Телефон_чистый'].str.contains(nomer_telefona, na=False)].tolist()
        if idx_list:
            if 'Телеграм' not in df.columns:
                df['Телеграм'] = None
            df.loc[idx_list[0], 'Телеграм'] = str(telegram_id)
            df = df.drop(columns=['Телефон_чистый'])
            df.to_excel(FILE_PUT, index=False)
    except Exception:
        pass

def dobavit_usera(dannie):
    try:
        df = pd.read_excel(FILE_PUT, engine='openpyxl')
    except FileNotFoundError:
        df = pd.DataFrame(columns=['ФИО', 'Группа', 'Телефон', 'Телеграм'])
    if 'Телеграм' not in df.columns:
        df['Телеграм'] = None
    dannie['Телеграм'] = str(dannie.get('Телеграм')) if dannie.get('Телеграм') else None
    noviy_user = pd.DataFrame([dannie])
    df = pd.concat([df, noviy_user], ignore_index=True)
    df.to_excel(FILE_PUT, index=False)

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
            df_donors = pd.DataFrame(columns=['ФИО', 'Группа', 'Телефон', 'Телеграм'])
            df_donors.to_excel(writer, sheet_name='donors', index=False)
            
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
