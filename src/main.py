import asyncio
import logging
import pandas as pd
import re
import os
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from typing import Union

logging.basicConfig(level=logging.INFO)
bot = Bot(token='8104630789:AAGAZ-ITfW3F0Rtno-h8iFUIiKqkxl1gqu0')
dp = Dispatcher()
FILE_PUT = "donors.xlsx"

# ID чата, куда пересылаются вопросы
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "-1002709368305"))

# --- Главное меню ---
menu_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Личный кабинет")],
        [KeyboardButton(text="Информация о донорстве")],
        [KeyboardButton(text="Вопрос организаторам")]
    ],
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

# --- Функции для работы с Excel ---

def nayti_usera_po_telegram_id(telegram_id):
    try:
        baza_dannyh = pd.read_excel(FILE_PUT, engine='openpyxl')
        if 'Телеграм' not in baza_dannyh.columns:
            return None
        user = baza_dannyh[baza_dannyh['Телеграм'].astype(str) == str(telegram_id)]
        if not user.empty:
            return user.iloc[0]
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
            df.loc[idx_list[0], 'Телеграм'] = telegram_id
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

@dp.callback_query(F.data.startswith("info_"))
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
    
    await callback.message.edit_text(text, parse_mode="Markdown")
    # Чтобы убрать часики на кнопке
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

    text += "**В регистре ДКМ:** Нет данных" # Этой колонки пока нет

    await message.answer(text, parse_mode="Markdown")

# --- Вопросы организаторам ---
# Единый обработчик для начала диалога с вопросом
async def start_question_dialog(message_or_callback: Union[types.Message, types.CallbackQuery], state: FSMContext):
    if isinstance(message_or_callback, types.CallbackQuery):
        await message_or_callback.message.edit_reply_markup(reply_markup=None)
        message = message_or_callback.message
    else:
        message = message_or_callback
    await message.answer("Напиши свой вопрос, и я передам его организаторам.", reply_markup=ReplyKeyboardRemove())
    await state.set_state(SostoyaniyaVoprosa.ozhidanie_voprosa)

@dp.message(F.text == "Вопрос организаторам")
async def start_question_text(message: types.Message, state: FSMContext):
    await start_question_dialog(message, state)

@dp.callback_query(F.data == "ask_question_inline")
async def start_question_inline(callback: types.CallbackQuery, state: FSMContext):
    await start_question_dialog(callback, state)


@dp.message(SostoyaniyaVoprosa.ozhidanie_voprosa)
async def recieve_question(message: types.Message, state: FSMContext):
    dannie_usera = await state.get_data()
    phone = dannie_usera.get("nomer_telefona", "не указан")
    fio = dannie_usera.get("fio", "Не указано")
    username = dannie_usera.get("username") or message.from_user.username or "нет username"

    # Пересылаем сообщение с вопросом и сохраняем ссылку на автора
    await bot.forward_message(ADMIN_CHAT_ID, message.chat.id, message.message_id)
    await bot.send_message(ADMIN_CHAT_ID, f"Вопрос от {fio} (тел: {phone}, @{username})")

    await message.answer("Спасибо! Вопрос отправлен организаторам.", reply_markup=menu_kb)
    await state.set_state(None)


@dp.message(F.chat.id == ADMIN_CHAT_ID, F.reply_to_message)
async def answer_to_user(message: types.Message):
    # Если организатор отвечает на пересланное сообщение, отправляем ответ пользователю
    if message.reply_to_message and message.reply_to_message.forward_from:
        user_id = message.reply_to_message.forward_from.id
        await bot.send_message(user_id, f"Ответ от организаторов:\n{message.text}")
        await message.answer("✅ Ответ отправлен пользователю.")


# --- Запуск бота ---
async def main():
    logging.info("Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот выключен") 
