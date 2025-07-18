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

logging.basicConfig(level=logging.INFO)
bot = Bot(token='8104630789:AAGAZ-ITfW3F0Rtno-h8iFUIiKqkxl1gqu0')
dp = Dispatcher()
FILE_PUT = "donors.xlsx"

# ID чата, куда пересылаются вопросы
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "-1002709368305"))

# --- Главное меню ---
menu_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Вопрос организаторам")]],
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

# Функция для поиска пользователя по номеру телефона
def nayti_usera_po_nomeru(nomer_telefona):
    try:
        baza_dannyh = pd.read_excel(FILE_PUT, engine='openpyxl')
        baza_dannyh['Телефон'] = baza_dannyh['Телефон'].astype(str).str.replace(r'\D', '', regex=True)
        user = baza_dannyh[baza_dannyh['Телефон'].str.contains(nomer_telefona, na=False)]
        if not user.empty:
            return user.iloc[0]
    except FileNotFoundError:
        print(f"ОШИБКА: Файл {FILE_PUT} не найден!")
        return None
    return None

# Функция для добавления нового пользователя
def dobavit_usera(dannie):
    try:
        baza_dannyh = pd.read_excel(FILE_PUT, engine='openpyxl')
    except FileNotFoundError:
        baza_dannyh = pd.DataFrame(columns=['ФИО', 'Группа', 'Телефон'])
    
    noviy_user = pd.DataFrame([dannie])
    baza_dannyh = pd.concat([baza_dannyh, noviy_user], ignore_index=True)
    baza_dannyh.to_excel(FILE_PUT, index=False)


# --- Обработчики команд и сообщений ---

# Обработчик команды /start
@dp.message(Command("start"))
async def command_start(message: types.Message, state: FSMContext):
    await state.clear() # На случай, если пользователь перезапустил бота на полпути
    await message.answer(
        "Привет! Я бот для доноров. Чтобы начать, мне нужен твой номер телефона.",
        reply_markup=knopka_dlya_nomera
    )

# Обработчик, который ловит контакт
@dp.message(F.contact)
async def contact_handler(message: types.Message, state: FSMContext):
    nomer_telefona = message.contact.phone_number.replace("+", "")
    await state.update_data(nomer_telefona=nomer_telefona, username=message.from_user.username)  # Сохраняем номер и username
    
    # убираем кнопку с номером телефона
    await message.answer("Спасибо, номер получен!", reply_markup=ReplyKeyboardRemove())
    
    user = nayti_usera_po_nomeru(nomer_telefona)
    
    if user is not None:
        # Если нашли юзера в базе
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
        # Переход в главное меню
        await state.set_state(None)
        await bot.send_message(callback.from_user.id, "Главное меню:", reply_markup=menu_kb)
    else:
        # Если юзер сказал "нет, это не я", запускаем регистрацию заново
        await callback.message.edit_text("Понял. Давай тогда пройдем регистрацию.")
        await callback.message.answer(
            "Для начала, пожалуйста, ознакомься с "
            "[политикой обработки персональных данных](https://telegra.ph/POLITIKA-NIYAU-MIFI-V-OTNOSHENII-OBRABOTKI-PERSONALNYH-DANNYH-07-18) "
            "и нажми кнопку ниже.",
            parse_mode="Markdown",
            disable_web_page_preview=True, # отключаем превью, чтобы не было громоздко
            reply_markup=knopki_soglasiya
        )
        await state.set_state(SostoyaniyaRegistracii.ozhidanie_soglasiya)


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
        }
        dobavit_usera(zapis)
        
        await callback.message.edit_text("Ты успешно зарегистрирован!")
        # переход в главное меню
        await state.set_state(None)
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
        # ... и другие колонки
    }
    dobavit_usera(zapis)
    
    await message.answer("Ты успешно зарегистрирован!")
    # и снова переход в главное меню
    await state.set_state(None)
    await message.answer("Главное меню:", reply_markup=menu_kb)

# --- Запуск бота ---

# --- Вопросы организаторам ---

@dp.message(lambda msg: msg.text == "Вопрос организаторам")
async def start_question(message: types.Message, state: FSMContext):
    await message.answer("Напиши свой вопрос, и я передам его организаторам.", reply_markup=ReplyKeyboardRemove())
    await state.set_state(SostoyaniyaVoprosa.ozhidanie_voprosa)


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
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main()) 
