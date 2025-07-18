import asyncio
import logging
import pandas as pd
import re
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove

logging.basicConfig(level=logging.INFO)
bot = Bot(token='8104630789:AAGAZ-ITfW3F0Rtno-h8iFUIiKqkxl1gqu0')
dp = Dispatcher()
FILE_PUT = "donors.xlsx"

class SostoyaniyaRegistracii(StatesGroup):
    ozhidanie_soglasiya = State()
    ozhidanie_fio = State()
    ozhidanie_kategorii = State()
    ozhidanie_gruppy = State()
    podtverzhdenie_fio = State()

# --- Клавиатуры ---
# Кнопка для запроса номера телефона
knopka_dlya_nomera = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Отправить мой номер 📱", request_contact=True)]],
    resize_keyboard=True
)

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
    await state.update_data(nomer_telefona=nomer_telefona) # Сохраняем номер в память
    
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
            "Похоже, ты у нас впервые! Для продолжения нужно принять условия использования данных.",
            reply_markup=knopki_soglasiya
        )
        await state.set_state(SostoyaniyaRegistracii.ozhidanie_soglasiya)

# Обработчик нажатия на кнопку "Принимаю условия"
# TODO!!!!!!! Текст
@dp.callback_query(SostoyaniyaRegistracii.ozhidanie_soglasiya, F.data == "soglasen")
async def obrabotchik_soglasiya(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Отлично! Теперь введи, пожалуйста, свои Фамилию Имя Отчество.")
    await state.set_state(SostoyaniyaRegistracii.ozhidanie_fio)

# Обработчик для подтверждения ФИО
@dp.callback_query(SostoyaniyaRegistracii.podtverzhdenie_fio)
async def obrabotchik_podtverzhdeniya_fio(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == 'fio_verno':
        await callback.message.edit_text("Отлично! Рад снова тебя видеть.")
        # Тут будет переход в главное меню
        await state.clear()
    else:
        # Если юзер сказал "нет, это не я", запускаем регистрацию заново
        await callback.message.edit_text("Понял. Давай тогда пройдем регистрацию. Для начала - прими условия.")
        await callback.message.answer("Текст соглашения...", reply_markup=knopki_soglasiya)
        await state.set_state(SostoyaniyaRegistracii.ozhidanie_soglasiya)


# Обработчик который ловит ФИО
@dp.message(SostoyaniyaRegistracii.ozhidanie_fio)
async def obrabotchik_fio(message: types.Message, state: FSMContext):
    fio = message.text.strip()
    # простенькая проверка на валиднорсть 
    # TODO!!!!!!! Валидация
    if len(fio.split()) < 2:
        await message.answer("Пожалуйста, введи полное Фамилию Имя Отчество. Например: Иванов Иван Иванович")
        return
        
    # фильтр имени
    fio_krasivoe = " ".join([word.capitalize() for word in fio.split()])
    
    await state.update_data(fio=fio_krasivoe)
    await message.answer("Спасибо! Теперь выбери свою категорию:", reply_markup=knopki_kategoriy)
    await state.set_state(SostoyaniyaRegistracii.ozhidanie_kategorii)

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
        await state.clear()

# обработчик, который ловит номер группы
@dp.message(SostoyaniyaRegistracii.ozhidanie_gruppy)
async def obrabotchik_gruppy(message: types.Message, state: FSMContext):
    gruppa = message.text.strip()
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
    await state.clear()

# --- Запуск бота ---
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main()) 