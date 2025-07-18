from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

router = Router()

@router.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """
    This handler receives messages with `/start` command
    """
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="Отправить номер телефона", request_contact=True),
            ]
        ],
        resize_keyboard=True,
    )
    await message.answer(
        "Привет! Это бот для доноров НИЯУ МИФИ. "
        "Для начала работы, пожалуйста, авторизуйтесь по номеру телефона.",
        reply_markup=keyboard,
    ) 