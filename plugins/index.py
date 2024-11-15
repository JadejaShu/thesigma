import logging, re, asyncio
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait
from pyrogram.errors.exceptions.bad_request_400 import ChannelInvalid, ChatAdminRequired, UsernameInvalid, UsernameNotModified
from info import CHANNELS, LOG_CHANNEL, ADMINS
from database.ia_filterdb import save_file
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from utils import temp
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
lock = asyncio.Lock()


@Client.on_message(filters.chat(CHANNELS) & (filters.document | filters.video | filters.audio))         
async def media(bot, message):
    for file_type in ("document", "video", "audio"):
        media = getattr(message, file_type, None)
        if media is not None: break
    else: return
    media.file_type = file_type
    media.caption = message.caption
    await save_file(media)



@Client.on_callback_query(filters.regex(r'^index'))
async def index_files(bot, query):
    logger.info(f"Received callback query: {query.data}")
    if query.data.startswith('index_cancel'):
        temp.CANCEL = True
        return await query.answer("Cᴀɴᴄᴇʟʟɪɴɢ Iɴᴅᴇxɪɴɢ", show_alert=True)
        
    perfx, chat, lst_msg_id = query.data.split("#")
    if lock.locked():
        return await query.answer('Wᴀɪᴛ Uɴᴛɪʟ Pʀᴇᴠɪᴏᴜs Pʀᴏᴄᴇss Cᴏᴍᴘʟᴇᴛᴇ', show_alert=True)
    msg = query.message
    button = InlineKeyboardMarkup([[
        InlineKeyboardButton('🚫 ᴄᴀɴᴄᴇʟʟ', "index_cancel")
    ]])
    await msg.edit("Indexing is started ✨", reply_markup=button)
    logger.info(f"Started indexing for chat: {chat} up to message ID: {lst_msg_id}")
    
    try:
        chat = int(chat)
    except ValueError:  # Catch only specific exceptions
        logger.warning(f"Chat ID is not an integer: {chat}")
        chat = chat  # Leave chat as a string if it's not an integer
    
    await index_files_to_db(int(lst_msg_id), chat, msg, bot)


@Client.on_message((filters.forwarded | (filters.regex("(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")) & filters.text ) & filters.private & filters.incoming & filters.user(ADMINS))
async def send_for_index(bot, message):
    if message.text:
        regex = re.compile("(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")
        match = regex.match(message.text)
        if not match: return await message.reply('Invalid link')
        chat_id = match.group(4)
        last_msg_id = int(match.group(5))
        if chat_id.isnumeric(): chat_id  = int(("-100" + chat_id))
    elif message.forward_from_chat.type == enums.ChatType.CHANNEL:
        last_msg_id = message.forward_from_message_id
        chat_id = message.forward_from_chat.username or message.forward_from_chat.id
    else: return
    try: await bot.get_chat(chat_id)
    except ChannelInvalid: return await message.reply('This may be a private channel / group. Make me an admin over there to index the files.')
    except (UsernameInvalid, UsernameNotModified): return await message.reply('Invalid Link specified.')
    except Exception as e: return await message.reply(f'Errors - {e}')
    try: k = await bot.get_messages(chat_id, last_msg_id)
    except: return await message.reply('Make Sure That Iam An Admin In The Channel, if channel is private')
    if k.empty: return await message.reply('This may be group and iam not a admin of the group.')
    buttons = InlineKeyboardMarkup([[
        InlineKeyboardButton('✨ ʏᴇꜱ', callback_data=f'index#{chat_id}#{last_msg_id}')
        ],[
        InlineKeyboardButton('🚫 ᴄʟᴏꜱᴇ', callback_data='close_data')
    ]])               
    await message.reply(f'Do You Want To Index This Channel/ Group ?\n\nChat ID/ Username: <code>{chat_id}</code>\nLast Message ID: <code>{last_msg_id}</code>', reply_markup=buttons)
    

@Client.on_message(filters.command('setskip') & filters.user(ADMINS))
async def set_skip_number(bot, message):
    if len(message.command) == 2:
        try: skip = int(message.text.split(" ", 1)[1])
        except: return await message.reply("Skip Number Should Be An Integer.")
        await message.reply(f"Successfully Set Skip Number As {skip}")
        temp.CURRENT = int(skip)
    else:
        await message.reply("Give Me A Skip Number")


async def index_files_to_db(lst_msg_id, chat, msg, bot):
    total_files = 0
    duplicate = 0
    errors = 0
    deleted = 0
    no_media = 0
    unsupported = 0
    processed = 0
    batch_size = 100  # Number of messages processed before reporting progress
    max_parallel_tasks = 10  # Maximum number of parallel tasks to process files

    async with lock:
        try:
            current = temp.CURRENT
            temp.CANCEL = False
            messages_to_process = []

            # Fetch the messages in parallel
            async for message in bot.iter_messages(chat, lst_msg_id, temp.CURRENT):
                messages_to_process.append(message)

            # Parallel process the messages in batches
            async def process_message(message):
                nonlocal total_files, duplicate, errors, deleted, no_media, unsupported, processed
                processed += 1
                if temp.CANCEL:
                    return  # Stop processing if cancelled

                if message.empty:
                    deleted += 1
                    return
                if not message.media:
                    no_media += 1
                    return
                if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.AUDIO, enums.MessageMediaType.DOCUMENT]:
                    unsupported += 1
                    return

                media = getattr(message, message.media.value, None)
                if not media:
                    unsupported += 1
                    return

                # Process the file and save to DB
                try:
                    media.file_type = message.media.value
                    media.caption = message.caption
                    saved, status = await save_file(media)
                    if saved:
                        total_files += 1
                    elif status == 0:
                        duplicate += 1
                    elif status == 2:
                        errors += 1
                except Exception as e:
                    logger.error(f"Error saving media: {e}")
                    errors += 1

            # Process messages in parallel (using async gather with limited concurrency)
            tasks = []
            with ThreadPoolExecutor(max_workers=max_parallel_tasks) as executor:
                for message in messages_to_process:
                    tasks.append(executor.submit(process_message, message))

            # Wait for all tasks to finish
            await asyncio.gather(*tasks)

            # Progress message
            await msg.edit(
                f"Indexing Completed!\n\n"
                f"Processed Messages: <code>{processed}</code>\n"
                f"Files Saved: <code>{total_files}</code>\n"
                f"Duplicate Files: <code>{duplicate}</code>\n"
                f"Deleted Messages: <code>{deleted}</code>\n"
                f"Non-Media Skipped: <code>{no_media + unsupported}</code>\n"
                f"Errors: <code>{errors}</code>"
            )

        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            await msg.edit(f"Error during indexing: <code>{e}</code>")
