import logging
import asyncio 
import json
import os
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ConversationHandler, ContextTypes
from web3 import Web3
from telegram.helpers import escape_markdown

load_dotenv()

from config import (
    TELEGRAM_BOT_TOKEN, OWNER_TELEGRAM_ID, ADMIN_NOTIF_ID, OWNER_TELEGRAM_USERNAME,
    SENDER_ADDRESS, SENDER_PRIVATE_KEY, CHANNEL_ID, network_configs, LABUBU_AI_BOT_ID
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# States for ConversationHandler
AWAITING_VERIFICATION, AWAITING_CLAIM_ADDRESS, SELECTING_PURCHASE_TOKEN, AWAITING_PURCHASE_AMOUNT, AWAITING_TASK_CONFIRMATION = range(5)
SELECTING_REWARD_TOKEN, AWAITING_REWARD_ADDRESS, \
SELECTING_GET_MORE_TOKENS_TASK_TYPE, \
AWAITING_TWITTER_FOLLOW_1_CONFIRM, AWAITING_TWITTER_USERNAME_1, \
AWAITING_TWITTER_FOLLOW_2_CONFIRM, \
AWAITING_TWITTER_POST_LINK, \
AWAITING_LABUBU_SCREENSHOT = range(5, 13)
AWAITING_CHANNEL_JOIN = 13

# Dictionary to hold web3 instances
w3_instances = {}

# Name of the JSON file for user data
USER_DATA_FILE = 'user_data.json'
# Global dictionary to hold user data loaded from JSON
user_data_cache = {}

# NEW: Name of the JSON file for redeemed addresses
REDEEMED_ADDRESSES_FILE = 'redeemed_addresses.json'
# NEW: Global dictionary to hold redeemed addresses mapped to user IDs
redeemed_addresses_cache = {}


# Global dictionary to hold pending task verifications for admin approval
pending_task_verifications = {}

# Constants for the Get More Tokens task
TWITTER_PROFILES_TO_FOLLOW_1 = "@Petruk_Star_"
TWITTER_PROFILES_TO_FOLLOW_2 = "@IkySyptraa"
PROMOTION_HASHTAGS = "#faucet #ethsepolia #pharos #ethholesky #ethbase #monad #xrplevm #lineasepolia #arbitrumsepolia #megaethtestnet"


def init_web3_instances():
    """Initializes Web3 instances for each network."""
    for net_name, config in network_configs.items():
        try:
            w3_instances[net_name] = Web3(Web3.HTTPProvider(config['rpc_url']))
            if not w3_instances[net_name].is_connected():
                logger.warning(f"Failed to connect to {net_name} at {config['rpc_url']}")
            else:
                logger.info(f"Connected to {net_name} RPC.")
        except Exception as e:
            logger.error(f"Error initializing Web3 for {net_name}: {e}")

def load_user_data():
    """Loads user data from the JSON file."""
    global user_data_cache
    if os.path.exists(USER_DATA_FILE):
        with open(USER_DATA_FILE, 'r') as f:
            try:
                user_data_cache = json.load(f)
                logger.info(f"Loaded {len(user_data_cache)} user records from {USER_DATA_FILE}")
            except json.JSONDecodeError:
                logger.warning(f"Error decoding JSON from {USER_DATA_FILE}. Starting with empty data.")
                user_data_cache = {}
    else:
        logger.info(f"No {USER_DATA_FILE} found. Starting with empty user data.")
        user_data_cache = {}

def save_user_data():
    """Saves user data to the JSON file."""
    with open(USER_DATA_FILE, 'w') as f:
        json.dump(user_data_cache, f, indent=4)

# NEW: Functions for redeemed addresses persistence
def load_redeemed_addresses():
    """Loads redeemed addresses from the JSON file."""
    global redeemed_addresses_cache
    if os.path.exists(REDEEMED_ADDRESSES_FILE):
        with open(REDEEMED_ADDRESSES_FILE, 'r') as f:
            try:
                redeemed_addresses_cache = json.load(f)
                logger.info(f"Loaded {len(redeemed_addresses_cache)} redeemed address records from {REDEEMED_ADDRESSES_FILE}")
            except json.JSONDecodeError:
                logger.warning(f"Error decoding JSON from {REDEEMED_ADDRESSES_FILE}. Starting with empty redeemed addresses data.")
                redeemed_addresses_cache = {}
    else:
        logger.info(f"No {REDEEMED_ADDRESSES_FILE} found. Starting with empty redeemed addresses data.")
        redeemed_addresses_cache = {}

def save_redeemed_addresses():
    """Saves redeemed addresses to the JSON file."""
    with open(REDEEMED_ADDRESSES_FILE, 'w') as f:
        json.dump(redeemed_addresses_cache, f, indent=4)

def init_db():
    """Initializes database related data (loads from JSON files)."""
    load_user_data()
    load_redeemed_addresses() # NEW: Load redeemed addresses
    logger.info("Database initialized (loaded from JSON).")

# --- BOT HANDLER FUNCTIONS ---
# All handler functions are defined BEFORE main() to ensure proper scope
# This section has been reordered to ensure all handlers are defined before main()

async def check_maintenance_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Checks if the bot is in maintenance mode."""
    if context.application.user_data.get('maintenance_mode', False):
        user_id = update.effective_user.id
        if is_owner(user_id):
            return False
        else:
            message_source = update.callback_query or update.message
            if message_source:
                await message_source.reply_text(
                    "⚠️ The bot is currently in maintenance mode. Please try again later."
                )
            return True
    return False

async def send_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends the main menu to the user with a Reply Keyboard."""
    keyboard = [
        [KeyboardButton("Faucet 🤖")],
        [KeyboardButton("Balance 💰")],
        [KeyboardButton("Purchase Token 💳")],
        [KeyboardButton("Get More Tokens ☕")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

    if update.callback_query:
        await update.callback_query.message.reply_text(
            "Hello! Welcome to the Faucet Bot. Please choose an option:", reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            "Hello! Welcome to the Faucet Bot. Please choose an option:", reply_markup=reply_markup
        )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the /start command, records user, and checks channel membership."""
    user_id_str = str(update.effective_user.id)
    
    # Record user regardless of channel join status for broadcast list
    if user_id_str not in user_data_cache:
        user_data_cache[user_id_str] = {
            'username': update.effective_user.username,
            'full_name': update.effective_user.full_name,
            'first_interaction': update.message.date.timestamp(),
            'last_claim_times': {}, 
            'completed_tasks': {}    
        }
        save_user_data()
        logger.info(f"New user recorded: {user_id_str} ({update.effective_user.full_name})")

    # --- Channel Verification Logic ---
    # CHANNEL_ID of -100 is often a placeholder for "not set", so we check for it.
    if not CHANNEL_ID or str(CHANNEL_ID) == "-100": 
        logger.warning("CHANNEL_ID is not properly configured. Bypassing mandatory channel check.")
        await send_main_menu(update, context)
        return ConversationHandler.END

    try:
        chat_member = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=update.effective_user.id)
        if chat_member.status in ['member', 'creator', 'administrator']:
            # User is a member, proceed to main menu
            await send_main_menu(update, context)
            return ConversationHandler.END
        else:
            # User is not a member, ask them to join
            chat_info = await context.bot.get_chat(chat_id=CHANNEL_ID)
            # Ensure invite_link is generated or default to public username link
            invite_link = chat_info.invite_link if chat_info.invite_link else f"https://t.me/{chat_info.username}"
            
            keyboard = [
                [InlineKeyboardButton("Join Channel Here 🚀", url=invite_link)],
                [InlineKeyboardButton("I Have Joined ✅", callback_data='check_channel_join')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"👋 Welcome! Before you can use the bot, you must join our official channel: **{chat_info.title}**.\n\n"
                "Please click the button below to join, then click 'I Have Joined ✅'.",
                reply_markup=reply_markup,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
            return AWAITING_CHANNEL_JOIN
    except Exception as e:
        logger.error(f"Error checking channel membership for {update.effective_user.id}: {e}. Ensure bot is admin in the channel and CHANNEL_ID is correct.")
        await update.message.reply_text("An error occurred while checking channel membership. Please ensure the bot is an admin in the channel and the Channel ID is correct in `.env`. Please try again later.")
        return ConversationHandler.END 

async def check_channel_membership(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Re-checks channel membership after user clicks 'I have joined'."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id

    if not CHANNEL_ID or str(CHANNEL_ID) == "-100": 
        logger.warning("CHANNEL_ID is not properly configured. Bypassing mandatory channel check.")
        await query.edit_message_text("Channel verification skipped due to incorrect CHANNEL_ID configuration. Returning to main menu.")
        await send_main_menu(update, context)
        return ConversationHandler.END

    try:
        chat_member = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        if chat_member.status in ['member', 'creator', 'administrator']:
            await query.edit_message_text("✅ Membership verification successful!")
            await send_main_menu(update, context)
            return ConversationHandler.END
        else:
            # Still not a member, remind them
            chat_info = await context.bot.get_chat(chat_id=CHANNEL_ID)
            invite_link = chat_info.invite_link if chat_info.invite_link else f"https://t.me/{chat_info.username}"
            
            keyboard = [
                [InlineKeyboardButton("Join Channel Here 🚀", url=invite_link)],
                [InlineKeyboardButton("I Have Joined ✅", callback_data='check_channel_join')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"You have not joined the channel **{chat_info.title}**. Please join first.\n\n"
                "After joining, click 'I Have Joined ✅' again.",
                reply_markup=reply_markup,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
            return AWAITING_CHANNEL_JOIN
    except Exception as e:
        logger.error(f"Error re-checking channel membership for {user_id}: {e}. Ensure bot is admin in the channel and CHANNEL_ID is correct.")
        await query.edit_message_text("An error occurred while verifying channel membership. Please ensure the bot is an admin in the channel and the Channel ID is correct in `.env`. Please try again later.")
        return ConversationHandler.END

async def back_to_start_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handler for the 'Back to Main Menu' button."""
    query = update.callback_query
    await query.answer()
    await send_main_menu(update, context)
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    await update.message.reply_text("Operation canceled. Use /start to see options again.")
    context.user_data.clear()
    return ConversationHandler.END

async def handle_faucet_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Displays faucet claim options."""
    if await check_maintenance_mode(update, context):
        return ConversationHandler.END

    keyboard = []
    for net_name, config in network_configs.items():
        if config.get('faucet_enabled', False):
            display_name = config.get('display_name', net_name.replace('_', ' ').title())
            button_text = f"Claim {display_name}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f'claim_token_{net_name}')])
    
    keyboard.append([InlineKeyboardButton("How to use? 🆘", callback_data='how_to_use_faucet')])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_start')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "Please select which testnet token you want to claim:",
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            "Please select which testnet token you want to claim:",
            reply_markup=reply_markup
        )
    return ConversationHandler.END

async def how_to_use_faucet(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Displays faucet usage instructions."""
    query = update.callback_query
    await query.answer()
    
    instructions = (
        "Here's how to use the faucet:\n"
        "1. Select the testnet token you want to claim.\n"
        "2. Send your wallet address when prompted.\n"
        "3. Make sure you respect the 24-hour claim limit.\n"
        "4. Complete any required verification tasks."
    )
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Back to Faucet Menu", callback_data='faucet_menu_reopen')],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(instructions, reply_markup=reply_markup)
    return ConversationHandler.END

async def handle_claim_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles specific claim button presses and asks for address."""
    if await check_maintenance_mode(update, context):
        return ConversationHandler.END

    query = update.callback_query
    await query.answer()

    token_type_raw = query.data.replace('claim_token_', '')
    context.user_data['token_type_claim'] = token_type_raw
    
    config = network_configs.get(token_type_raw, {})
    display_name = config.get('display_name', token_type_raw.replace('_', ' ').title())
    currency_symbol = config.get('currency_symbol', display_name.split(' ')[0].upper())

    await query.edit_message_text(
        f"Please send your {currency_symbol} wallet address to receive the testnet tokens."
    )
    return AWAITING_CLAIM_ADDRESS

async def send_native_token(w3_instance: Web3, recipient_address: str, amount_eth: float, chain_id: int, net_name: str, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Sends native token to the given address. (FULLY CORRECTED)"""
    try:
        if not w3_instance.is_connected():
            return f"ERROR: Not connected to {net_name} network."

        gas_price = w3_instance.eth.gas_price
        gas_limit = 21000
        amount_wei = w3_instance.to_wei(amount_eth, 'ether')
        nonce = w3_instance.eth.get_transaction_count(SENDER_ADDRESS)

        transaction = {
            'from': SENDER_ADDRESS, 'to': recipient_address, 'value': amount_wei,
            'gas': gas_limit, 'gasPrice': gas_price, 'nonce': nonce, 'chainId': chain_id
        }

        signed_txn = w3_instance.eth.account.sign_transaction(transaction, private_key=SENDER_PRIVATE_KEY)
        
        tx_hash = w3_instance.eth.send_raw_transaction(signed_txn.raw_transaction)
        tx_hash_hex = w3_instance.to_hex(tx_hash)

        config = network_configs.get(net_name, {})
        display_name = config.get('display_name', net_name.replace('_', ' ').title())
        currency_symbol = config.get('currency_symbol', 'TOKEN')
        
        notification_message = (
            f"💸 **Outgoing Transaction Sent!**\n"
            f"Network: **{display_name}**\n"
            f"Amount: `{amount_eth:.4f} {currency_symbol}`\n"
            f"To: `{recipient_address}`\n"
            f"Tx Hash: [`{tx_hash_hex}`]({config.get('explorer_url', '')}/tx/{tx_hash_hex})"
        )
        if ADMIN_NOTIF_ID:
            await context.bot.send_message(
                chat_id=ADMIN_NOTIF_ID, text=notification_message,
                parse_mode='Markdown', disable_web_page_preview=True
            )
            logger.info(f"Sent outgoing transaction notification to admin: {tx_hash_hex}")

        return tx_hash_hex
    except Exception as e:
        logger.error(f"Error sending native token on {net_name}: {e}")
        return f"ERROR: {e}"

async def handle_claim_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the wallet address for claiming."""
    if await check_maintenance_mode(update, context):
        return ConversationHandler.END

    user_address = update.message.text
    token_type_claim = context.user_data.get('token_type_claim')
    user_id_str = str(update.effective_user.id)

    if not token_type_claim:
        await update.message.reply_text("Error: Token type not specified. Please start over.")
        return ConversationHandler.END

    if not Web3.is_address(user_address):
        await update.message.reply_text("That doesn't look like a valid wallet address. Please send a correct one.")
        return AWAITING_CLAIM_ADDRESS

    current_time = update.message.date.timestamp()
    user_data_cache.setdefault(user_id_str, {})
    user_data_cache[user_id_str].setdefault('last_claim_times', {})

    last_claim_time_for_token = user_data_cache[user_id_str]['last_claim_times'].get(token_type_claim, 0)
    
    if (current_time - last_claim_time_for_token) < 86400: # 24 hours
        remaining_time = 86400 - (current_time - last_claim_time_for_token)
        hours, remainder = divmod(remaining_time, 3600)
        minutes, _ = divmod(remainder, 60)
        # Fix: Explicitly set parse_mode=None for this plain text message
        await update.message.reply_text(
            f"You can only claim this token once every 24 hours. Please wait {int(hours)} hours and {int(minutes)} minutes.",
            parse_mode=None
        )
        context.user_data.clear()
        return ConversationHandler.END

    w3_instance = w3_instances.get(token_type_claim)
    config = network_configs.get(token_type_claim)

    if not w3_instance or not config or not w3_instance.is_connected():
        await update.message.reply_text(f"Faucet for this token is currently unavailable.")
        context.user_data.clear()
        return ConversationHandler.END

    amount_to_send = config.get('faucet_amount')
    if amount_to_send is None:
        display_name = config.get('display_name', token_type_claim.replace('_', ' ').title())
        await update.message.reply_text(
            f"Configuration error: Faucet amount not specified for {display_name}. "
            f"Please contact the bot admin."
        )
        logger.error(f"FATAL ERROR: 'faucet_amount' not defined for network '{token_type_claim}' in config.py")
        context.user_data.clear()
        return ConversationHandler.END

    context.user_data['claim_address'] = user_address

    # --- START OF MODIFICATION: REMOVING LABUBU BOT TASK VERIFICATION ---
    
    display_name = config.get('display_name', token_type_claim.replace('_', ' ').title())
    currency_symbol = config.get('currency_symbol', 'TOKEN')
    chain_id = config.get('chain_id')

    await update.message.reply_text(f"Processing your request to send `{amount_to_send}` {currency_symbol} to `{user_address}`...")

    tx_hash = await send_native_token(w3_instance, user_address, amount_to_send, chain_id, token_type_claim, context)

    if "ERROR:" in tx_hash:
        await update.message.reply_text(f"Failed to send token. Reason: {tx_hash}")
    else:
        explorer_url = config.get('explorer_url')
        full_tx_url = f"{explorer_url}/tx/{tx_hash}"
        await update.message.reply_text(
            f"✅ Success! Token sent.\n**Tx Hash**: [`{tx_hash}`]({full_tx_url})",
            parse_mode='Markdown', disable_web_page_preview=True
        )
        user_data_cache[user_id_str]['last_claim_times'][token_type_claim] = update.message.date.timestamp()
        save_user_data()

    context.user_data.clear()
    return ConversationHandler.END # End the conversation after sending token
    
    # --- END OF MODIFICATION ---

async def handle_forwarded_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    This function is primarily kept for legacy or potential future use,
    as the Labubu task in 'Get More Tokens' now uses screenshots.
    """
    if await check_maintenance_mode(update, context):
        return ConversationHandler.END

    await update.message.reply_text("This specific verification flow is not active for the current task. Please follow the instructions provided earlier in the 'Get More Tokens' process.")
    context.user_data.clear()
    return ConversationHandler.END

async def purchase_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Displays token purchase options."""
    if await check_maintenance_mode(update, context):
        return ConversationHandler.END

    query = update.callback_query
    if query:
        await query.answer()

    keyboard = []
    for net_name, config in network_configs.items():
        if config.get('purchase_enabled', False):
            display_name = config.get('display_name', net_name.replace('_', ' ').title())
            keyboard.append([InlineKeyboardButton(f"Buy {display_name}", callback_data=f'buy_token_{net_name}')])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_start')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = "Which token would you like to purchase?"

    if query:
        await query.edit_message_text(text=message_text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text=message_text, reply_markup=reply_markup)
    
    return SELECTING_PURCHASE_TOKEN

async def handle_purchase_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles token selection for purchase."""
    if await check_maintenance_mode(update.callback_query, context):
        return ConversationHandler.END

    query = update.callback_query
    await query.answer()

    token_type_raw = query.data.replace('buy_token_', '')
    context.user_data['token_type_purchase'] = token_type_raw

    config = network_configs.get(token_type_raw, {})
    display_name = config.get('display_name', token_type_raw.replace('_', ' ').title())
    currency_symbol = config.get('currency_symbol', 'TOKEN')

    await query.edit_message_text(
        f"You selected **{display_name}**. How much {currency_symbol} would you like to purchase?",
        parse_mode='Markdown'
    )
    return AWAITING_PURCHASE_AMOUNT

async def handle_purchase_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles purchase amount and gives instructions."""
    if await check_maintenance_mode(update, context):
        return ConversationHandler.END

    try:
        purchase_amount = float(update.message.text)
        if purchase_amount <= 0:
            await update.message.reply_text("Please enter a positive amount.")
            return AWAITING_PURCHASE_AMOUNT
    except ValueError:
        await update.message.reply_text("Invalid amount. Please enter a numeric value.")
        return AWAITING_PURCHASE_AMOUNT

    token_type = context.user_data.get('token_type_purchase')
    config = network_configs.get(token_type, {})
    w3 = w3_instances.get(token_type)
    display_name = config.get('display_name', 'Token')
    symbol = config.get('balance_symbol', config.get('currency_symbol', 'TOKEN'))

    if not w3 or not w3.is_connected():
        await update.message.reply_text(f"🚫 Apologies! Connection to {display_name} network is unavailable.")
    else:
        bot_balance_wei = w3.eth.get_balance(SENDER_ADDRESS)
        bot_balance_eth = w3.from_wei(bot_balance_wei, 'ether')

        if bot_balance_eth < purchase_amount:
            await update.message.reply_text(f"🚫 Apologies! The bot does not have enough **{display_name}** to fulfill your request.", parse_mode='Markdown')
        else:
            await update.message.reply_text(
                f"✅ We can fulfill your request for `{purchase_amount:.4f} {symbol}` of **{display_name}**!\n\n"
                f"Please contact the admin to proceed: @{OWNER_TELEGRAM_USERNAME}",
                parse_mode='Markdown'
            )
            if ADMIN_NOTIF_ID:
                await context.bot.send_message(
                    chat_id=ADMIN_NOTIF_ID,
                    text=f"❗ **NEW PURCHASE REQUEST!**\nUser: {update.effective_user.full_name} (`{update.effective_user.id}`)\nWants to buy: `{purchase_amount:.4f} {symbol}` of **{display_name}**.",
                    parse_mode='Markdown'
                )
    
    context.user_data.clear()
    return ConversationHandler.END

# --- GET MORE TOKENS (TASK-BASED) FLOW ---
async def handle_get_more_tokens_button_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Entry point for Get More Tokens task. Displays reward token selection."""
    if await check_maintenance_mode(update, context):
        return ConversationHandler.END

    user_id_str = str(update.effective_user.id)
    # NEW: Set a flag to indicate if user has completed the main task before.
    # This flag will be used later to inform them about reward eligibility,
    # but it will NOT prevent them from entering the flow.
    context.user_data['get_more_tokens_reentry'] = user_data_cache.get(user_id_str, {}).get('completed_tasks', {}).get('get_more_tokens_main_task', False)

    keyboard = []
    for net_name, config in network_configs.items():
        if config.get('faucet_enabled', False): # Only tokens that can be faucet-ed can be rewards
            display_name = config.get('display_name', net_name.replace('_', ' ').title())
            currency_symbol = config.get('currency_symbol', display_name.split(' ')[0].upper())
            task_reward_amount = config.get('task_reward_amount', config.get('faucet_amount'))
            if task_reward_amount is None:
                task_reward_amount = 0

            keyboard.append([InlineKeyboardButton(f"Get {display_name} ({task_reward_amount} {currency_symbol})", callback_data=f'select_reward_token_{net_name}')])
    
    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_start')])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "Great! Which testnet token would you like to receive as a reward for completing tasks?",
        reply_markup=reply_markup
    )
    return SELECTING_REWARD_TOKEN


async def handle_reward_token_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles reward token selection and asks for wallet address."""
    query = update.callback_query
    await query.answer()

    if await check_maintenance_mode(query, context):
        return ConversationHandler.END
    
    selected_token_raw = query.data.replace('select_reward_token_', '')
    context.user_data['selected_reward_token'] = selected_token_raw

    reward_config = network_configs.get(selected_token_raw)
    if not reward_config:
        await query.edit_message_text("Error: Selected token not found. Please try again or contact admin.")
        context.user_data.clear()
        return ConversationHandler.END
    
    task_reward_amount = reward_config.get('task_reward_amount', reward_config.get('faucet_amount'))
    if task_reward_amount is None:
        await query.edit_message_text(f"Configuration error: Task reward amount not specified for {reward_config.get('display_name')}. Please contact admin.")
        logger.error(f"FATAL ERROR: 'task_reward_amount' or 'faucet_amount' not defined for network '{selected_token_raw}' in config.py")
        context.user_data.clear()
        return ConversationHandler.END
    context.user_data['task_reward_amount'] = task_reward_amount
    
    reward_currency_symbol = reward_config.get('currency_symbol', selected_token_raw.upper())

    await query.edit_message_text(
        f"You've chosen {reward_config.get('display_name')}. "
        f"Please send your {reward_currency_symbol} wallet address to receive the reward."
    )
    return AWAITING_REWARD_ADDRESS

async def handle_reward_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the reward wallet address and presents the choice of tasks."""
    user_address = update.message.text
    user_id_str = str(update.effective_user.id)

    selected_token_raw = context.user_data.get('selected_reward_token')
    reward_config = network_configs.get(selected_token_raw)
    reward_currency_symbol = reward_config.get('currency_symbol', selected_token_raw.upper())

    if not Web3.is_address(user_address):
        await update.message.reply_text(f"That doesn't look like a valid {reward_currency_symbol} wallet address. Please send a correct one.")
        return AWAITING_REWARD_ADDRESS

    # NEW CHECK: Prevent using an address already redeemed by another Telegram account
    # Also prevents current user from using an address they already successfully redeemed with
    if user_address in redeemed_addresses_cache:
        if redeemed_addresses_cache[user_address] != user_id_str:
            await update.message.reply_text(
                "🚫 This wallet address has already been used to claim rewards by another Telegram account. "
                "Each address can only be used by one Telegram account for this campaign. Please use a different address."
            )
            context.user_data.clear()
            return ConversationHandler.END
        else: # Address is in cache and belongs to current user. This means they are re-entering flow with their own previous address.
            logger.info(f"User {user_id_str} re-entered 'Get More Tokens' with their own previously redeemed address {user_address}. Allowed, but no further rewards.")


    context.user_data['reward_recipient_address'] = user_address

    # NEW: Inform user if this is a re-entry and no rewards will be given
    if context.user_data.get('get_more_tokens_reentry'):
        await update.message.reply_text(
            "⚠️ You have completed the 'Get More Tokens' task before. "
            "You can go through the tasks, but **no further rewards will be distributed to this address for this campaign**.",
            parse_mode='Markdown'
        )
        logger.info(f"User {user_id_str} re-entered 'Get More Tokens' flow. Notifying no further rewards.")

    # Present the choice between Twitter tasks and LabubuAI tasks
    keyboard = [
        [InlineKeyboardButton("Complete Twitter Tasks 🐦", callback_data='select_twitter_tasks')],
        [InlineKeyboardButton("Complete LabubuAI Tasks 🤖", callback_data='select_labubu_tasks')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "Great! Now, please select which task you would like to complete to earn your reward:",
        reply_markup=reply_markup
    )
    return SELECTING_GET_MORE_TOKENS_TASK_TYPE


async def select_get_more_tokens_task_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Presents options to choose between Twitter tasks or LabubuAI tasks."""
    query = update.callback_query
    await query.answer()

    if query.data == 'select_twitter_tasks':
        # Start Twitter task flow
        keyboard = [
            [InlineKeyboardButton(f"Follow {TWITTER_PROFILES_TO_FOLLOW_1} 🐦", url=f"https://x.com/{TWITTER_PROFILES_TO_FOLLOW_1.lstrip('@')}")],
            [InlineKeyboardButton("I have followed ✅", callback_data='followed_petrukstar_check')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            f"Great! Now for the first task:\n\n"
            f"1. Follow our Twitter (X) account: **{TWITTER_PROFILES_TO_FOLLOW_1}**\n"
            f"2. Click 'I have followed ✅' when done."
            , reply_markup=reply_markup, parse_mode='Markdown', disable_web_page_preview=True
        )
        return AWAITING_TWITTER_FOLLOW_1_CONFIRM
    
    elif query.data == 'select_labubu_tasks':
        # Start LabubuAI screenshot task flow
        user_id = update.effective_user.id
        labubu_bot_link = f"https://t.me/LabubuAi_AirdropBot?start={user_id}"

        keyboard = [
            [InlineKeyboardButton("Go to LabubuAi Airdrop Bot 🚀", url=labubu_bot_link)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "Please go to the LabubuAi Airdrop Bot using the button below and complete any tasks mentioned there.\n\n"
            "Once you have completed them, **send a screenshot of your completion proof directly to me (this bot)**. "
            "Your submission will be manually reviewed by the admin."
            , reply_markup=reply_markup, parse_mode='Markdown', disable_web_page_preview=True
        )
        return AWAITING_LABUBU_SCREENSHOT

# Handler functions for Twitter tasks (moved here to ensure they are defined before being used in main())
async def handle_twitter_follow_1_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles confirmation of first Twitter follow and asks for username."""
    query = update.callback_query
    await query.answer()

    if await check_maintenance_mode(query, context):
        return ConversationHandler.END

    await query.edit_message_text(
        "Thank you for following! Please send your Twitter (X) username (e.g., `@yourusername`) "
        "or profile link (e.g., `https://x.com/yourusername`) for verification."
    )
    return AWAITING_TWITTER_USERNAME_1

async def handle_twitter_username_1(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receives the first Twitter username and presents the second Twitter task."""
    twitter_username_or_link = update.message.text
    context.user_data['twitter_username_1'] = twitter_username_or_link

    keyboard = [
        [InlineKeyboardButton(f"Follow {TWITTER_PROFILES_TO_FOLLOW_2} 🐦", url=f"https://x.com/{TWITTER_PROFILES_TO_FOLLOW_2.lstrip('@')}")],
        [InlineKeyboardButton("I have followed ✅", callback_data='followed_ikysyptraa_check')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"Got it! Now for the second task:\n\n"
        f"1. Follow our partner's Twitter (X) account: **{TWITTER_PROFILES_TO_FOLLOW_2}**\n"
        f"2. Click 'I have followed ✅' when done."
        , reply_markup=reply_markup, parse_mode='Markdown', disable_web_page_preview=True
    )
    return AWAITING_TWITTER_FOLLOW_2_CONFIRM

async def handle_twitter_follow_2_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles confirmation of second Twitter follow and presents the third Twitter task (post)."""
    query = update.callback_query
    await query.answer()

    if await check_maintenance_mode(query, context):
        return ConversationHandler.END

    promo_text = (
        f"Get free testnet tokens from this awesome faucet bot!\n\n"
        f"Click here: t.me/{context.bot.username}\n\n"
        f"Check out the faucet for {PROMOTION_HASHTAGS}"
    )
    
    await query.edit_message_text(
        f"Almost there! Now for the final task:\n\n"
        f"1. Create a post on Twitter (X) with the following content:\n"
        f"```\n{promo_text}\n```\n\n"
        f"2. Make sure to include the provided hashtags.\n"
        f"3. Once posted, send me the **link to your Twitter (X) post** (e.g., `https://x.com/yourusername/status/1234567890`)."
        , parse_mode='Markdown', disable_web_page_preview=True
    )
    return AWAITING_TWITTER_POST_LINK

async def handle_labubu_screenshot_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the screenshot submission for LabubuAI task and notifies admin."""
    if await check_maintenance_mode(update, context):
        return ConversationHandler.END

    message = update.message
    if not message.photo:
        await message.reply_text("That doesn't look like a screenshot. Please send an image of your LabubuAI task completion.")
        return AWAITING_LABUBU_SCREENSHOT

    user_id = update.effective_user.id
    user_full_name = update.effective_user.full_name
    user_username = update.effective_user.username
    screenshot_file_id = message.photo[-1].file_id # Get the largest photo size

    reward_recipient_address = context.user_data.get('reward_recipient_address')
    selected_reward_token = context.user_data.get('selected_reward_token')
    reward_amount = context.user_data.get('task_reward_amount')

    # Get the reentry status to include in admin notification
    get_more_tokens_reentry = context.user_data.get('get_more_tokens_reentry', False)

    if not reward_recipient_address or not selected_reward_token or reward_amount is None:
        await message.reply_text("Error: Task data missing. Please restart the 'Get More Tokens' process.")
        context.user_data.clear()
        return ConversationHandler.END
    
    await message.reply_text("Thank you for submitting your screenshot! Your task completion will now be reviewed by the admin.")

    # Escape strings for Markdown in admin notification
    escaped_user_full_name = escape_markdown(user_full_name, version=2)
    escaped_user_username = escape_markdown(user_username or 'N/A', version=2) 
    escaped_reward_address = escape_markdown(reward_recipient_address, version=2)

    notification_message = (
        f"✅ **NEW LABUBUAI TASK VERIFICATION REQUEST (SCREENSHOT)!**\n\n"
        f"**User:** {escaped_user_full_name} (`{user_id}`)\n"
        f"**Username:** @{escaped_user_username}\n"
        f"**Reward Address:** `{escaped_reward_address}` (`{selected_reward_token.upper()}`)\n"
        f"**Reward:** {reward_amount} `{selected_reward_token.upper()}`\n"
        f"**Status:** {'Re-entry (No Reward)' if get_more_tokens_reentry else 'First Time (Reward Eligible)'}\n\n" # NEW: Status line
        f"Please review the screenshot and decide."
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Approve", callback_data=f'admin_approve_task_{user_id}'),
            InlineKeyboardButton("❌ Reject", callback_data=f'admin_reject_task_{user_id}')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if ADMIN_NOTIF_ID:
        try:
            # Send photo to admin
            admin_message = await context.bot.send_photo(
                chat_id=ADMIN_NOTIF_ID,
                photo=screenshot_file_id,
                caption=notification_message, # Use caption for text with photo
                reply_markup=reply_markup,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
            # Store data in pending_task_verifications for admin approval
            pending_task_verifications[user_id] = {
                'reward_amount': reward_amount,
                'reward_token': selected_reward_token,
                'reward_recipient_address': reward_recipient_address,
                'task_type': 'labubu_screenshot', # Mark task type for admin verification
                'admin_msg_id': admin_message.message_id,
                'user_full_name': user_full_name,
                'user_username': user_username,
                'screenshot_file_id': screenshot_file_id, # Store file ID for potential re-display
                'get_more_tokens_reentry': get_more_tokens_reentry # Pass reentry status to admin verification
            }
            logger.info(f"Sent LabubuAI screenshot task verification request for user {user_id} to admin.")
        except Exception as e:
            logger.error(f"Failed to send admin notification for LabubuAI screenshot for user {user_id}: {e}")
            await message.reply_text("Failed to send your screenshot for verification. Please try again later or contact support.")
            
    context.user_data.clear() # Clear user_data after submission
    return ConversationHandler.END # End conversation after submission


# --- EXISTING HANDLERS MODIFIED ---

async def handle_twitter_post_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Receives the Twitter post link and sends it to admin for verification.
    This now marks the end of the Twitter task branch.
    """
    user_post_link = update.message.text
    user_id = update.effective_user.id
    user_full_name = update.effective_user.full_name
    user_username = update.effective_user.username

    reward_recipient_address = context.user_data.get('reward_recipient_address')
    selected_reward_token = context.user_data.get('selected_reward_token')
    reward_amount = context.user_data.get('task_reward_amount')

    # Get the reentry status to include in admin notification
    get_more_tokens_reentry = context.user_data.get('get_more_tokens_reentry', False)

    if not reward_recipient_address or not selected_reward_token or reward_amount is None:
        await update.message.reply_text("Error: Task data missing. Please start the task again.")
        context.user_data.clear()
        return ConversationHandler.END

    if not (user_post_link.startswith("https://x.com/") or user_post_link.startswith("https://twitter.com/")) or "/status/" not in user_post_link:
        await update.message.reply_text(
            "That doesn't look like a valid Twitter (X) post link. Please send a direct link to your post (e.g., `https://x.com/username/status/12345`)."
        )
        return AWAITING_TWITTER_POST_LINK

    await update.message.reply_text("We are checking your assignment, please wait a few moments.")

    # Escape these strings to prevent Markdown parsing issues
    # Handle potential None for username
    escaped_user_full_name = escape_markdown(user_full_name, version=2)
    escaped_user_username = escape_markdown(user_username or 'N/A', version=2) 
    escaped_twitter_username = escape_markdown(context.user_data.get('twitter_username_1', 'N/A'), version=2)
    escaped_user_post_link = escape_markdown(user_post_link, version=2)

    notification_message = (
        f"✅ **NEW TWITTER TASK VERIFICATION REQUEST!**\n\n"
        f"**User:** {escaped_user_full_name} (`{user_id}`)\n"
        f"**Username:** @{escaped_user_username}\n"
        f"**Reward Address:** `{reward_recipient_address}` (`{selected_reward_token.upper()}`)\n"
        f"**Twitter Username (Self-Confirmed):** `{escaped_twitter_username}`\n"
        f"**Submitted Twitter (X) Post Link:** [`{escaped_user_post_link}`]({escaped_user_post_link})\n"
        f"**Reward:** {reward_amount} `{selected_reward_token.upper()}`\n"
        f"**Status:** {'Re-entry (No Reward)' if get_more_tokens_reentry else 'First Time (Reward Eligible)'}\n\n" # NEW: Status line
        f"Please review and decide."
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Approve", callback_data=f'admin_approve_task_{user_id}'),
            InlineKeyboardButton("❌ Reject", callback_data=f'admin_reject_task_{user_id}')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if ADMIN_NOTIF_ID:
        try:
            admin_message = await context.bot.send_message(
                chat_id=ADMIN_NOTIF_ID,
                text=notification_message,
                reply_markup=reply_markup,
                parse_mode='Markdown',
                disable_web_page_preview=False # Set to False to preview the Twitter link
            )
            pending_task_verifications[user_id] = {
                'reward_amount': reward_amount,
                'reward_token': selected_reward_token,
                'reward_recipient_address': reward_recipient_address,
                'twitter_username_1': context.user_data.get('twitter_username_1', 'N/A'),
                'user_post_link': user_post_link,
                'task_type': 'twitter_tasks', # Mark task type
                'admin_msg_id': admin_message.message_id,
                'user_full_name': user_full_name,
                'user_username': user_username,
                'get_more_tokens_reentry': get_more_tokens_reentry # Pass reentry status to admin verification
            }
            logger.info(f"Sent Twitter task verification request for user {user_id} to admin.")
            await update.message.reply_text("Verification completed. Your task submission has been sent to the admin for review. You will be notified of the outcome shortly!")
        except Exception as e:
            logger.error(f"Failed to send admin notification for Twitter task for user {user_id}: {e}")
            await update.message.reply_text("Failed to send verification request to admin. Please try again later or contact support.")
            
    context.user_data.clear()
    return ConversationHandler.END


# --- Other unchanged helper/command handlers ---
def is_owner(user_id: int) -> bool:
    """Checks if the user_id is the bot owner."""
    return user_id == OWNER_TELEGRAM_ID

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the bot's wallet balance with custom formatting."""
    message_text = "💰 Current Bot Balances 💰\n\n"
    for net_name, config in network_configs.items():
        w3 = w3_instances.get(net_name)
        if w3 and w3.is_connected():
            try:
                balance_wei = w3.eth.get_balance(SENDER_ADDRESS)
                balance_eth = w3.from_wei(balance_wei, 'ether')
                
                label = config.get('balance_label', net_name.replace('_', ' ').title().replace(' Testnet', ''))
                symbol = config.get('balance_symbol', config.get('currency_symbol', 'ERR'))
                
                message_text += f"{label}: {balance_eth:.4f} {symbol}\n"
            except Exception as e:
                label = config.get('balance_label', net_name.replace('_', ' ').title())
                message_text += f"{label}: Error fetching balance\n"
                logger.error(f"Error fetching balance for {net_name}: {e}")
        else:
            label = config.get('balance_label', net_name.replace('_', ' ').title())
            message_text += f"{label}: Not connected to RPC\n"
    
    if update.callback_query:
        await update.callback_query.message.reply_text(message_text)
    else:
        await update.message.reply_text(message_text)

async def send_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manually sends tokens from the bot's wallet (owner only)."""
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("You are not authorized to use this command.")
        return

    try:
        args = update.message.text.split()
        if len(args) != 4:
            await update.message.reply_text("Usage: `/send <amount> <token_name> <recipient_address>`\nExample: `/send 0.5 monad 0xabc...`", parse_mode='Markdown')
            return

        amount_str = args[1]
        token_name_raw = args[2]
        recipient_address = args[3]
        amount = float(amount_str)

    except ValueError:
        await update.message.reply_text("Invalid amount. Usage: `/send <amount> <token_name> <recipient_address>`", parse_mode='Markdown')
        return

    token_type = None
    for key, config in network_configs.items():
        if token_name_raw.lower() in [key.lower(), config.get('currency_symbol', '').lower(), config.get('display_name', '').lower().replace(' ', '')]:
            token_type = key
            break

    if not token_type or not Web3.is_address(recipient_address):
        await update.message.reply_text("Invalid arguments. Check token name and address.")
        return

    w3 = w3_instances.get(token_type)
    config = network_configs.get(token_type)
    
    if not w3 or not config:
        await update.message.reply_text(f"Network configuration for '{token_name_raw}' not found.")
        return
        
    chain_id = config.get("chain_id")
    symbol = config.get('balance_symbol', config.get('currency_symbol', 'TOKEN'))

    await update.message.reply_text(f"Sending `{amount}` {symbol} to `{recipient_address}`...")
    tx_hash = await send_native_token(w3, recipient_address, amount, chain_id, token_type, context)

    if "ERROR:" in tx_hash:
        await update.message.reply_text(f"Failed to send token. Reason: {tx_hash}")
    else:
        explorer_url = config.get('explorer_url')
        full_tx_url = f"{explorer_url}/tx/{tx_hash}"
        await update.message.reply_text(
            f"✅ Success! Token sent.\n**Tx Hash**: [`{tx_hash}`]({full_tx_url})",
            parse_mode='Markdown', disable_web_page_preview=True
        )

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcasts a message to all known users (owner only)."""
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("You are not authorized to use this command.")
        return

    try:
        message_to_broadcast = " ".join(context.args)
        if not message_to_broadcast:
            await update.message.reply_text("Usage: `/broadcast <your_message>`", parse_mode='Markdown')
            return

        sent_count = 0
        failed_count = 0

        for user_id_str in user_data_cache.keys():
            try:
                user_id = int(user_id_str)
                await context.bot.send_message(chat_id=user_id, text=message_to_broadcast)
                sent_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.warning(f"Failed to send broadcast to user {user_id_str}: {e}")
                failed_count += 1

        await update.message.reply_text(
            f"Broadcast message sent to {sent_count} users. Failed to send to {failed_count} users."
        )
        logger.info(f"Broadcast completed. Sent: {sent_count}, Failed: {failed_count}")

    except Exception as e:
        logger.error(f"Error in broadcast command: {e}")
        await update.message.reply_text("An error occurred while broadcasting the message.")

async def stat_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays bot statistics (owner only)."""
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("You are not authorized to use this command.")
        return

    total_users = len(user_data_cache)
    total_redeemed_addresses = len(redeemed_addresses_cache)

    message = (
        f"📊 **Bot Statistics**\n"
        f"Total Unique Users: {total_users}\n"
        f"Total Redeemed Addresses (Get More Tokens): {total_redeemed_addresses}\n"
    )
    await update.message.reply_text(message, parse_mode='Markdown')

async def maintenance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Toggles maintenance mode for the bot (owner only)."""
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("You are not authorized to use this command.")
        return

    try:
        action = context.args[0].lower()
        if action == "on":
            context.application.user_data['maintenance_mode'] = True
            await update.message.reply_text("Maintenance mode is now **ON**.", parse_mode='Markdown')
            logger.info("Maintenance mode turned ON.")
        elif action == "off":
            context.application.user_data['maintenance_mode'] = False
            await update.message.reply_text("Maintenance mode is now **OFF**.", parse_mode='Markdown')
            logger.info("Maintenance mode turned OFF.")
        else:
            await update.message.reply_text("Usage: `/maintenance <on/off>`", parse_mode='Markdown')
    except (IndexError, AttributeError):
        await update.message.reply_text("Usage: `/maintenance <on/off>`", parse_mode='Markdown')

async def handle_admin_verification(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles admin's approval or rejection of a task submission."""
    query = update.callback_query
    await query.answer()

    if not is_owner(query.from_user.id):
        await query.edit_message_text("You are not authorized to perform this action.")
        return

    parts = query.data.split('_')
    action = parts[1]
    user_id_str = parts[3]

    user_id = int(user_id_str)

    task_data = pending_task_verifications.pop(user_id, None)
    if not task_data:
        await query.edit_message_text(f"Task request for user {user_id} not found or already processed.")
        logger.warning(f"Admin tried to process non-existent task for user {user_id}. Callback: {query.data}")
        return

    reward_amount = task_data['reward_amount']
    reward_token = task_data['reward_token']
    reward_recipient_address = task_data['reward_recipient_address']
    task_type = task_data.get('task_type', 'unknown')
    get_more_tokens_reentry = task_data.get('get_more_tokens_reentry', False) # Get reentry status from task_data

    # NEW: Prepare base status message for admin (either edit caption or text)
    base_status_message_admin = (
        f"User: {escape_markdown(task_data.get('user_full_name', f'User {user_id}'), version=2)} (`{user_id}`) (@{escape_markdown(task_data.get('user_username', 'N/A'), version=2)})\n"
        f"Reward: {reward_amount} {reward_token.upper()}\n"
        f"Address: `{reward_recipient_address}`\n"
        f"Task Type: {task_type.replace('_', ' ').title()}\n"
    )
    if task_type == 'twitter_tasks':
        base_status_message_admin += (
            f"Twitter: {escape_markdown(task_data.get('twitter_username_1', 'N/A'), version=2)}\n"
            f"Post: {escape_markdown(task_data.get('user_post_link', 'N/A'), version=2)}\n"
        )
    elif task_type == 'labubu_screenshot':
        base_status_message_admin += f"Screenshot ID: `{task_data.get('screenshot_file_id', 'N/A')}`\n"


    admin_notification_message_id = task_data['admin_msg_id']
    user_chat_id = user_id
    
    reward_config = network_configs.get(reward_token)
    reward_display_name = reward_config.get('display_name', reward_token.replace('_', ' ').title()) if reward_config else reward_token
    reward_currency_symbol = reward_config.get('currency_symbol', 'TOKEN') if reward_config else 'TOKEN'
    
    status_message_admin = ""
    status_message_user = ""

    if action == "approve":
        if get_more_tokens_reentry: # If they've completed it before, no actual token send
            status_message_admin = f"✅ **Approved (No Token Sent - Re-entry)**\n{base_status_message_admin}"
            status_message_user = (
                f"🎉 Congratulations! Your task submission for the Get More Tokens campaign has been **APPROVED**.\n\n"
                f"However, as you have completed this campaign before, **no further rewards will be distributed**."
            )
            logger.info(f"Admin approved re-entry task for user {user_id}. No token sent.")
        else: # First time completion, send token
            w3_instance = w3_instances.get(reward_token)
            chain_id = reward_config.get('chain_id') if reward_config else None
            
            if not w3_instance or not chain_id or not w3_instance.is_connected():
                status_message_admin = (
                    f"❌ Approval failed (RPC/Config Error)!\n{base_status_message_admin}"
                    f"Reason: Reward token '{reward_token}' not configured or RPC not connected.\n"
                )
                status_message_user = (
                    f"🚫 Unfortunately, your task submission was approved, but there was an issue sending your {reward_display_name} reward. "
                    f"Please contact the bot admin: @{OWNER_TELEGRAM_USERNAME}"
                )
                logger.error(f"Failed to send reward for user {user_id}: Token '{reward_token}' RPC issue.")
            else:
                tx_hash = await send_native_token(w3_instance, reward_recipient_address, reward_amount, chain_id, reward_token, context)

                if "ERROR:" in tx_hash:
                    status_message_admin = (
                        f"❗ **Approved, but failed to send token!**\n{base_status_message_admin}"
                        f"Reason: {tx_hash}"
                    )
                    status_message_user = (
                        f"🚫 Unfortunately, your task submission was approved, but there was an issue sending your {reward_display_name} reward. "
                        f"Reason: {tx_hash}. Please contact the bot admin: @{OWNER_TELEGRAM_USERNAME}"
                    )
                    logger.error(f"Failed to send reward to {reward_recipient_address} for user {user_id}: {tx_hash}")
                else:
                    explorer_url = reward_config.get('explorer_url', '')
                    full_tx_url = f"{explorer_url}/tx/{tx_hash}"
                    
                    status_message_admin = (
                        f"✅ **Approved & Token Sent!**\n{base_status_message_admin}"
                        f"Tx Hash: [`{tx_hash}`]({full_tx_url})"
                    )
                    status_message_user = (
                        f"🎉 Congratulations! Your task submission for the Get More Tokens campaign has been **APPROVED** and your reward has been sent!\n\n"
                        f"You received `{reward_amount} {reward_currency_symbol}` at `{reward_recipient_address}`.\n"
                        f"**Tx Hash**: [`{tx_hash}`]({full_tx_url})"
                    )
                    logger.info(f"Admin approved and sent reward to user {user_id}. Tx: {tx_hash}")

                    user_data_cache.setdefault(str(user_id), {}).setdefault('completed_tasks', {})
                    user_data_cache[str(user_id)]['completed_tasks']['get_more_tokens_main_task'] = True # Mark as completed
                    save_user_data()

                    # NEW: Add address to redeemed_addresses_cache on first successful completion
                    redeemed_addresses_cache[reward_recipient_address] = user_id_str
                    save_redeemed_addresses()

    elif action == "reject":
        status_message_admin = (
            f"❌ **Rejected task for user {user_id}**\n{base_status_message_admin}"
        )
        if task_type == 'twitter_tasks':
            status_message_user = (
                f"🚫 Unfortunately, your task submission for the Get More Tokens campaign has been **REJECTED**.\n\n"
                f"Please ensure you followed all instructions correctly (followed {TWITTER_PROFILES_TO_FOLLOW_1} and {TWITTER_PROFILES_TO_FOLLOW_2}, and provided a valid promotion post link). "
                f"You can try again by re-selecting 'Get More Tokens ☕' from the menu."
            )
            logger.info(f"Admin rejected Twitter task for user {user_id}. Link: {task_data.get('user_post_link', 'N/A')}.")
        elif task_type == 'labubu_screenshot':
            status_message_user = (
                f"🚫 Unfortunately, your LabubuAI task submission for the Get More Tokens campaign has been **REJECTED**.\n\n"
                f"Please ensure you followed all instructions and provided a clear screenshot of completion. "
                f"You can try again by re-selecting 'Get More Tokens ☕' from the menu."
            )
            logger.info(f"Admin rejected LabubuAI screenshot task for user {user_id}. Screenshot ID: {task_data.get('screenshot_file_id', 'N/A')}.")
        else: # Fallback for unknown task type
            status_message_user = "🚫 Your task submission has been **REJECTED**. Please try again."

    try:
        # If it's a screenshot, edit the photo caption; otherwise, edit text message
        if task_data.get('task_type') == 'labubu_screenshot':
            await context.bot.edit_message_caption(
                chat_id=ADMIN_NOTIF_ID,
                message_id=admin_notification_message_id,
                caption=f"{status_message_admin}\n\nProcessed by @{query.from_user.username or 'Admin'}",
                reply_markup=None, # Remove buttons after processing
                parse_mode='Markdown'
            )
        else: # For Twitter tasks
            await context.bot.edit_message_text(
                chat_id=ADMIN_NOTIF_ID,
                message_id=admin_notification_message_id,
                text=f"{status_message_admin}\n\nProcessed by @{query.from_user.username or 'Admin'}",
                reply_markup=None, # Remove buttons after processing
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
    except Exception as e:
        logger.error(f"Failed to edit admin notification message {admin_notification_message_id}: {e}")

    try:
        await context.bot.send_message(
            chat_id=user_chat_id,
            text=status_message_user,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Failed to send task verification result to user {user_id}: {e}")

# NEW: post_init callback to check bot's admin status in channel
async def post_init_callback(application: Application):
    """Callback function to be run after the application is initialized."""
    # CHANNEL_ID is loaded from config, which loads from .env
    # We explicitly convert CHANNEL_ID to string for consistent comparison with "-100"
    if CHANNEL_ID and str(CHANNEL_ID) != "-100":
        try:
            bot_member = await application.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=application.bot.id)
            if bot_member.status not in ['administrator', 'creator']:
                logger.error(f"Bot is NOT an administrator in the configured CHANNEL_ID ({CHANNEL_ID}). Mandatory channel verification might fail for users.")
                print(f"WARNING: Bot is NOT an administrator in the configured CHANNEL_ID ({CHANNEL_ID}). Mandatory channel verification might fail for users. Ensure bot is admin in the channel.")
            else:
                logger.info(f"Bot is an administrator in CHANNEL_ID ({CHANNEL_ID}).")
        except Exception as e:
            logger.error(f"Could not verify bot's admin status in CHANNEL_ID {CHANNEL_ID}: {e}. Ensure CHANNEL_ID is correct and bot has been added to the channel.")
            print(f"WARNING: Could not verify bot's admin status in CHANNEL_ID {CHANNEL_ID}. Ensure CHANNEL_ID is correct and bot has been added to the channel. Error: {e}")


def main() -> None: 
    """Runs the bot."""
    init_web3_instances()
    init_db() 

    # Build the Application with post_init callback directly
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init_callback).build() 

    # Conversation Handler for /start and channel join check
    start_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            AWAITING_CHANNEL_JOIN: [CallbackQueryHandler(check_channel_membership, pattern='^check_channel_join$')],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)], 
        allow_reentry=True 
    )

    # Add other conversation handlers
    claim_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_claim_button, pattern='^claim_token_.*$')],
        states={
            AWAITING_CLAIM_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_claim_address)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
        allow_reentry=True
    )
    
    purchase_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(purchase_menu, pattern='^purchase_menu$'),
            MessageHandler(filters.Regex("^Purchase Token 💳$"), purchase_menu)
        ],
        states={
            SELECTING_PURCHASE_TOKEN: [CallbackQueryHandler(handle_purchase_selection, pattern='^buy_token_.*$')],
            AWAITING_PURCHASE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_purchase_amount)]
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
        allow_reentry=True
    )

    # UPDATED: get_more_tokens_conv_handler to include new states and handlers
    get_more_tokens_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^Get More Tokens ☕$"), handle_get_more_tokens_button_entry)
        ],
        states={
            SELECTING_REWARD_TOKEN: [CallbackQueryHandler(handle_reward_token_selection, pattern='^select_reward_token_.*$')],
            AWAITING_REWARD_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_reward_address)],
            SELECTING_GET_MORE_TOKENS_TASK_TYPE: [CallbackQueryHandler(select_get_more_tokens_task_type, pattern='^(select_twitter_tasks|select_labubu_tasks)$')],
            AWAITING_TWITTER_FOLLOW_1_CONFIRM: [CallbackQueryHandler(handle_twitter_follow_1_check, pattern='^followed_petrukstar_check$')],
            AWAITING_TWITTER_USERNAME_1: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_twitter_username_1)],
            AWAITING_TWITTER_FOLLOW_2_CONFIRM: [CallbackQueryHandler(handle_twitter_follow_2_check, pattern='^followed_ikysyptraa_check$')],
            AWAITING_TWITTER_POST_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_twitter_post_link)],
            AWAITING_LABUBU_SCREENSHOT: [MessageHandler(filters.PHOTO & ~filters.COMMAND, handle_labubu_screenshot_submission)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
        allow_reentry=True
    )

    # Add all handlers
    application.add_handler(start_conv_handler) 
    application.add_handler(CommandHandler("cancel", cancel_conversation)) 
    application.add_handler(CommandHandler("faucet", handle_faucet_button))
    application.add_handler(MessageHandler(filters.Regex("^Faucet 🤖$"), handle_faucet_button))
    application.add_handler(MessageHandler(filters.Regex("^Balance 💰$"), balance_command))
    application.add_handler(CallbackQueryHandler(back_to_start_menu, pattern='^back_to_start$'))
    application.add_handler(CallbackQueryHandler(how_to_use_faucet, pattern='^how_to_use_faucet$'))
    application.add_handler(CallbackQueryHandler(handle_faucet_button, pattern='^faucet_menu_reopen$'))
    application.add_handler(claim_conv_handler)
    application.add_handler(purchase_conv_handler)
    application.add_handler(get_more_tokens_conv_handler)

    application.add_handler(CallbackQueryHandler(handle_admin_verification, pattern='^admin_(approve|reject)_task_.*$'))
    
    application.add_handler(CommandHandler("send", send_command))
    application.add_handler(CommandHandler("stat", stat_command))
    application.add_handler(CommandHandler("broadcast", broadcast_message))
    application.add_handler(CommandHandler("maintenance", maintenance_command))
    
    logger.info("Bot is running...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
