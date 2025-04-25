import os
import time
import json
import threading
from flask import Flask, request, jsonify
import redis
import requests # To send data to your chatbot

# --- Configuration ---
# Load configuration from environment variables set in Render
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379') # Get Redis URL from Render env
ZAPIER_SECRET_TOKEN = os.environ.get('ZAPIER_SECRET_TOKEN', 'default_secret_token') # Shared secret with Zapier
CHATBOT_API_URL = os.environ.get('CHATBOT_API_URL') # The URL to send combined messages to
DEBOUNCE_DELAY_SECONDS = 10 # How long to wait after the LAST message before sending (e.g., 10 seconds)

# --- Initialization ---
app = Flask(__name__)

# Connect to Redis
# Render provides the REDIS_URL environment variable
# decode_responses=True makes sure Redis returns strings, not bytes
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping() # Check connection
    print("Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    print(f"Error connecting to Redis: {e}")
    # Handle error appropriately - maybe exit or run without Redis (not recommended for this app)
    redis_client = None # Indicate connection failure

# Dictionary to keep track of active timers for each user
# IMPORTANT LIMITATION: This timer dict is in-memory. If the Render service restarts,
# running timers are lost. A more robust solution might use APScheduler with a Redis job store,
# or a purely Redis-based time check, but that adds complexity.
user_timers = {}

# --- Helper Functions ---

def send_combined_messages(user_id):
    """
    Retrieves messages from Redis, combines them, sends to chatbot, and clears.
    This function is called by the timer when it expires.
    """
    global user_timers
    if not redis_client:
        print(f"Redis client not available. Cannot send messages for user {user_id}")
        return

    user_message_key = f"user_messages:{user_id}"

    try:
        # Retrieve all messages for the user using Redis List commands
        # LRANGE 0 -1 gets all elements
        messages_json_list = redis_client.lrange(user_message_key, 0, -1)

        if not messages_json_list:
            print(f"No messages found in Redis for user {user_id} when timer fired.")
            return # Nothing to send

        # Combine messages
        combined_text = ""
        message_texts = []
        for msg_json in messages_json_list:
            try:
                msg_data = json.loads(msg_json)
                message_texts.append(msg_data.get('text', ''))
            except json.JSONDecodeError:
                print(f"Warning: Could not decode message JSON: {msg_json}")
        
        # Join messages with a newline for clarity
        combined_text = "\n".join(message_texts) 

        print(f"Combined messages for user {user_id}: {combined_text}")

        # --- Send to Chatbot ---
        if not CHATBOT_API_URL:
            print("CHATBOT_API_URL not set. Cannot send message.")
            # Optionally delete messages from Redis even if sending fails
            # redis_client.delete(user_message_key) 
            return

        try:
            # !!! IMPORTANT: Adapt this payload structure for YOUR chatbot's API !!!
            chatbot_payload = {
                'userId': user_id,
                'combinedMessage': combined_text
                # Add any other fields your chatbot API requires
            }
            # Add headers if needed (e.g., Content-Type, Authorization)
            headers = {'Content-Type': 'application/json'} 
            
            response = requests.post(CHATBOT_API_URL, json=chatbot_payload, headers=headers, timeout=10)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            print(f"Successfully sent combined message for user {user_id} to chatbot. Status: {response.status_code}")

            # --- Clear messages from Redis ONLY after successful send ---
            redis_client.delete(user_message_key)
            print(f"Cleared messages for user {user_id} from Redis.")

        except requests.exceptions.RequestException as e:
            print(f"Error sending message to chatbot for user {user_id}: {e}")
            # Decide if you want to keep the messages in Redis to retry later
            # or delete them anyway. For simplicity here, we don't delete on failure.

    except redis.exceptions.RedisError as e:
        print(f"Redis error processing messages for user {user_id}: {e}")
    except Exception as e:
        print(f"Unexpected error processing messages for user {user_id}: {e}")
    finally:
        # Clean up the timer entry for this user
        if user_id in user_timers:
            del user_timers[user_id]


# --- Webhook Endpoint ---

@app.route('/webhook', methods=['POST'])
def zapier_webhook():
    """Receives messages from Zapier, stores them, and manages timers."""
    global user_timers
    
    # --- Security Check ---
    auth_header = request.headers.get('Authorization')
    expected_auth = f"Bearer {ZAPIER_SECRET_TOKEN}"
    if not auth_header or auth_header != expected_auth:
        print("Unauthorized access attempt.")
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    # --- Get Data ---
    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "Missing JSON payload"}), 400

    user_id = data.get('userID')
    message_text = data.get('messageText')

    if not user_id or not message_text:
        return jsonify({"status": "error", "message": "Missing 'userID' or 'messageText'"}), 400

    if not redis_client:
         return jsonify({"status": "error", "message": "Redis connection not available"}), 500

    print(f"Received message from user {user_id}: {message_text}")

    # --- Store Message in Redis List ---
    user_message_key = f"user_messages:{user_id}"
    message_data = {
        'text': message_text,
        'timestamp': time.time()
    }
    try:
        # RPUSH adds the message to the end of the list
        redis_client.rpush(user_message_key, json.dumps(message_data))
        # Optional: Set an expiration on the list itself as a safety net,
        # but the timer logic is primary. Expire after 1 day for example.
        redis_client.expire(user_message_key, 86400) 
        print(f"Stored message in Redis list: {user_message_key}")
    except redis.exceptions.RedisError as e:
        print(f"Redis error storing message for user {user_id}: {e}")
        return jsonify({"status": "error", "message": "Failed to store message"}), 500


    # --- Manage Debounce Timer ---
    # Cancel existing timer for this user, if any
    if user_id in user_timers:
        user_timers[user_id].cancel()
        print(f"Cancelled existing timer for user {user_id}")

    # Start a new timer
    # When this timer finishes (if not cancelled by another incoming message),
    # it will call send_combined_messages
    timer = threading.Timer(DEBOUNCE_DELAY_SECONDS, send_combined_messages, args=[user_id])
    user_timers[user_id] = timer
    timer.start()
    print(f"Started/Reset timer for user {user_id} for {DEBOUNCE_DELAY_SECONDS} seconds.")

    # --- Respond to Zapier ---
    # Send success response immediately, don't wait for the timer
    return jsonify({"status": "success", "message": "Message received and buffered."}), 200

# --- Run the App ---
# This part is usually handled by the deployment platform (like Gunicorn on Render)
# You might need this for local testing:
# if __name__ == '__main__':
#    # Make sure CHATBOT_API_URL is set if testing locally
#    # Use a different port if 5000 is taken
#    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
