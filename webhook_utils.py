# webhook_utils.py
import threading
import requests

# WEBHOOK_URL = "https://540c-86-98-4-252.ngrok-free.app/mobile/webhook/audio-processed"
WEBHOOK_URL = "https://biggerbluebutton.com/mobile/webhook/audio-processed"

def async_send_webhook(data):
    """Send the webhook in a separate thread to avoid blocking."""
    def background_send():
        try:
            response = requests.post(WEBHOOK_URL, json=data, headers={"Content-Type": "application/json"})
            response.raise_for_status()
            print(f"Webhook sent successfully: {response.status_code}")
        except requests.RequestException as e:
            print(f"Failed to send webhook: {str(e)}")

    t = threading.Thread(target=background_send)
    t.start()
