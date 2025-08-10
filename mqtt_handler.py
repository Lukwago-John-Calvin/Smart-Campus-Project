import paho.mqtt.client as mqtt
import json
import time # Added for time.sleep
# import database_handler # Uncomment this line if you have a database_handler.py file

# =============================================
# MQTT Configuration
# =============================================
MQTT_BROKER = "localhost" # The MQTT broker is expected to be running on the same machine as this script
MQTT_PORT = 1883          # Standard MQTT port
MQTT_TOPIC_WILDCARD = "iot/campus/data/#" # Subscribe to all sub-topics under iot/campus/data/

# =============================================
# MQTT Callbacks
# =============================================

def on_connect(client, userdata, flags, rc):
    """
    Callback function for when the client connects to the MQTT broker.
    """
    if rc == 0:
        print("✅ Connected to MQTT Broker")
        # Subscribe to the wildcard topic to receive messages from all nodes
        client.subscribe(MQTT_TOPIC_WILDCARD)
        print(f"Subscribed to topic: {MQTT_TOPIC_WILDCARD}")
    else:
        print(f"❌ Failed to connect. Return code {rc}")
        # You might want to add more robust error handling here,
        # like exiting the script or attempting to reconnect.

def on_message(client, userdata, msg):
    """
    Callback function for when a message is received from the MQTT broker.
    """
    print(f"\n📥 Message received from topic: {msg.topic}")
    try:
        # Decode the payload from bytes to a UTF-8 string, then parse as JSON
        payload = json.loads(msg.payload.decode('utf-8'))
        print(f"📊 Payload: {json.dumps(payload, indent=2)}") # Pretty print JSON

        # =================================================================
        # Database Handling (Placeholder)
        # =================================================================
        # This is where you would integrate with your database.
        # The `msg.topic` can be used to determine the type of data and
        # route it to the appropriate database function.

        # Example of how you might use the topic to route data:
        # if msg.topic == "iot/campus/data/environment":
        #     print("Processing Environment Data...")
        #     # database_handler.write_environment_data(payload)
        # elif msg.topic == "iot/campus/data/classroom":
        #     print("Processing Classroom Data...")
        #     # database_handler.write_classroom_data(payload)
        # elif msg.topic == "iot/campus/data/security":
        #     print("Processing Security Data...")
        #     # database_handler.write_security_data(payload)
        # else:
        #     print("Processing Unknown Data Type...")
        #     # database_handler.write_generic_data(payload, msg.topic)

        # For now, if you have a single `write_sensor_data` function in `database_handler.py`:
        # database_handler.write_sensor_data(payload) # Uncomment this line if database_handler is ready

    except json.JSONDecodeError:
        print("❗ Invalid JSON received. Could not decode payload.")
        print(f"Raw payload: {msg.payload.decode('utf-8', errors='ignore')}") # Print raw payload for debugging
    except Exception as e:
        print(f"❗ An unexpected error occurred while handling message: {e}")

# =============================================
# Main MQTT Client Logic
# =============================================

def start_mqtt_client():
    """
    Initializes and starts the MQTT client.
    """
    client = mqtt.Client()
    client.on_connect = on_connect # Assign the connect callback
    client.on_message = on_message # Assign the message callback

    try:
        # Attempt to connect to the broker
        client.connect(MQTT_BROKER, MQTT_PORT, 60) # 60-second keepalive
    except Exception as e:
        print(f"Failed to connect to MQTT broker: {e}")
        print("Please ensure the broker is running and accessible at "
              f"{MQTT_BROKER}:{MQTT_PORT}")
        # Exit if connection fails at startup
        exit(1)

    # Start the loop in a separate thread. This allows the main thread
    # to continue running other code (like the `while True: pass` loop).
    client.loop_start()
    return client

# =============================================
# Script Entry Point
# =============================================

if __name__ == "__main__":
    print("Starting MQTT Broker Client...")
    mqtt_client = start_mqtt_client()

    try:
        # Keep the main thread alive. Messages are handled by the loop_start() thread.
        while True:
            time.sleep(1) # Sleep to prevent busy-waiting, but still allow KeyboardInterrupt
    except KeyboardInterrupt:
        print("\n🔌 Disconnecting MQTT client...")
        mqtt_client.loop_stop()    # Stop the MQTT loop thread
        mqtt_client.disconnect()   # Disconnect from the broker
        print("MQTT client disconnected.")
    except Exception as e:
        print(f"An unexpected error occurred in the main loop: {e}")