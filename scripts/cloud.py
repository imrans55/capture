#!/usr/bin/env python



# [START iot_mqtt_includes]
import argparse
import datetime
import logging
import os
import random
import ssl
import time
import shutil
from google.cloud import storage

import jwt
import paho.mqtt.client as mqtt
import paramiko


# [END iot_mqtt_includes]

cams = ["cam01_out", "cam02_out", "cam03_out"]
cams1 = ["cam01", "cam02", "cam03"]
camera_stream = ["live01.m3u8", "live02.m3u8", "live03.m3u8"]
config = "/opt/keys/g_creds.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.CRITICAL)

# The initial backoff time after a disconnection occurs, in seconds.
minimum_backoff_time = 1

# The maximum backoff time before giving up, in seconds.
MAXIMUM_BACKOFF_TIME = 32

# Whether to wait with exponential backoff before publishing.
should_backoff = False
router_ip = "192.168.203.254"
router_username = "gps"
router_password = "gps321"
bucket_name = "production-setup-littering375jukm"
cam_path = "/home/jetson/cams/"
archive_path = "/home/jetson/archive/"
duration = "30"
def gps_data():

    vals = []
    starttime=time.time()
    time.sleep(0.98)
    ssh = paramiko.SSHClient()

    # Load SSH host keys.
    ssh.load_system_host_keys()
    # Add SSH host key automatically if needed.
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Connect to router using username/password authentication.
    ssh.connect(router_ip,
                username=router_username,
                password=router_password,
                look_for_keys=False )

    # Run command.
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    vals.append(timestamp + ";")
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("/system gps monitor once")

    output = ssh_stdout.readlines()
    # Close connection.
    ssh.close()

    for line in output:
        if "latitude" in line:
            lat, lval = line.split()
            vals.append(lval + ";")
        if "longitude" in line:
            log, loval = line.split()
            vals.append(loval + "\n")
    #print(vals)
    gdata = ""
    for ele in vals:
        gdata += ele
    return gdata

# [START iot_mqtt_jwt]
def create_jwt(project_id, private_key_file, algorithm):
    """Creates a JWT (https://jwt.io) to establish an MQTT connection.
    Args:
     project_id: The cloud project ID this device belongs to
     private_key_file: A path to a file containing either an RSA256 or
             ES256 private key.
     algorithm: The encryption algorithm to use. Either 'RS256' or 'ES256'
    Returns:
        A JWT generated from the given project_id and private key, which
        expires in 20 minutes. After 20 minutes, your client will be
        disconnected, and a new JWT will have to be generated.
    Raises:
        ValueError: If the private_key_file does not contain a known key.
    """

    token = {
        # The time that the token was issued at
        "iat": datetime.datetime.utcnow(),
        # The time the token expires.
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=30),
        # The audience field should always be set to the GCP project id.
        "aud": project_id,
    }

    # Read the private key file.
    with open(private_key_file, "r") as f:
        private_key = f.read()

    print(
        "Creating JWT using {} from private key file {}".format(
            algorithm, private_key_file
        )
    )

    return jwt.encode(token, private_key, algorithm=algorithm)


# [END iot_mqtt_jwt]


# [START iot_mqtt_config]
def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return "{}: {}".format(rc, mqtt.error_string(rc))


def on_connect(unused_client, unused_userdata, unused_flags, rc):
    """Callback for when a device connects."""
    print("on_connect", mqtt.connack_string(rc))

    # After a successful connect, reset backoff time and stop backing off.
    global should_backoff
    global minimum_backoff_time
    should_backoff = False
    minimum_backoff_time = 1


def on_disconnect(unused_client, unused_userdata, rc):
    """Paho callback for when a device disconnects."""
    print("on_disconnect", error_str(rc))

    # Since a disconnect occurred, the next loop iteration will wait with
    # exponential backoff.
    global should_backoff
    should_backoff = True


def on_publish(unused_client, unused_userdata, unused_mid):
    """Paho callback when a message is sent to the broker."""
    print("on_publish")

def bucket_upload(blob_name, file_path):
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        return True

    except Exception as e:
        print(e)
        return False

def on_message_b(unused_client, unused_userdata, message):
    """Callback when the device receives a message on a subscription."""
    payload = str(message.payload.decode("utf-8"))
    print(
        "Received message '{}' on topic '{}' with Qos {}".format(
            payload, message.topic, str(message.qos)
        )
    )
    tstamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    delete = datetime.datetime.now()
    strg1 = "sudo ffmpeg -i /home/jetson/cams/cam01/live01.m3u8 -t 30 /home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp)
    strg2 = "sudo ffmpeg -i /home/jetson/cams/cam02/live02.m3u8 -t 30 /home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp)
    strg3 = "sudo ffmpeg -i /home/jetson/cams/cam03/live03.m3u8 -t 30 /home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp)
    if "store_remotelly" in payload:
        os.system(strg1)
        os.system(strg2)
        os.system(strg3)
        #time.sleep(60)
        valid = 3
        for cam in cams:
            vids = os.listdir(cam_path + cam)
            for vid in vids:
                blob_file = cam + '/' +vid
                blob_path = cam_path + cam + '/' + vid
                bucket_upload(blob_file, blob_path )
                print(blob_path + "uploaded")
        validity = delete  + datetime.timedelta(days=valid)
        vdate = validity.strftime("%Y-%m-%d")
        shutil.copyfile("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp), "/home/jetson/archive/{}_c1_30.mp4".format(tstamp) )
        shutil.copyfile("/home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp), "/home/jetson/archive/{}_c2_30.mp4".format(tstamp) )
        shutil.copyfile("/home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp), "/home/jetson/archive/{}_c3_30.mp4".format(tstamp))
        txt_file = open("/home/jetson/archive.txt","a")
        text1 = "/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp) + " " + str(valid) + " " + vdate + "\n"
        text2 = "/home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp) + " " + str(valid) + " " + vdate+ "\n"
        text3 = "/home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp) + " " + str(valid) + " " + vdate + "\n"
        txt_file.write(text1)
        txt_file.write(text2)
        txt_file.write(text3)

        os.remove("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp))
        os.remove("/home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp))
        os.remove("/home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp))
        os.system|("sudo reboot now")
    elif "store_locally" in payload:
        os.system(strg1)
        os.system(strg2)
        os.system(strg3)
        #time.sleep(60)
        valid = [int(i) for i in payload.split() if i.isdigit()]
        #if valid:
        valid1 = valid[0]
        for cam in cams:
            vids = os.listdir(cam_path + cam)
            for vid in vids:
                blob_file = cam + '/' +vid
                blob_path = cam_path + cam + '/' + vid
                #bucket_upload(blob_file, blob_path )
                print(blob_path + " Operation Finished")
        validity = delete  + datetime.timedelta(days=valid1)
        vdate = validity.strftime("%Y-%m-%d")
        shutil.copyfile("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp), "/home/jetson/archive/{}_c1_30.mp4".format(tstamp) )
        shutil.copyfile("/home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp), "/home/jetson/archive/{}_c2_30.mp4".format(tstamp) )
        shutil.copyfile("/home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp), "/home/jetson/archive/{}_c3_30.mp4".format(tstamp))
        txt_file = open("/home/jetson/archive.txt","a")
        text1 = "/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp) + " " + str(valid) + " " + vdate + "\n"
        text2 = "/home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp) + " " + str(valid) + " " + vdate+ "\n"
        text3 = "/home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp) + " " + str(valid) + " " + vdate + "\n"
        txt_file.write(text1)
        txt_file.write(text2)
        txt_file.write(text3)
        os.remove("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp))
        os.remove("/home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp))
        os.remove("/home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp))
        os.system|("sudo reboot now")
    elif "remotelly_and_locally" in payload:
        os.system(strg1)
        os.system(strg2)
        os.system(strg3)
        #time.sleep(60)
        valid = [int(i) for i in payload.split() if i.isdigit()]
        #if valid:
        valid1 = valid[0]
        for cam in cams:
            vids = os.listdir(cam_path + cam)
            for vid in vids:
                blob_file = cam + '/' +vid
                blob_path = cam_path + cam + '/' + vid
                bucket_upload(blob_file, blob_path )
                print(blob_path + "uploaded")

        validity = delete  + datetime.timedelta(days=valid1)
        vdate = validity.strftime("%Y-%m-%d")
        shutil.copyfile("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp), "/home/jetson/archive/{}_c1_30.mp4".format(tstamp) )
        shutil.copyfile("/home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp), "/home/jetson/archive/{}_c2_30.mp4".format(tstamp) )
        shutil.copyfile("/home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp), "/home/jetson/archive/{}_c3_30.mp4".format(tstamp))
        txt_file = open("/home/jetson/archive.txt","a")
        text1 = "/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp) + " " + str(valid) + " " + vdate + "\n"
        text2 = "/home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp) + " " + str(valid) + " " + vdate+ "\n"
        text3 = "/home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp) + " " + str(valid) + " " + vdate + "\n"
        txt_file.write(text1)
        txt_file.write(text2)
        txt_file.write(text3)
        os.remove("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp))
        os.remove("/home/jetson/cams/cam02_out/{}_c2_30.mp4".format(tstamp))
        os.remove("/home/jetson/cams/cam03_out/{}_c3_30.mp4".format(tstamp))
        os.system|("sudo reboot now")

def on_message(unused_client, unused_userdata, message):
    """Callback when the device receives a message on a subscription."""
    payload = str(message.payload.decode("utf-8"))
    print(
        "Received message '{}' on topic '{}' with Qos {}".format(
            payload, message.topic, str(message.qos)
        )
    )
    tstamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    delete = datetime.datetime.now()
    cmds = []
    for r in range(0,2):
        cmd = "sudo ffmpeg -i /home/jetson/cams/{}/{} -t {} /home/jetson/cams/{}/{}_{}_{}.mp4".format(cams1[r], camera_stream[r], duration,cams[r],tstamp, camera_stream[r],duration)
        cmds.append(cmd)
    if "store_remotelly" in payload:
        for c in cmds:
            print(c)
            #os.system(c)
        #time.sleep(60)
        valid = 3
        for cam in cams:
            vids = os.listdir(cam_path + cam)
            for vid in vids:
                blob_file = cam + '/' +vid
                blob_path = cam_path + cam + '/' + vid
                up = bucket_upload(blob_file, blob_path )
                if up == True:
                    print(blob_path + "uploaded")
                else:
                    print("Error")
        validity = delete  + datetime.timedelta(days=valid)
        vdate = validity.strftime("%Y-%m-%d")
        for r in range(0,2):

            shutil.copyfile("/home/jetson/cams/{}/{}_{}_{}.mp4".format(cams[r],tstamp, camera_stream[r], duration), "/home/jetson/archive/{}/{}_{}_{}.mp4".format(cams[r],tstamp, camera_stream[r], duration) )
        txt_file = open("/home/jetson/archive.txt","a")
        for r in range(0,2):
            text = "/home/jetson/cams/{}/{}_{}_{}.mp4".format(cams[r],tstamp, camera_stream[r], duration) + " " + str(valid) + " " + vdate + "\n"
            txt_file.write(text)
        
        for r in range(0,2):
            os.remove("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp))

        for r in range(0,2):
            os.remove("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp))
        os.system|("sudo reboot now")


    elif "store_locally" in payload:
        for r in range(0,2):
            cmd = "sudo ffmpeg -i /home/jetson/cams/{}/{} -t {} /home/jetson/cams/{}/{}_{}_{}.mp4".format(cams1[r], camera_stream[r], duration,cams[r],tstamp, camera_stream[r],duration)
            cmds.append(cmd)
        #time.sleep(60)
        valid = [int(i) for i in payload.split() if i.isdigit()]
        #if valid:
        valid1 = valid[0]
        for cam in cams:
            vids = os.listdir(cam_path + cam)
            for vid in vids:
                blob_file = cam + '/' +vid
                blob_path = cam_path + cam + '/' + vid
                #bucket_upload(blob_file, blob_path )
                print(blob_path + " Operation Finished")
        validity = delete  + datetime.timedelta(days=valid1)
        vdate = validity.strftime("%Y-%m-%d")
        for r in range(0,2):

            shutil.copyfile("/home/jetson/cams/{}/{}_{}_{}.mp4".format(cams[r],tstamp, camera_stream[r], duration), "/home/jetson/archive/{}/{}_{}_{}.mp4".format(cams[r],tstamp, camera_stream[r], duration) )
        txt_file = open("/home/jetson/archive.txt","a")
        for r in range(0,2):
            text = "/home/jetson/cams/{}/{}_{}_{}.mp4".format(cams[r],tstamp, camera_stream[r], duration) + " " + str(valid) + " " + vdate + "\n"
            txt_file.write(text)
        
        for r in range(0,2):
            os.remove("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp))

        for r in range(0,2):
            os.remove("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp))
        os.system|("sudo reboot now")
    elif "remotelly_and_locally" in payload:
        for r in range(0,2):
            cmd = "sudo ffmpeg -i /home/jetson/cams/{}/{} -t {} /home/jetson/cams/{}/{}_{}_{}.mp4".format(cams1[r], camera_stream[r], duration,cams[r],tstamp, camera_stream[r],duration)
            cmds.append(cmd)
        #time.sleep(60)
        valid = [int(i) for i in payload.split() if i.isdigit()]
        #if valid:
        valid1 = valid[0]
        for cam in cams:
            vids = os.listdir(cam_path + cam)
            for vid in vids:
                blob_file = cam + '/' +vid
                blob_path = cam_path + cam + '/' + vid
                bucket_upload(blob_file, blob_path )
                print(blob_path + "uploaded")

        validity = delete  + datetime.timedelta(days=valid1)
        vdate = validity.strftime("%Y-%m-%d")
        for r in range(0,2):

            shutil.copyfile("/home/jetson/cams/{}/{}_{}_{}.mp4".format(cams[r],tstamp, camera_stream[r], duration), "/home/jetson/archive/{}/{}_{}_{}.mp4".format(cams[r],tstamp, camera_stream[r], duration) )
        txt_file = open("/home/jetson/archive.txt","a")
        for r in range(0,2):
            text = "/home/jetson/cams/{}/{}_{}_{}.mp4".format(cams[r],tstamp, camera_stream[r], duration) + " " + str(valid) + " " + vdate + "\n"
            txt_file.write(text)
        
        for r in range(0,2):
            os.remove("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp))

        for r in range(0,2):
            os.remove("/home/jetson/cams/cam01_out/{}_c1_30.mp4".format(tstamp))
        os.system|("sudo reboot now")

def get_client(
    project_id,
    cloud_region,
    registry_id,
    device_id,
    private_key_file,
    algorithm,
    ca_certs,
    mqtt_bridge_hostname,
    mqtt_bridge_port,
):
    """Create our MQTT client. The client_id is a unique string that identifies
    this device. For Google Cloud IoT Core, it must be in the format below."""
    client_id = "projects/{}/locations/{}/registries/{}/devices/{}".format(
        project_id, cloud_region, registry_id, device_id
    )
    print("Device client_id is '{}'".format(client_id))

    client = mqtt.Client(client_id=client_id)

    # With Google Cloud IoT Core, the username field is ignored, and the
    # password field is used to transmit a JWT to authorize the device.
    client.username_pw_set(
        username="unused", password=create_jwt(project_id, private_key_file, algorithm)
    )

    # Enable SSL/TLS support.
    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    # Register message callbacks. https://eclipse.org/paho/clients/python/docs/
    # describes additional callbacks that Paho supports. In this example, the
    # callbacks just print to standard out.
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # Connect to the Google MQTT bridge.
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    # This is the topic that the device will receive configuration updates on.
    mqtt_config_topic = "/devices/{}/config".format(device_id)

    # Subscribe to the config topic.
    client.subscribe(mqtt_config_topic, qos=1)

    # The topic that the device will receive commands on.
    mqtt_command_topic = "/devices/{}/commands/#".format(device_id)

    # Subscribe to the commands topic, QoS 1 enables message acknowledgement.
    print("Subscribing to {}".format(mqtt_command_topic))
    client.subscribe(mqtt_command_topic, qos=0)

    return client


# [END iot_mqtt_config]









def parse_command_line_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=("Example Google Cloud IoT Core MQTT device connection code.")
    )
    parser.add_argument(
        "--algorithm",
        choices=("RS256", "ES256"),
        required=True,
        help="Which encryption algorithm to use to generate the JWT.",
    )
    parser.add_argument(
        "--ca_certs",
        default="roots.pem",
        help="CA root from https://pki.google.com/roots.pem",
    )
    parser.add_argument(
        "--cloud_region", default="us-central1", help="GCP cloud region"
    )
    parser.add_argument(
        "--data",
        default="Hello there",
        help="The telemetry data sent on behalf of a device",
    )
    parser.add_argument("--device_id", required=True, help="Cloud IoT Core device id")
    parser.add_argument("--gateway_id", required=False, help="Gateway identifier.")
    parser.add_argument(
        "--jwt_expires_minutes",
        default=20,
        type=int,
        help="Expiration time, in minutes, for JWT tokens.",
    )
    parser.add_argument(
        "--listen_dur",
        default=60,
        type=int,
        help="Duration (seconds) to listen for configuration messages",
    )
    parser.add_argument(
        "--message_type",
        choices=("event", "state"),
        default="event",
        help=(
            "Indicates whether the message to be published is a "
            "telemetry event or a device state message."
        ),
    )
    parser.add_argument(
        "--mqtt_bridge_hostname",
        default="mqtt.googleapis.com",
        help="MQTT bridge hostname.",
    )
    parser.add_argument(
        "--mqtt_bridge_port",
        choices=(8883, 443),
        default=8883,
        type=int,
        help="MQTT bridge port.",
    )
    parser.add_argument(
        "--num_messages", type=int, default=17280, help="Number of messages to publish."
    )
    parser.add_argument(
        "--private_key_file", required=True, help="Path to private key file."
    )
    parser.add_argument(
        "--project_id",
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
        help="GCP cloud project name",
    )
    parser.add_argument(
        "--registry_id", required=True, help="Cloud IoT Core registry id"
    )
    parser.add_argument(
        "--service_account_json",
        default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Path to service account json file.",
    )

    # Command subparser
    command = parser.add_subparsers(dest="command")

    command.add_parser("device_demo", help=mqtt_device_demo.__doc__)


    return parser.parse_args()


def mqtt_device_demo(args):
    """Connects a device, sends data, and receives data."""
    # [START iot_mqtt_run]
    global minimum_backoff_time
    global MAXIMUM_BACKOFF_TIME

    # Publish to the events or state topic based on the flag.
    #sub_topic = "events" if args.message_type == "event" else "state"

    mqtt_topic = "/devices/jetson03/events/projects/littering-car/topics/littering-production01_messages"

    jwt_iat = datetime.datetime.utcnow()
    jwt_exp_mins = args.jwt_expires_minutes
    client = get_client(
        args.project_id,
        args.cloud_region,
        args.registry_id,
        args.device_id,
        args.private_key_file,
        args.algorithm,
        args.ca_certs,
        args.mqtt_bridge_hostname,
        args.mqtt_bridge_port,
    )

    # Publish num_messages messages to the MQTT bridge once per second.
    for i in range(1, args.num_messages + 1):
        # Process network events.
        client.loop()

        # Wait if backoff is required.
        if should_backoff:
            # If backoff time is too large, give up.
            if minimum_backoff_time > MAXIMUM_BACKOFF_TIME:
                print("Exceeded maximum backoff time. Giving up.")
                break

            # Otherwise, wait and connect again.
            delay = minimum_backoff_time
            print("Waiting for {} before reconnecting.".format(delay))
            time.sleep(delay)
            minimum_backoff_time = 1
            client.connect(args.mqtt_bridge_hostname, args.mqtt_bridge_port)

        #payload = gps_data()
       # payload = "test"
        #print("Publishing message {}/{}: '{}'".format(i, args.num_messages, payload))
        # [START iot_mqtt_jwt_refresh]
        seconds_since_issue = (datetime.datetime.utcnow() - jwt_iat).seconds
        if seconds_since_issue > 60 :
            print("Refreshing token after {}s".format(seconds_since_issue))
            jwt_iat = datetime.datetime.utcnow()
            client.loop()
            client.disconnect()
            client = get_client(
                args.project_id,
                args.cloud_region,
                args.registry_id,
                args.device_id,
                args.private_key_file,
                args.algorithm,
                args.ca_certs,
                args.mqtt_bridge_hostname,
                args.mqtt_bridge_port,
            )
        # [END iot_mqtt_jwt_refresh]
        # Publish "payload" to the MQTT topic. qos=1 means at least once
        # delivery. Cloud IoT Core also supports qos=0 for at most once
        # delivery.
        #payload = gps_data()
       # print("Publishing message {}/{}: '{}'".format(i, args.num_messages, payload))
        # Send events every second. State should not be updated as often
        while True:
            payload = gps_data()
            time.sleep(5)
            client.publish(mqtt_topic, payload, qos=0)
            print(payload)
            client.loop()
    # [END iot_mqtt_run]


def main():
    args = parse_command_line_args()

    
    mqtt_device_demo(args)
    print("Finished.")


if __name__ == "__main__":
    main()
