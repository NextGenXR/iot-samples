# SPDX-FileCopyrightText: Copyright (c) 2023 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# pip install pandas

import asyncio
import os
import omni.client
from pxr import Usd, Sdf
from pathlib import Path
import pandas as pd
import time

OMNI_HOST = os.environ.get("OMNI_HOST", "localhost")
BASE_URL = "omniverse://" + OMNI_HOST + "/Projects/IoT/Samples/HeadlessApp"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONTENT_DIR = Path(SCRIPT_DIR).resolve().parents[1].joinpath("content")

messages = []


def log_handler(thread, component, level, message):
    # print(message)
    messages.append((thread, component, level, message))


def initialize_device_prim(live_layer, iot_topic):
    """
    Initializes the IoT root and spec for the given iot_topic, and creates all the IoT attributes that will be written.

    Args:
        live_layer (Sdf.Layer): The layer to create the IoT root and spec in.
        iot_topic (str): The name of the IoT topic.

    Raises:
        Exception: If the IoT spec could not be created or if an attribute could not be defined.
    """
    iot_root = live_layer.GetPrimAtPath("/iot")
    if not iot_root:
        iot_root = Sdf.PrimSpec(live_layer, "iot", Sdf.SpecifierDef, "IoT Root")

    iot_spec = live_layer.GetPrimAtPath(f"/iot/{iot_topic}")
    if not iot_spec:
        iot_spec = Sdf.PrimSpec(iot_root, iot_topic, Sdf.SpecifierDef, "ConveyorBelt Type")
    if not iot_spec:
        raise Exception("Failed to create the IoT Spec.")

    # clear out any attrubutes that may be on the spec
    for attrib in iot_spec.attributes:
        iot_spec.RemoveProperty(attrib)

    IOT_TOPIC_DATA = os.path.join(CONTENT_DIR, f"{iot_topic}_iot_data.csv")
    data = pd.read_csv(IOT_TOPIC_DATA)
    data.head()

    # create all the IoT attributes that will be written
    attr = Sdf.AttributeSpec(iot_spec, "_ts", Sdf.ValueTypeNames.Double)
    if not attr:
        raise Exception(f"Could not define the attribute: {attrName}")

    # infer the unique data points in the CSV.
    # The values may be known in advance and can be hard coded
    grouped = data.groupby("Id")
    for attrName, group in grouped:
        attr = Sdf.AttributeSpec(iot_spec, attrName, Sdf.ValueTypeNames.Double)
        if not attr:
            raise Exception(f"Could not define the attribute: {attrName}")


def create_live_layer(iot_topic):
    """
    Creates a new live layer for the given IoT topic.

    Args:
        iot_topic (str): The name of the IoT topic.

    Returns:
        Sdf.Layer: The newly created live layer.
    """
    LIVE_URL = f"{BASE_URL}/{iot_topic}.live"

    live_layer = Sdf.Layer.CreateNew(LIVE_URL)
    if not live_layer:
        raise Exception(f"Could load the live layer {LIVE_URL}.")

    Sdf.PrimSpec(live_layer, "iot", Sdf.SpecifierDef, "IoT Root")
    live_layer.Save()
    return live_layer


async def initialize_async(iot_topic):
    """
    Initializes the USD stage for the given IoT topic by:

    * copying the Conveyor Belt USD file to the target nucleus server,
    * opening the stage,
    * creating a live layer if one does not already exist,
    * adding the live layer as a sublayer to the root layer,
    * setting the live layer as the edit target,
    * initializing the device prim, and
    * running the live process.

    Args:
        iot_topic (str): The IoT topic to initialize the USD stage for.

    Returns:
        Tuple[Usd.Stage, Sdf.Layer]: A tuple containing the initialized USD stage and the live layer.
    """

    await check_connection()

    # copy a the Conveyor Belt to the target nucleus server
    LOCAL_URL = f"file:{CONTENT_DIR}\\ConveyorBelt_{iot_topic}.usd"
    STAGE_URL = f"{BASE_URL}/ConveyorBelt_{iot_topic}.usd"
    LIVE_URL = f"{BASE_URL}/{iot_topic}.live"
    print(f'Copying {LOCAL_URL} to {STAGE_URL}...')
    result = await omni.client.copy_async(
        LOCAL_URL,
        STAGE_URL,
        behavior=omni.client.CopyBehavior.OVERWRITE,
        message="Copy Conveyor Belt",
    )

    if result is omni.client.Result.OK:
        print(f"\tCopying succeeded.")
    else:
        print(f'\tCopying failed. Status: {result}')

    print(f'Opening {STAGE_URL}...')
    stage = Usd.Stage.Open(STAGE_URL)
    if not stage:
        raise Exception(f"Could load the stage {STAGE_URL}.")

    root_layer = stage.GetRootLayer()
    live_layer = Sdf.Layer.FindOrOpen(LIVE_URL)
    if not live_layer:
        live_layer = create_live_layer(iot_topic)

    found = False
    subLayerPaths = root_layer.subLayerPaths
    for subLayerPath in subLayerPaths:
        if subLayerPath == live_layer.identifier:
            found = True

    if not found:
        root_layer.subLayerPaths.append(live_layer.identifier)
        root_layer.Save()

    # set the live layer as the edit target
    stage.SetEditTarget(live_layer)
    initialize_device_prim(live_layer, iot_topic)
    omni.client.live_process()
    return stage, live_layer


def write_to_live(live_layer, iot_topic, group, ts):
    """
    Write the IoT values to the USD prim attributes.

    Args:
        live_layer (pxr.Usd.Stage): The USD stage to write to.
        iot_topic (str): The name of the IoT topic.
        group (pandas.DataFrame): The group of IoT values to write.
        ts (float): The timestamp of the IoT values.

    Raises:
        Exception: If the attribute for an IoT value is not found.

    Returns:
        None
    """
    print(group.iloc[0]["TimeStamp"])
    ts_attribute = live_layer.GetAttributeAtPath(f"/iot/{iot_topic}._ts")
    ts_attribute.default = ts
    with Sdf.ChangeBlock():
        for index, row in group.iterrows():
            id = row["Id"]
            value = row["Value"]
            attr = live_layer.GetAttributeAtPath(f"/iot/{iot_topic}.{id}")
            if not attr:
                raise Exception(f"Could not find attribute /iot/{iot_topic}.{id}.")
            attr.default = value
    omni.client.live_process()


def run(stage, live_layer, iot_topic):
    """
    Plays back data from a CSV file in real-time, writing it to a live layer.

    Args:
        stage (str): The stage of the application.
        live_layer (str): The name of the live layer to write data to.
        iot_topic (str): The name of the IoT topic to read data from.
    """
    IOT_TOPIC_DATA = os.path.join(CONTENT_DIR, f"{iot_topic}_iot_data.csv")
    print(f"IoT Data file is: {IOT_TOPIC_DATA}")
    data = pd.read_csv(IOT_TOPIC_DATA)
    data.head()

    # Converting to DateTime Format and drop ms
    data["TimeStamp"] = pd.to_datetime(data["TimeStamp"])
    data["TimeStamp"] = data["TimeStamp"].dt.floor("s")

    data.set_index("TimeStamp")
    start_time = data.min()["TimeStamp"]
    last_time = start_time
    grouped = data.groupby("TimeStamp")

    # play back the data in real-time
    for next_time, group in grouped:
        diff = (next_time - last_time).total_seconds()
        if diff > 0:
            time.sleep(diff)
        write_to_live(live_layer, iot_topic, group, (next_time - start_time).total_seconds())
        last_time = next_time

async def check_connection():
    server_url = f'omniverse://{OMNI_HOST}'
    print(f'Checking host URL = {server_url}')
    status, response = omni.client.stat(server_url)
    if status is not omni.client.Result.OK:
        print(f"Failed to connect to {server_url}.")
        # exit(1)
    else:
        print(f"Successfully connected to {server_url}.")


if __name__ == "__main__":
    IOT_TOPIC = "A08_PR_NVD_01"

    result = omni.client.initialize()
    if not result:
        print("Failed to initialize the client library.")
        exit(1)

    omni.client.set_log_level(omni.client.LogLevel.DEBUG)
    omni.client.set_log_callback(log_handler)
    try:
        stage, live_layer = asyncio.run(initialize_async(IOT_TOPIC))
        run(stage, live_layer, IOT_TOPIC)
    except Exception as e:
        print(f'Exception occurred: {e}')
        print('\n---- LOG MESSAGES ---')
        # print(*messages, sep='\n')
        print('----')
    finally:
        omni.client.shutdown()
