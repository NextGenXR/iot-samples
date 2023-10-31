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

    IOT_TOPIC_DATA = f"{CONTENT_DIR}/{iot_topic}_iot_data.csv"
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
    LIVE_URL = f"{BASE_URL}/{iot_topic}.live"

    live_layer = Sdf.Layer.CreateNew(LIVE_URL)
    if not live_layer:
        raise Exception(f"Could load the live layer {LIVE_URL}.")

    Sdf.PrimSpec(live_layer, "iot", Sdf.SpecifierDef, "IoT Root")
    live_layer.Save()
    return live_layer


async def initialize_async(iot_topic):
    # copy a the Conveyor Belt to the target nucleus server
    LOCAL_URL = f"file:{CONTENT_DIR}/ConveyorBelt_{iot_topic}.usd"
    STAGE_URL = f"{BASE_URL}/ConveyorBelt_{iot_topic}.usd"
    LIVE_URL = f"{BASE_URL}/{iot_topic}.live"
    result = await omni.client.copy_async(
        LOCAL_URL,
        STAGE_URL,
        behavior=omni.client.CopyBehavior.ERROR_IF_EXISTS,
        message="Copy Conveyor Belt",
    )

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
    # write the iot values to the usd prim attributes
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
    # we assume that the file contains the data for single device
    IOT_TOPIC_DATA = f"{CONTENT_DIR}/{iot_topic}_iot_data.csv"
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


if __name__ == "__main__":
    IOT_TOPIC = "A08_PR_NVD_01"
    omni.client.initialize()
    omni.client.set_log_level(omni.client.LogLevel.DEBUG)
    omni.client.set_log_callback(log_handler)
    try:
        stage, live_layer = asyncio.run(initialize_async(IOT_TOPIC))
        run(stage, live_layer, IOT_TOPIC)
    except:
        print('---- LOG MESSAGES ---')
        print(*messages, sep='\n')
        print('----')
    finally:
        omni.client.shutdown()
