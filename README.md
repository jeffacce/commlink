# Commlink

Commlink exposes a lightweight Remote Procedure Call (RPC) layer on top of [ZeroMQ](https://zeromq.org/) that lets you interact
with objects running in a different process or host as if they were local. You can wrap any existing object with a single line
and obtain a client-side proxy that transparently mirrors attribute access, mutation, and callable invocation for anything that
can be pickled. Simple publisher/subscriber helpers are also available for broadcast-style messaging when you need them.

## Installation

```bash
pip install commlink
```

## RPC quickstart

### Server

```python
import numpy as np
from commlink import RPCServer


class Robot:
    def __init__(self):
        self.name = "robot_arm"
        self.target = np.zeros(7)
        self.joint_angles = np.zeros(7)

    def move_to(self, target):
        """Pretend to command the arm and update internal state."""
        self.target = target
        self.joint_angles = target  # Pretend we reached the target.
        return f"moving to {target}"

    def start_background_planner(self):
        """Threads inside the object are fine; RPC will just call into them."""
        # Launch your own planner thread here; omitted for brevity.
        return "planner started"


if __name__ == "__main__":
    # Wrap the robot with a one-line RPC server. The server runs in a background thread by default.
    robot = Robot()
    server = RPCServer(robot, port=6000)
    server.start()
```

### Client

```python
from commlink import RPCClient

# Connect to the robot and use it like it's local (up to anything pickle-able)
robot = RPCClient("localhost", port=6000)

print(robot.name)  # "robot_arm"
robot.name = "something_else"
print(robot.name)  # "something_else"

robot.move_to(np.ones(7))
print(robot.joint_angles)

# Kick off threaded work that lives inside the remote object.
print(robot.start_background_planner())

# When you're finished, politely stop the remote server.
robot.stop_server()
```

### RPC capabilities

* **Transparent calls** – Functions and methods execute remotely with arbitrary pickle-able arguments and return values.
* **Attribute access** – Reading or setting attributes forwards the operation to the remote object.
* **Drop-in adoption** – Wrap any pre-existing object with `RPCServer(obj, ...)` and obtain a live proxy by instantiating
  `RPCClient(host, port)`.
* **Thread-friendly** – `RPCServer` can run in a background thread, and the wrapped object can manage its own worker threads or
  loops without special handling.

## Publisher/subscriber helpers

If you also need broadcast-style messaging (images, poses, strings), Commlink ships with simple ZeroMQ publishers and subscribers:

```python
import numpy as np
from commlink import Publisher, Subscriber

pub = Publisher("*", port=5555)
sub = Subscriber("localhost", port=5555, topics=["rgb_image", "depth_image", "camera_pose", "info"])

# Publish rich data using dict-style access.
pub["rgb_image"] = np.random.randn(3, 224, 224)
pub["depth_image"] = np.random.randn(1, 224, 224)
pub["camera_pose"] = np.random.randn(4, 4)
pub["info"] = "helloworld"

# Receive them on the subscriber side.
print(sub["rgb_image"].shape)
print(sub["depth_image"].shape)
print(sub["camera_pose"].shape)
print(sub["info"])
```

## Development

Run the automated test suite with:

```bash
pytest
```

## License

Commlink is distributed under the terms of the [MIT License](./LICENSE).
