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
from commlink import RPCServer


class TemperatureController:
    def __init__(self):
        self.target_celsius = 20

    def get_reading(self):
        """Pretend to talk to a sensor and return the current reading."""
        return self.target_celsius

    def setpoint(self, value):
        self.target_celsius = value
        return f"Set target temperature to {value}°C"


if __name__ == "__main__":
    # Wrap the object with a one-line RPC server. The server runs in a background thread by default.
    server = RPCServer(TemperatureController(), port=6000)
    server.start()
    server.thread.join()  # Optional: keep the process alive while the server thread runs.
```

### Client

```python
from commlink import RPCClient

# Instantiate the remote object locally – attribute access, setters, and method calls all proxy to the server.
controller = RPCClient("localhost", port=6000)

print(controller.get_reading())  # Call remote methods with any pickle-able arguments or return values.
print(controller.setpoint(25))

controller.target_celsius = 18  # Mutate attributes on the remote instance.
print(controller.target_celsius)

# When you're finished, politely stop the remote server.
controller.stop_server()
```

### RPC capabilities

* **Transparent calls** – Functions and methods execute remotely with arbitrary pickle-able arguments and return values.
* **Attribute access** – Reading or setting attributes forwards the operation to the remote object.
* **Drop-in adoption** – Wrap any pre-existing object with `RPCServer(obj, ...)` and obtain a live proxy by instantiating
  `RPCClient(host, port)`.
* **Threaded by default** – `RPCServer` starts in a background thread so your host application can continue doing work or cleanly
  manage lifecycle events.

## Publisher/subscriber helpers

If you also need broadcast-style messaging, Commlink ships with simple ZeroMQ publishers and subscribers:

```python
from commlink import Publisher, Subscriber

publisher = Publisher("*", port=5555)
subscriber = Subscriber("localhost", port=5555, topics=["updates"])

publisher.publish("updates", {"message": "Hello, world!"})
print(subscriber.get())
```

## Development

Run the automated test suite with:

```bash
pytest
```

## License

Commlink is distributed under the terms of the [MIT License](./LICENSE).
