from pulsar import Client, Message, MessageId


class Pulsar:
    def __init__(self, domain, port, *, topic):
        self.connection: Client = Client(f"pulsar://{domain}:{port}")
        self.topic: str = topic

    def producer(self, **kwargs):
        return Pub(self, **kwargs)

    def consumer(self, group, **kwargs):
        return Sub(self, group=group, **kwargs)

    def disconnect(self):
        self.connection.close()


class Pub:
    def __init__(self, pulsar: Pulsar, **kwargs):
        self.pub = pulsar.connection.create_producer(pulsar.topic, **kwargs)

    def send(self, message: str, **kwargs) -> MessageId:
        return self.pub.send(message.encode(), **kwargs)

    def disconnect(self):
        self.pub.close()


class Sub:
    def __init__(self, pulsar: Pulsar, *, group, **kwargs):
        self.sub = pulsar.connection.subscribe(pulsar.topic, group, **kwargs)

    def get_next(self) -> Message:
        return self.sub.receive()

    def acknowledge(self, msg: Message | MessageId):
        self.sub.acknowledge(msg)

    def reset(self, message_id: MessageId = None):
        if message_id:
            self.sub.seek(message_id)
        else:
            self.sub.redeliver_unacknowledged_messages()

    def get_last(self) -> MessageId:
        return self.sub.get_last_message_id()

    def disconnect(self):
        if self.sub.is_connected():
            self.sub.unsubscribe()
        self.sub.close()
