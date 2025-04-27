import concurrent
import unittest
from concurrent.futures import ThreadPoolExecutor, wait

from davidkhala.pulsar import Pulsar


class LocalTestCase(unittest.TestCase):
    topic = 'my-topic'
    pulsar = Pulsar('localhost', 6650, topic=topic)
    group = 'my-sub'
    message = 'hello world'

    def sub(self):
        sub = self.pulsar.consumer(self.group)
        msg = sub.get_next()
        self.assertEqual(self.message, msg.data().decode('utf-8'))

        sub.acknowledge(msg)
        sub.disconnect()

    def test_sub(self):
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(self.test_pub),
                executor.submit(self.sub),
            ]
        done, not_done = wait(futures)
        assert not not_done
        for _ in done:
            _.result()

    def test_pub(self):
        pub = self.pulsar.producer()
        msg_id = pub.send(self.message)
        pub.disconnect()
        return msg_id

    def test_ack_last(self):
        msg_id = self.test_pub()
        sub = self.pulsar.consumer(self.group)
        self.assertEqual(msg_id, sub.get_last())
        sub.acknowledge(msg_id)
        sub.disconnect()


if __name__ == '__main__':
    unittest.main()
