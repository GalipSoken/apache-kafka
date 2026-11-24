"""Broker gerektirmeyen birim testleri için sahte Message / Producer."""


class FakeMsg:
    def __init__(self, value, topic="islemler.json", partition=0, offset=0, key=None, err=None, ts=0):
        self._v, self._t, self._p, self._o, self._k, self._e, self._ts = value, topic, partition, offset, key, err, ts

    def value(self): return self._v
    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def key(self): return self._k
    def error(self): return self._e
    def timestamp(self): return (1, self._ts)
    def headers(self): return None


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.sent.append((topic, key, value, headers))

    def poll(self, _t=0): return 0
    def flush(self, _t=0): return 0
