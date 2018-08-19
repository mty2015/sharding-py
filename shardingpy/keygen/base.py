from datetime import datetime


class DefaultKeyGenerator(object):
    """
    Use snowflake algorithm. Length is 64 bit.

    1bit   sign bit.
    41bits timestamp offset from 2016.11.01(Sharding-Sphere distributed primary key published data) to now.
    10bits worker process id.
    12bits auto increment offset in one mills
    """
    EPOCH = int(datetime(2016, 11, 1).timestamp() * 1000)
    SEQUENCE_BITS = 12
    WORKER_ID_BITS = 10
    SEQUENCE_MASK = (1 << SEQUENCE_BITS) - 1
    WORKER_ID_LEFT_SHIFT_BITS = SEQUENCE_BITS
    TIMESTAMP_LEFT_SHIFT_BITS = WORKER_ID_LEFT_SHIFT_BITS + WORKER_ID_BITS
    WORKER_ID_MAX_VALUE = 1 << WORKER_ID_BITS

    worker_id = 0

    def __init__(self):
        self.sequence = 0
        self.last_time = 0

    @staticmethod
    def set_worker_id(worker_id):
        assert 0 <= worker_id < DefaultKeyGenerator.WORKER_ID_MAX_VALUE
        DefaultKeyGenerator.worker_id = worker_id

    def generate_key(self):
        current_millis = self._get_current_millis()
        assert current_millis >= self.last_time, 'Clock is moving backwards, last time is %d milliseconds, current time is %d milliseconds'
        if current_millis == self.last_time:
            self.sequence = (self.sequence + 1) & DefaultKeyGenerator.SEQUENCE_MASK
            if self.sequence == 0:
                current_millis = self._wait_next_time(current_millis)
        else:
            self.sequence = 0
        self.last_time = current_millis
        return ((current_millis - self.EPOCH) << self.TIMESTAMP_LEFT_SHIFT_BITS) | (
                self.worker_id << self.WORKER_ID_LEFT_SHIFT_BITS) | self.sequence

    def _wait_next_time(self, last_time):
        t = self._get_current_millis()
        while t <= last_time:
            t = self._get_current_millis()
        return t

    @staticmethod
    def _get_current_millis():
        return int(datetime.now().timestamp() * 1000)
