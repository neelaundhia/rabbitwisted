

import json

from twisted.internet import reactor
from twisted.application import service
from twisted.internet.task import deferLater
from twisted.internet.defer import inlineCallbacks

from tendril.asynchronous.services.mq import PikaService
from tendril.asynchronous.services.mq import default_pika_parameters

from tendril.asynchronous.utils.logger import TwistedLoggerMixin

_application_name = "throughput-test"
application = service.Application(_application_name)

ps = PikaService(default_pika_parameters())
ps.setServiceParent(application)


class ThroughputTest(service.Service, TwistedLoggerMixin):
    def __init__(self):
        super(ThroughputTest, self).__init__()
        self.amqp = None
        self._write_index = 0
        self._read_index = 0

    def startService(self):
        amqp_service = self.parent.getServiceNamed("amqp")  # pylint: disable=E1111,E1121
        self.amqp = amqp_service.getFactory()
        self.amqp.read_messages("testing.topic", "throughput", self.read)
        deferLater(reactor, 1, self.dataGenerator)

    @inlineCallbacks
    def dataGenerator(self):
        self._write_index = 0
        self._read_index = 0
        while True:
            yield self.write()
            yield deferLater(reactor, 0.00001, lambda: None)

    def write(self):
        msg = json.dumps({'index': self._write_index})
        self._write_index += 1
        return self.amqp.send_message("testing.topic", 'throughput', msg)

    def read(self, msg):
        ri = json.loads(msg.body)['index']
        if not ri == self._read_index:
            self.log.error("Expecting {exp}, Got {index}", index=ri, exp=self._read_index)
        else:
            self.log.info("Publishing {index}, WQ Length: {wql} Current RI {ri}",
                          index=self._write_index, ri=self._read_index, wql=len(self.amqp.queued_messages))
        self._read_index += 1


ts = ThroughputTest()
ts.setServiceParent(application)
