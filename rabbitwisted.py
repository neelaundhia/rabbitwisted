import json
import pytz
import datetime
import dateutil.parser as dp

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


class rabbitwisted(service.Service, TwistedLoggerMixin):
    def __init__(self):
        super(rabbitwisted, self).__init__()
        self.amqp = None
        self.local_pytz = pytz.timezone("Asia/Kolkata")

    def startService(self):
        amqp_service = self.parent.getServiceNamed("amqp")  # pylint: disable=E1111,E1121
        self.amqp = amqp_service.getFactory()
        self.amqp.read_messages("i4.topic", "monitoring.#", self.reshape)

    def write(self, msg_reshaped):
        return self.amqp.send_message("storage.topic", 'influxdb', msg_reshaped)

    @inlineCallbacks
    def reshape(self, msg):

        received_dict = json.loads(msg.body)

        if set(received_dict.keys()) == set(['equipmentName', 'tagName', 'tagDataType', 'tagValue', 'tagTimestamp']):
            local_received_datetime = dp.parse(received_dict['tagTimestamp'])
            utc_received_datetime = local_received_datetime.astimezone(pytz.utc)

        msg_reshaped = "{tagName},equipmentName={equipmentName} tagName={tagName} tagDataType={tagDataType}, value={tagValue} {tagTimestamp}".format(tagName=received_dict['tagName'],
                                                                                                                                                     equipmentName=received_dict['equipmentName'],
                                                                                                                                                     tagDataType=received_dict['tagDataType'],
                                                                                                                                                     tagValue=received_dict['tagValue'],
                                                                                                                                                     tagTimestamp=utc_received_datetime.isoformat())
        self.log.info(msg_reshaped)
        # yield self.write(msg_reshaped)
        yield deferLater(reactor, 0.05, lambda: None)


ts = rabbitwisted()
ts.setServiceParent(application)
