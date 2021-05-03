import json
import arrow

from twisted.application import service
from twisted.internet import defer

from tendril.asynchronous.services.mq import PikaService
from tendril.asynchronous.services.mq import default_pika_parameters

from tendril.asynchronous.utils.logger import TwistedLoggerMixin

_application_name = "storage-engine"
application = service.Application(_application_name)

ps = PikaService(default_pika_parameters())
ps.setServiceParent(application)


class rabbitwisted(service.Service, TwistedLoggerMixin):
    def __init__(self):
        super(rabbitwisted, self).__init__()
        self.amqp = None

    def startService(self):
        amqp_service = self.parent.getServiceNamed("amqp")  # pylint: disable=E1111,E1121
        self.amqp = amqp_service.getFactory()
        self.amqp.read_messages("i4.topic", "monitoring.#", self.reshape)

    def write(self, msg_reshaped):
        return self.amqp.send_message(exchange="storage.topic", routing_key='influxdb', message=msg_reshaped)

    @defer.inlineCallbacks
    def reshape(self, msg):

        received_dict = json.loads(msg.body)
        msg_reshaped = ""

        #daqEngine Message
        if set(received_dict.keys()) == set(['equipmentName', 'tagName', 'tagDataType', 'tagValue', 'tagTimestamp']):
            local_received_datetime = arrow.get(received_dict['tagTimestamp'])
            utc_received_datetime = local_received_datetime.to('UTC')

            #operationalStatus
            if received_dict['tagDataType'] == "operationalStatus":
                if received_dict['tagValue'] == "offline":
                    tag_value = 0
                elif received_dict['tagValue'] == "emergency_signal":
                    tag_value = 1
                elif received_dict['tagValue'] == "fault_alarm":
                    tag_value = 2
                elif received_dict['tagValue'] == "idle":
                    tag_value = 3
                elif received_dict['tagValue'] == "operational_manual":
                    tag_value = 4
                elif received_dict['tagValue'] == "operational_auto":
                    tag_value = 5
                else:
                    self.log.info("Malformed tagValue for operationalStatus tag.")
                msg_reshaped = '{tagName},' \
                               'equipmentName={equipmentName},' \
                               'tagName={tagName},' \
                               'tagDataType={tagDataType} ' \
                               'tagValue={tagValue} ' \
                               '{tagTimestamp}'.format(equipmentName=received_dict['equipmentName'],
                                                       tagName=received_dict['tagName'].replace(' ', ''),
                                                       tagDataType=received_dict['tagDataType'],
                                                       tagValue=tag_value,
                                                       tagTimestamp=int(
                                                            utc_received_datetime.timestamp() * 1000 * 1000 * 1000))
                
            #boolean
            if received_dict['tagDataType'] == "boolean":
                msg_reshaped = '{tagName},' \
                               'equipmentName={equipmentName},' \
                               'tagName={tagName},' \
                               'tagDataType={tagDataType} ' \
                               'tagValue={tagValue} ' \
                               '{tagTimestamp}'.format(equipmentName=received_dict['equipmentName'],
                                                       tagName=received_dict['tagName'].replace(' ', ''),
                                                       tagDataType=received_dict['tagDataType'],
                                                       tagValue=received_dict['tagValue'],
                                                       tagTimestamp=int(
                                                           utc_received_datetime.timestamp() * 1000 * 1000 * 1000))

            #integer or decimal
            if received_dict['tagDataType'] == "integer" or received_dict['tagDataType'] == "decimal":
                msg_reshaped = '{tagName},' \
                               'equipmentName={equipmentName},' \
                               'tagName={tagName},' \
                               'tagDataType={tagDataType} ' \
                               'tagValue={tagValue} ' \
                               '{tagTimestamp}'.format(equipmentName=received_dict['equipmentName'],
                                                       tagName=received_dict['tagName'].replace(' ', ''),
                                                       tagDataType=received_dict['tagDataType'],
                                                       tagValue=received_dict['tagValue'],
                                                       tagTimestamp=int(
                                                           utc_received_datetime.timestamp() * 1000 * 1000 * 1000))
            # string
            if received_dict['tagDataType'] == "string":
                msg_reshaped = '{tagName},' \
                               'equipmentName={equipmentName},' \
                               'tagName={tagName},' \
                               'tagDataType={tagDataType} ' \
                               'tagValue=\"{tagValue}\" ' \
                               '{tagTimestamp}'.format(equipmentName=received_dict['equipmentName'],
                                                       tagName=received_dict['tagName'].replace(' ', ''),
                                                       tagDataType=received_dict['tagDataType'],
                                                       tagValue=received_dict['tagValue'],
                                                       tagTimestamp=int(
                                                           utc_received_datetime.timestamp() * 1000 * 1000 * 1000))
        if msg_reshaped != "":
            self.log.info(msg_reshaped)
            yield self.write(msg_reshaped)
            # yield defer.succeed(True)


ts = rabbitwisted()
ts.setServiceParent(application)
