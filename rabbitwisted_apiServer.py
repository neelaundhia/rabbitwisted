import json
import pika

from twisted.internet import reactor
from twisted.application import service
from twisted.internet.task import deferLater
from twisted.internet import defer

from tendril.asynchronous.services.mq import PikaService
from tendril.asynchronous.services.mq import default_pika_parameters

from tendril.asynchronous.utils.logger import TwistedLoggerMixin

_application_name = "api-server"
application = service.Application(_application_name)

ps = PikaService(default_pika_parameters)
ps.setServiceParent(application)


class rabbitwisted(service.Service, TwistedLoggerMixin):
    def __init__(self):
        super(rabbitwisted, self).__init__()
        self.amqp = None

    def startService(self):
        amqp_service = self.parent.getServiceNamed("amqp")  # pylint: disable=E1111,E1121
        self.amqp = amqp_service.getFactory()
        self.amqp.read_messages("i4.apihost.topic", "request.#", self.handle)

    # def write(self, msg_reshaped):
    #     return self.amqp.send_message("storage.topic", 'influxdb', msg_reshaped)

    def echo(self, request):
        response = request.body
        return defer.succeed(response)

    @defer.inlineCallbacks
    def handle(self, request):
        # request_dict = json.loads(request.body)
        try:
            # self.log.info(request.method.routing_key)
            # self.log.info(request.properties.reply_to)
            # self.log.info(request.properties.correlation_id)

            ep = request.method.routing_key.split(".")[1]
            if hasattr(self, ep) and callable(getattr(self, ep)):
                response = yield getattr(self, ep)(request)
                yield self.respond(request, response)
            else:
                pass
            # yield self.write(msg_reshaped)
            # yield deferLater(reactor, 0.05, lambda: None)
            return defer.succeed(True)
        except Exception as err:
            self.log.info(err)
            return defer.fail(err)

    @defer.inlineCallbacks
    def respond(self, request, response):
        pass


ts = rabbitwisted()
ts.setServiceParent(application)
