import json
import arrow

from pika import spec

from twisted.internet import defer
from twisted.internet import reactor
from twisted.application import service
from twisted.internet.task import deferLater
from twisted.internet.defer import inlineCallbacks

from tendril.asynchronous.services.mq import PikaService
from tendril.asynchronous.services.mq import default_pika_parameters

from tendril.asynchronous.utils.logger import TwistedLoggerMixin

_application_name = "api-host"
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
        self.amqp.read_messages("i4.apihost.topic", "request.#", self.handle)

    def write(self, exchange, routing_key, properties, message):
        return self.amqp.send_message(exchange=exchange, routing_key=routing_key, properties=properties, message=message)

    def echo(self, request):
        response = request.body
        return defer.succeed(response)

    @defer.inlineCallbacks
    def handle(self, request):
        # request_dict = json.loads(request.body)
        try:
            self.log.debug("Handling Request: {request}", request=request)

            if not request.properties.correlation_id:
                self.log.warn("Request has no correlation_id!")
                yield defer.fail(Exception)
                return
            if not request.properties.reply_to:
                self.log.warn("Request has no reply_to!")
                yield defer.fail(Exception)
                return

            endpoint = request.method.routing_key.split(".")[1]
            if hasattr(self, endpoint) and callable(getattr(self, endpoint)):
                response = yield getattr(self, endpoint)(request)
                yield self.respond(request, response)
            else:
                pass

            yield defer.succeed(True)

        except Exception as err:
            self.log.info(err)
            yield defer.fail(err)

    @defer.inlineCallbacks
    def respond(self, request, response):
        properties = {"correlation_id": request.properties.correlation_id}
        yield self.write(exchange="i4.apihost.topic",
                         routing_key='response.{reply_to}'.format(reply_to=request.properties.reply_to),
                         message=response, properties=properties)


ts = rabbitwisted()
ts.setServiceParent(application)
