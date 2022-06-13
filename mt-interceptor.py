import logging
import uuid
import requests
import pika
import pickle
import chardet
from datetime import datetime
from jasmin.protocols.smpp.operations import DeliverSM
from jasmin.managers.content import DeliverSmContent
from jasmin.routing.Routables import RoutableDeliverSm
from jasmin.routing.jasminApi import SmppServerSystemIdConnector
from jasmin.managers.content import DLRContentForSmpps
from jasmin.routing.jasminApi import Connector
from smpp.pdu.pdu_types import (EsmClass, EsmClassMode, EsmClassType, EsmClassGsmFeatures,
                                              MoreMessagesToSend, MessageState, AddrTon, AddrNpi)


#routable.pdu.params['source_addr'] = '5115105555'
#routable.lockPduParam('source_addr')

RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/%2F'
log = logging.getLogger("jasmin-interceptor-pb")

submit_sm = routable.pdu
log.info("routable.user %s", routable.user)
log.info("routable.user.uid %s", routable.user.uid)
log.info("routable.pdu %s", routable.pdu)
log.info("routable.pdu.id %s", routable.pdu.id)
log.info("command_id name: %s", routable.pdu.id.name)
log.info("command_id value: %s", routable.pdu.id.value)
log.info("routable.pdu.status %s", routable.pdu.status)
log.info("status name: %s", routable.pdu.status.name)
log.info("status value: %s", routable.pdu.status.value)
log.info("routable.pdu.seqNum %s", routable.pdu.seqNum)

origen = routable.pdu.params['source_addr']
destino = routable.pdu.params['destination_addr']
content = routable.pdu.params['short_message']

to_fravatel = False
to_movistar = False
to_intico = False

#before decode: type destino <class 'bytes'>
destino = destino.decode('UTF-8')
#after decode: type destino <class 'str'>
log.info("destino %s", destino)

#Homologar numero B
if len(destino) == 8 and destino[0] == '1':
    destino = '51' + destino

if len(destino) == 9 and destino[0] == '9':
    destino = '51' + destino

routable.pdu.params['destination_addr'] = destino

log.info("destino %s", destino[2:])
params = {'numero':destino[2:]}
url_porta = "http://172.16.1.98/porta2.php"
r_portado = requests.get(url_porta, params)
numero_portado = r_portado.text
log.info("numero_portado %s", numero_portado)

if numero_portado[0:2] == '61':
    to_fravatel = True
    routable.addTag(5161)
elif numero_portado[0:2] == '22':
    to_movistar = True
    routable.addTag(5122)
else:
    to_intico = True
    routable.addTag(5199)

if to_fravatel == True:
    msgid = str(uuid.uuid4())
    extra['message_id'] = msgid
    log.info("message_id %s", msgid)

    content = routable.pdu.params['short_message']
    coding = routable.pdu.params['data_coding']
    data_coding = routable.pdu.params['data_coding'].schemeData
    log.info("coding %s", coding)
    log.info("data_coding.schemeData %s", data_coding)
    log.info("data_coding.schemeData name %s", data_coding.name)
    log.info("data_coding.schemeData value %s", data_coding.value)

    smpp_texto = content
    if data_coding.name == "UCS2":
        log.info("decoding smpp_texto")
        smpp_texto = content.decode('utf_16_be')
    log.info("smpp_texto %s", smpp_texto)

    playsms_texto = "@%s %s" % (destino[2:], smpp_texto)
    log.info("playsms_texto %s", playsms_texto)

    baseParams = {'id':msgid, 'from':origen, 'to':destino, 'content':playsms_texto, 'origin-connector':'TDP'}
    #url = "http://172.17.0.2/plugin/gateway/jasmin/callback.php"
    url = "http://10.11.20.5/playsms/plugin/gateway/jasmin/callback.php"
    res = requests.post(url, data = baseParams)

    smpp_status = 0
    http_status = 200
    log.info("Returning smpp_status %s http_status %s", smpp_status, http_status)

    registered_delivery = routable.pdu.params['registered_delivery']
    flag_dlr = registered_delivery.receipt
    log.info("registered_delivery %s", registered_delivery)
    log.info("flag_dlr %s", flag_dlr)
    #flag_dlr RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED
    log.info("flag dlr name: %s", flag_dlr.name)
    log.info("flag dlr value: %s", flag_dlr.value)

    if flag_dlr.name == 'SMSC_DELIVERY_RECEIPT_REQUESTED':
        err = 0
        dlvrd = 1
        sm_message_stat = 'DELIVRD'
        sub_date = datetime.now()
        deliver_sm = DeliverSM(
            submit_sm.seqNum,
            service_type=submit_sm.params['service_type'],
            source_addr_ton=submit_sm.params['source_addr_ton'],
            source_addr_npi=submit_sm.params['source_addr_npi'],
            #source_addr=submit_sm.params['source_addr'],
            source_addr=submit_sm.params['destination_addr'],
            dest_addr_ton=submit_sm.params['dest_addr_ton'],
            dest_addr_npi=submit_sm.params['dest_addr_npi'],
            #destination_addr=submit_sm.params['destination_addr'],
            destination_addr=submit_sm.params['source_addr'],
            esm_class=submit_sm.params['esm_class'],
            protocol_id=submit_sm.params['protocol_id'],
            priority_flag=submit_sm.params['priority_flag'],
            #registered_delivery=submit_sm.params['registered_delivery'],
            replace_if_present_flag=submit_sm.params['replace_if_present_flag'],
            #data_coding=submit_sm.params['data_coding'],
            #short_message=submit_sm.params['short_message'],
            short_message = bytes(r"id:%s sub:%03d dlvrd:%03d submit date:%s done date:%s stat:%s err:%03d text: %s" % (
                msgid,
                1,
                dlvrd,
                sub_date.strftime("%y%m%d%H%M"),
                datetime.now().strftime("%y%m%d%H%M"),
                sm_message_stat,
                err,
                smpp_texto[:32],
            ), 'utf-8'),
            sm_default_msg_id=submit_sm.params['sm_default_msg_id'])

        log.info("Prepared a new deliver_sm: %s", deliver_sm)

        _routable = RoutableDeliverSm(deliver_sm, Connector(routable.user.uid))
        content = DeliverSmContent(_routable, routable.user.uid, pickleProtocol=pickle.HIGHEST_PROTOCOL)
        routing_key = 'deliver.sm.%s' % routable.user.uid
        log.info('routing_key: %s' % routing_key)

        # Connecto RabbitMQ and publish deliver_sm
        log.info('Init pika and publish..')
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        log.info('RabbitMQ channel ready, publishing now msgid %s ...', content.properties['message-id'])
        channel.basic_publish(
            'messaging',
            routing_key,
            content.body,
            pika.BasicProperties(
            message_id=content.properties['message-id'],
            headers=content.properties['headers'])
        )
        log.info('Published deliver_sm to %s', routing_key)

        # Explicitly return ESME_ROK
        # This will bypass Jasmin's router as
        # described in gh #589
        smpp_status = 0
        http_status = 200
        log.info("Returning smpp_status %s http_status %s", smpp_status, http_status)

        # Send back a DLR
        log.info("Send back a DLR")
        status = 'DELIVRD'
        dlr_content = DLRContentForSmpps(
            status,
            extra['message_id'],
            routable.user.uid,
            submit_sm.params['source_addr'],
            submit_sm.params['destination_addr'],
            datetime.now(),
            str(submit_sm.params['dest_addr_ton']),
            str(submit_sm.params['dest_addr_npi']),
            str(submit_sm.params['source_addr_ton']),
            str(submit_sm.params['source_addr_npi']),
        )

        # Publish DLR
        routing_key = 'dlr_thrower.smpps'
        channel.basic_publish(
            'messaging',
            routing_key,
            dlr_content.body,
            pika.BasicProperties(
                message_id=dlr_content.properties['message-id'],
                headers=dlr_content.properties['headers']))

        log.info("Published DLRContentForSmpps[%s] to [%s], having status = %s",
                extra['message_id'], routing_key, status)

        connection.close()

