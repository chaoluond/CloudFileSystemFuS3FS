#!/usr/bin/env python
import BaseHTTPServer
import logging
import json
import urllib2


class SNS_HTTPServer(BaseHTTPServer.HTTPServer):
    """ HTTP Server to receive SNS notifications via HTTP """
    def set_fs(self, fs):
        self.fs = fs

class SNS_HTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """ HTTP Request Handler to receive SNS notifications via HTTP """
    def do_POST(self):
        if self.path != self.server.fs.http_listen_path:
            self.send_response(404)
            return

        content_len = int(self.headers.getheader('content-length'))
        post_body = self.rfile.read(content_len)

        message_type = self.headers.getheader('x-amz-sns-message-type')
        message_content = json.loads(post_body)

        # Check SNS signature, I was not able to use boto for this...

        url = message_content['SigningCertURL']
        if not hasattr(self, 'certificate_url') or self.certificate_url != url:
            logging.debug('downloading certificate')
            self.certificate_url = url
            self.certificate = urllib2.urlopen(url).read()

        signature_version = message_content['SignatureVersion']
        if signature_version != '1':
            logger.debug('unknown signature version')
            self.send_response(404)
            return

        signature = message_content['Signature']

        del message_content['SigningCertURL']
        del message_content['SignatureVersion']
        del message_content['Signature']
        if 'UnsubscribeURL' in message_content:
            del message_content['UnsubscribeURL']
        string_to_sign = '\n'.join(list(itertools.chain.from_iterable(
                    [ (k, message_content[k]) for k in sorted(message_content.iterkeys()) ]
                    ))) + '\n'

        cert = M2Crypto.X509.load_cert_string(self.certificate)
        pub_key = cert.get_pubkey().get_rsa()
        verify_evp = M2Crypto.EVP.PKey()
        verify_evp.assign_rsa(pub_key)
        verify_evp.reset_context(md='sha1')
        verify_evp.verify_init()
        verify_evp.verify_update(string_to_sign.encode('ascii'))

        if verify_evp.verify_final(signature.decode('base64')):
            self.send_response(200)
            if message_type== 'Notification':
                message = message_content['Message']
                logging.debug('message = %s' % message)
                self.server.fs.process_message(message)
            elif message_type == 'SubscriptionConfirmation':
                token = message_content['Token']
                response = self.server.fs.sns.confirm_subscription(self.server.fs.sns_topic_arn, token)
                self.server.fs.http_subscription = response['ConfirmSubscriptionResponse']['ConfirmSubscriptionResult']['SubscriptionArn']
                logging.debug('SNS HTTP subscription = %s' % self.server.fs.http_subscription)
            else:
                logging.debug('unknown message type')
            return
        else:
            logging.debug('wrong signature')

        # If nothing better, return 404
        self.send_response(404)

    def do_GET(self):
        logging.debug('http get')
        self.send_response(404)
    def do_HEAD(self):
        logging.debug('http head')
        self.send_response(404)
