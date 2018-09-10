#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from email.message import EmailMessage
from smtplib import SMTP

logger = logging.getLogger('mundone')
logger.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(
    logging.Formatter(
        fmt='%(asctime)s: %(message)s',
        datefmt='%y-%m-%d %H:%M:%S'
    )
)
logger.addHandler(_ch)


class Notification(object):
    def __init__(self, smtp_host, smtp_user, smtp_port=475):
        self.host = smtp_host
        self.user = smtp_user
        self.port = smtp_port

    def send(self, subject, content, to_addrs=None):
        msg = EmailMessage()
        msg.set_content(content)

        msg['Subject'] = subject
        msg['From'] = self.user

        if to_addrs and isinstance(to_addrs, (list, tuple)):
            to_addrs = set(to_addrs)
            msg['To'] = ','.join(to_addrs)
        else:
            msg['To'] = [self.user]

        with SMTP(self.host, port=self.port) as s:
            s.send_message(msg)
