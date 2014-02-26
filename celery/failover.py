# -*- coding: utf-8 -*-
"""
    celery.failover
    ~~~~~~~~~~~

    Mutex for services.

"""
import time
import socket
import amqp

from .utils.log import get_logger

logger = get_logger(__name__)
debug, info, error, warning = (logger.debug, logger.info,
                               logger.error, logger.warning)


class ServiceFailover(object):
    def __init__(self, app, name):
        self.app = app
        self.name = name

        self._conn = None

    @property
    def connection(self):
        if self._conn == None:
            self._conn = self.app.connection()
        return self._conn

    def wait_master(self, timeout=1):
        debug('Waiting for beat token')
        while not self.is_master():
            debug("Checking for failover in %s seconds", timeout)
            time.sleep(timeout)
        debug('Beat token received')

    def is_master(self):
        try:
            self._get_mutex()
            return True
        except (socket.error, amqp.exceptions.ConnectionError):
            warning('Connection lost!')
        except amqp.exceptions.ChannelError:
            debug("Not the beat master")
        return False

    def _get_mutex(self):
        return self.connection.SimpleQueue(
                '%s.mutex' % self.name,
                queue_opts={'exclusive': True})


