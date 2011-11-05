import time
from uuid import uuid4 as uuid

class Queue(object):
    def __init__(self):
        self.messages = dict()
        self.session_ids = dict()
        self.sleep = time.sleep

        self.message_waiters = []
        self.ack_waiters = {}

    def check_for_new_messages(self):
        if not self.message_waiters:
            return

        session_id, cb = self.message_waiters[0]
        msg_id, message = self.get(session_id, True)
        if msg_id:
            self.message_waiters.pop(0)
            cb(msg_id, message)

    def put(self, message, message_id, priority):
        message_id = message_id or uuid()
        priority = priority or 50
        enterdate = time.time()

        if message_id not in self.messages:
            self.messages[message_id] = {
                'message': message, 'priority':priority, 'enterdate':enterdate, 'get':False}
        else:
            if priority > self.messages[message_id]['priority']:
                self.messages[message_id]['priority'] = priority

            if enterdate < self.messages[message_id]['enterdate']:
                self.messages[message_id]['enterdate'] = enterdate

        self.check_for_new_messages()
        return message_id

    def reput(self, session_id, message_id, delay=0):
        if message_id in self.messages:
            self.messages[message_id]['enterdate'] = time.time() + delay
            self.messages[message_id]['get'] = False

            if session_id in self.session_ids:
                if message_id in self.session_ids[session_id]:
                    del self.session_ids[session_id][message_id]

            self.check_for_new_messages()

    def get(self, session_id, multi=False):
        if not multi:
            self.clear_session(session_id)

        message_id = None
        now = time.time()
        for k, v in self.messages.iteritems():
            if v['get'] or v['enterdate'] > now: continue

            priority = ( now - v['enterdate'] ) * v['priority']

            if message_id is None or priority > max_prior:
                max_prior = priority
                message_id = k

        if message_id:
            self.session_ids.setdefault(session_id, dict())[message_id] = 1
            self.messages[message_id]['get'] = True
            return message_id, self.messages[message_id]['message']
        else:
            return None, None

    def wait_for_message(self, session_id, cb):
        self.message_waiters.append((session_id, cb))

    def wait_for_ack(self, msg_id, cb):
        self.ack_waiters.setdefault(msg_id, []).append(cb)

    def __len__(self):
        return len(self.messages)

    def ack(self, session_id, message_id, result=None):
        if session_id in self.session_ids:
            if message_id in self.session_ids[session_id]:
                del self.session_ids[session_id][message_id]
                del self.messages[message_id]

                if message_id in self.ack_waiters:
                    for cb in self.ack_waiters[message_id]:
                        cb(result)

                    del self.ack_waiters[message_id]

    def clear_session(self, session_id):
        if session_id in self.session_ids:
            for id in self.session_ids.get(session_id, dict()).keys():
                self.messages[id]['get'] = False

            del self.session_ids[session_id]
            self.check_for_new_messages()

    def all_messages(self):
        for k, v in self.messages.iteritems():
            yield k, v