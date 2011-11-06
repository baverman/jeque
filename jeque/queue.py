import time
from uuid import uuid4 as uuid
from collections import defaultdict

class Message(object):
    __slots__ = ('message', 'priority', 'group', 'enterdate', 'get')

    def __init__(self, message, priority, group, enterdate, get):
        self.message = message
        self.priority = priority
        self.group = group
        self.enterdate = enterdate
        self.get = get


class Queue(object):
    def __init__(self):
        self.messages = dict()
        self.pmessages = dict()

        self.message_waiters = []
        self.ack_waiters = defaultdict(list)

    def check_for_new_messages(self):
        if not self.message_waiters:
            return

        session_id, cb = self.message_waiters[0]
        msg_id, message = self.get(session_id, True)
        if msg_id:
            self.message_waiters.pop(0)
            cb(msg_id, message)

    def put(self, message, message_id, priority, group):
        message_id = message_id or uuid()
        priority = priority or 50
        enterdate = time.time()

        if message_id not in self.messages:
            self.messages[message_id] = Message(message, priority, group, enterdate, False)
        else:
            if priority > self.messages[message_id].priority:
                self.messages[message_id].priority = priority

            if enterdate < self.messages[message_id].enterdate:
                self.messages[message_id].enterdate = enterdate

        self.check_for_new_messages()
        return message_id

    def reput(self, message_id, delay=0):
        if message_id in self.messages:
            self.messages[message_id].enterdate = time.time() + delay
            self.messages[message_id].get = False

            try:
                del self.pmessages[message_id]
            except:
                pass

            self.check_for_new_messages()

    def get(self, session_id, multi=False):
        if not multi:
            groups = None
            self.clear_session(session_id)
        else:
            try:
                groups = set(self.pmessages.itervalues())
            except KeyError:
                groups = None

        message_id = None
        now = time.time()
        for k, v in self.messages.iteritems():
            if v.get is not False or v.enterdate > now or (
                    v.group is not None and groups and v.group in groups): continue

            priority = (now - v.enterdate) * v.priority

            if message_id is None or priority > max_prior:
                max_prior = priority
                message_id = k
                message = v

        if message_id:
            self.pmessages[message_id] = message.group
            self.messages[message_id].get = session_id
            return message_id, self.messages[message_id].message
        else:
            return None, None

    def wait_for_message(self, session_id, cb):
        self.message_waiters.append((session_id, cb))

    def wait_for_ack(self, session_id, msg_id, cb):
        self.ack_waiters[msg_id].append((session_id, cb))

    def __len__(self):
        return len(self.messages)

    def ack(self, message_id, result=None):
        if message_id in self.pmessages:
            del self.pmessages[message_id]
            del self.messages[message_id]

            if message_id in self.ack_waiters:
                for _, cb in self.ack_waiters[message_id]:
                    try:
                        cb(result)
                    except:
                        import traceback
                        traceback.print_exc()

                del self.ack_waiters[message_id]

            self.check_for_new_messages()

    def session_done(self, session_id):
        self.message_waiters[:] = [r for r in self.message_waiters if r[0] != session_id]
        for mid, ack_waiters in self.ack_waiters.iteritems():
            ack_waiters[:] = [r for r in ack_waiters if r[0] != session_id]

        self.clear_session(session_id)

    def clear_session(self, session_id):
        messages_cleared = False
        for mid in list(self.pmessages):
            if self.messages[mid].get == session_id:
                del self.pmessages[mid]
                self.messages[mid].get = False
                messages_cleared = True

        if messages_cleared:
            self.check_for_new_messages()

    def all_messages(self):
        for k, v in self.messages.iteritems():
            yield k, v