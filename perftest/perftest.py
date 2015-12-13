import os
import time

from rabcorelib.address import Action, Domain
from rabcorelib.pyriffle import FabricSession
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred


class BackendSession(FabricSession):
    def __init__(self, config):
        super(BackendSession, self).__init__(config)

        self.app_domain = Domain(self.domain).pop().pop()
        self.container_domain = self.app_domain + "Container"
        self.storage_domain = self.app_domain + "Storage"

    @inlineCallbacks
    def onJoin(self, details):
        yield self.register(self.test, "test")

    @inlineCallbacks
    def test(self):
        timings = dict()
        yield self.testContainer(timings)
        yield self.testStorage(timings)
        returnValue(timings)

    @inlineCallbacks
    def testContainer(self, timings):
        cmd = str(self.container_domain + Action("list"))
        start = time.time()
        yield self.absCall(cmd)
        timings[cmd] = time.time() - start

    @inlineCallbacks
    def testStorage(self, timings):
        # Test entry that we will store and read back.
        entry = {'time': time.time()}

        insert_one = str(self.storage_domain + Action("collection/insert_one"))
        start = time.time()
        yield self.absCall(insert_one, "test", entry)
        timings[insert_one] = time.time() - start

        find_one = str(self.storage_domain + Action("collection/find_one"))
        start = time.time()
        yield self.absCall(find_one, "test", entry)
        timings[find_one] = time.time() - start

        delete_one = str(self.storage_domain + Action("collection/delete_one"))
        start = time.time()
        yield self.absCall(delete_one, "test", entry)
        timings[delete_one] = time.time() - start


if __name__ == "__main__":
    ws_url = os.environ['WS_URL']
    domain = os.environ['DOMAIN']

    BackendSession.start(unicode(ws_url), unicode(domain), start_reactor=True)
