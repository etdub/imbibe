import collections
import ujson
import zmq

class Imbibe(object):
    def __init__(self, servers):
        if not isinstance(servers, list):
            self.servers = [servers]
        else:
            self.servers = servers

        self.context = zmq.Context()

        self.sub_socket = self.context.socket(zmq.SUB)
        for server in servers:
            print "Connect to {0}".format(server)
            self.sub_socket.connect('tcp://{0}'.format(server))
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, '')

        self.poller = zmq.Poller()
        self.poller.register(self.sub_socket, zmq.POLLIN)

        self.counters = collections.defaultdict(dict)

    def imbibe(self):
        """ Yield metrics """
        self.running = True
        while self.running:
            socks = dict(self.poller.poll(1000))
            if self.sub_socket in socks and socks[self.sub_socket] == zmq.POLLIN:
                metrics = ujson.loads(self.sub_socket.recv())
                for m in metrics:
                    yield self.__process_metric(m)

    def stop(self):
        self.running = False

    def __process_metric(self, metric):
        hostname, app_name, metric_name, metric_type, value, metric_time = metric
        value = float(value)
        metric_time = float(metric_time)
        ret_val = value
        if metric_type == 'COUNTER':
            # Calculate a rate
            full_name = '{0}/{1}'.format(app_name, metric_name)
            if full_name in self.counters[hostname]:
                last_val, last_ts = self.counters[hostname][full_name]
                if value > last_val:
                    ret_val = (value - last_val) / (metric_time - last_ts)
                else:
                    ret_val = None
            else:
                ret_val = None
            self.counters[hostname][full_name] = (value, metric_time)
        return (hostname, app_name, metric_name, ret_val, metric_time)

if __name__=='__main__':
    i = Imbibe(['127.0.0.1:5002'])
    try:
        for m in i.imbibe():
            print m
    except Exception, e:
        print "Exception... stop imbibing - {0}".format(e)
        i.stop()
