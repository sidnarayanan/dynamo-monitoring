from time import time 
now = time()

class Replica(object):
    def __init__(self, name, size):
        self.name = name 
        self.size = size 
        self.start = -1
        self.end = now
        self.deleted = False
    def extend(self, until, deletion=False):
        if self.deleted:
            raise ValueError("Trying to modify a deleted replica %s!"%self.name)
        if self.start < 0:
            self.start = until 
        else:
            self.end = until
        if deletion:
            self.end = until 
            self.deleted = True
    def volume(self, start=-1, end=now):
        start = max(start, self.start)
        end = min(end, self.end)
        return self.size * (end - start)


class Dataset(object):
    def __init__(self, name, size=0):
        self.name = name 
        self.size = size 
        self.replicas = []
        self.accesses = []
    def add_replica(self, repl_size, timestamp, decision):
        deletion = (decision == 1)
        if not self.replicas or self.replicas[-1].deleted:
            self.replicas.append( Replica(self.name, repl_size) )
        self.replicas[-1].extend(timestamp, deletion)
    def volume(self, start, end):
        return reduce(lambda x, y: x + y.volume(start, end), self.replicas, 0) / (end - start)
    def add_access(self, ts, n):
        self.accesses.append((ts, n))
    def usage(self, start, end):
        return sum([x[1] for x in self.accesses if x[0] > start and x[0] < end])


class Site(object):
    def __init__(self, site_id, name):
        self.site_id = site_id
        self.name = name 
        self.datasets = {}
    def add_replica(self, dataset_id, repl_size, timestamp, decision, global_datasets):
        if dataset_id not in self.datasets:
            gds = global_datasets[dataset_id]
            gds_name = gds.name 
            gds_size = gds.size
            ds = Dataset(gds_name, gds_size)
            self.datasets[dataset_id] = ds
        
        ds = self.datasets[dataset_id]
        ds.add_replica(repl_size, timestamp, decision)
    def add_access(self, dataset_id, ts, n):
        try:
            self.datasets[dataset_id].add_access(ts, n)
        except KeyError:
            return
    def volumes(self, start, end):
        return dict([(k,v.volume(start, end)) for k,v in self.datasets.iteritems()])
