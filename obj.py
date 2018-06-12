from time import time 
now = time()
import re

class Replica(object):
    __slots__ = ['size', 'start', 'end', 'deleted']
    def __init__(self, size):
        self.size = size 
        self.start = -1
        self.end = now
        self.deleted = False
    def extend(self, until, size, deletion=False):
        if self.deleted:
            raise ValueError("Trying to modify a deleted replica!")
        self.size = max(self.size, size)
        if self.start < 0:
            self.start = until 
        else:
            self.end = until
        if deletion:
            self.end = until 
            self.deleted = True
    def volume(self, start=-1, end=now):
        start_ = max(start, self.start)
        end_ = min(end, self.end)
        if end_ <= start_:
            return 0
        v = self.size * (end_ - start_) / 1.e12
        return v 

class AccessHistory(object):
    __slots__ = ['accesses']
    def __init__(self):
        self.accesses = [] 
    def add_access(self, ts, n):
        self.accesses.append((ts, n))
    def usage(self, start=-1, end=now):
        return sum([n for ts,n in self.accesses
                      if ts > start and ts < end])


class Dataset(object):
    __slots__ = ['name', 'size', 'replicas', 'accesses', 
                 'created', 'nfiles', '_nfiles_inverse', 'tier']
    def __init__(self, name, size=0):
        self.name = name 
        self.size = size 
        self.replicas = {}
        self.accesses = {}
        self.created = 1 # needs to be filled outside of constructor
        self.nfiles = 0
        self._nfiles_inverse = 0
        self.tier = self.name.split('/')[-1] 
    def add_replica(self, node, repl_size, timestamp, decision):
        deletion = (decision == 1)
        if node not in self.replicas:
            self.replicas[node] = [Replica(repl_size)]
        elif self.replicas[node][-1].deleted:
            self.replicas[node].append( Replica(repl_size) )
        self.replicas[node][-1].extend(timestamp, repl_size, deletion)
    def volume(self, start, end, pattern, site_mask):
        if not pattern.match(self.tier):
            return 0
        V = 0
        for sid,replicas in self.replicas.iteritems():
            if sid not in site_mask:
                continue
            for r in replicas:
                V += r.volume(start, end)
        return float(V) / (end - start)
    def add_access(self, node, timestamp, n):
        if node not in self.accesses:
            self.accesses[node] = AccessHistory()
        self.accesses[node].add_access(timestamp, n)
    def usage(self, start, end, pattern, site_mask):
        if not self.nfiles:
            return 0 
        if not self._nfiles_inverse:
            self._nfiles_inverse = 1./self.nfiles
        if not pattern.match(self.tier):
            return 0
        N = 0 
        for sid, acc in self.accesses.iteritems():
            if sid not in site_mask:
                continue 
            N += acc.usage(start, end)
        return N * self._nfiles_inverse
