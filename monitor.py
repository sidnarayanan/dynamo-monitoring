import sys
import db
import obj
sys.path = [x for x in sys.path if '/.local/' not in x] # for testing purposes to avoid conflicting libraries
import time
import cPickle as pickle
from argparse import ArgumentParser
import re

parser = ArgumentParser()
parser.add_argument('--skip_history', action='store_true')
parser.add_argument('--skip_accesses', action='store_true')
args = parser.parse_args()

now = time.time()
year_ago = now - 365*86400

dynamo = db.query_cursor('dynamo')
dynamo_history = db.query_cursor('dynamohistory')

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
# import matplotlib.lines as mlines

print 'get a collection of datasets'
results = db.query_datasets(dynamo_history)
global_datasets = {}
dname_to_id = {}
for r in results:
    global_datasets[r[0]] = obj.Dataset(r[1])
    dname_to_id[r[1]] = r[0]


print 'get a list of sites'
sites = {}
results = db.query_sites(dynamo_history)
sname_to_id = {}
for r in results:
    if 'MSS' in r[1] or 'Buffer' in r[1]:
        continue
    sites[r[0]] = obj.Site(r[0], r[1])
    sname_to_id[r[1]] = r[0]

if args.skip_history:
    sites = pickle.load(open('sites.pkl', 'rb'))
else:
    archive = '/mnt/hadoop/dynamo/dynamo/detox_snapshots/000'


    print 'figure out which dynamo runs are relevant and read them in'
    runs = db.valid_runs(dynamo_history)
    n_valid_runs = 0
    for run in runs:
        ts = time.mktime(run[1].timetuple())
        if ts < year_ago:
            continue
        n_valid_runs += 1
        rows = db.read_run(run[0])
        for row in rows:
            sites[row[0]].add_replica(row[1], row[2], ts, row[3], global_datasets)
#        if n_valid_runs > 10:
#            break

    with open('sites.pkl', 'wb') as pklfile:
        pickle.dump(sites, pklfile, -1)


if args.skip_accesses:
    accesses = pickle.load(open('accesses.pkl', 'rb')) 
else:
    accesses = {}
    print 'querying accesses'
    for r in db.query_accesses(dynamo):
        ts = r[3]
        if ts < year_ago:
            continue
        if r[0] not in accesses:
            accesses[r[0]] = {}
        if r[1] not in accesses[r[0]]:
            accesses[r[0]][r[1]] = []
        accesses[r[0]][r[1]].append( (ts, r[4]) )
    with open('accesses.pkl', 'wb') as pklfile:
        pickle.dump(accesses, pklfile, -1)


print 'merging access and transfer history'
for dk,dv in accesses.iteritems():
    did = dname_to_id[dk]
    for sk,sv in dv.iteritems():
        sid = sname_to_id[sk]
        sites[sid].add_access(did, *sv)



datatiers = {'XAODX'    : '.*AOD.*', 
             'RECO'     : '^RECO$', 
             'MINIAODX' : '^MINIAOD.*',
             'AODX'     : '^AOD.*',}
datatiers = dict([(k,re.compile(v)) for k,v in datatiers.iteritems()])

sitetiers = {'T1'  : '^T1.*',
             'T2'  : '^T2.*',
             'T12' : '^T[12].*'}
sitetiers = dict([(k,re.compile(v)) for k,v in sitetiers.iteritems()])

def bin(usage, age, threshold):
    if usage == 0:
        if age > threshold:
            return 1
        else:
            return 0
    else:
        return min(14, int(usage)) + 2


