#!/usr/bin/python

import sys
sys.path = [x for x in sys.path if '/.local/' not in x] # for testing purposes to avoid conflicting libraries

import db
import obj
import xls
from os import system
import time
import cPickle as pickle
from argparse import ArgumentParser
import re
import numpy as np

parser = ArgumentParser()
parser.add_argument('--skip_history', action='store_true')
parser.add_argument('--skip_accesses', action='store_true')
parser.add_argument('--skip_all', action='store_true')
args = parser.parse_args()

def myprint(x):
    print x
    sys.stdout.flush()

if args.skip_all:
    args.skip_history = True
    args.skip_accesses = True

now = time.time()
year_ago = now - 400*86400

dynamo = db.get_cursor('dynamo')
dynamo_history = db.get_cursor('dynamohistory')

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
# import matplotlib.lines as mlines

myprint('get a collection of datasets')
results,_ = db.query_datasets(dynamo_history)
global_datasets = {}
dname_to_id = {}
id_to_dname = {}
for r in results:
    global_datasets[r[0]] = obj.Dataset(r[1])
    dname_to_id[r[1]] = r[0]
    id_to_dname[r[0]] = r[1]

myprint('get dataset info')
results,_ = db.query_datasets_ext(dynamo)
for r in results:
    dname = r[1]
    try:
        did = dname_to_id[dname]
        ct = time.mktime(r[2].timetuple())
        gds = global_datasets[did]
        gds.created = ct
        gds.nfiles = r[3]
    except KeyError:
        pass # this is a superset

myprint('get a list of sites')
results,_ = db.query_sites(dynamo_history)
sname_to_id = {}
id_to_sname = {}
for r in results:
    if 'MSS' in r[1] or 'Buffer' in r[1]:
        continue
    sname_to_id[r[1]] = r[0]
    id_to_sname[r[0]] = r[1]

if args.skip_history and args.skip_accesses:
    datasets = pickle.load(open('merged.pkl','rb'))
else:
    datasets = {}
    if args.skip_history:
        myprint('loading datasets cache')
        datasets = pickle.load(open('datasets.pkl', 'rb'))
        myprint('-> fetched %i datasets'%(len(datasets)))
    else:
        myprint('figure out which dynamo runs are relevant and read them in')
        runs,_ = db.valid_runs(dynamo_history)
        n_valid_runs = 0
        for run in runs:
            ts = time.mktime(run[1].timetuple())
            if ts < year_ago:
                continue
            n_valid_runs += 1
            rows,_ = db.read_run(run[0])
            for row in rows:
                if row[1] not in datasets:
                    datasets[row[1]] = global_datasets[row[1]]
                datasets[row[1]].add_replica(node=row[0], 
                                             repl_size=row[2],
                                             timestamp=ts,
                                             decision=row[3])
    #        if n_valid_runs > 10:
    #            break

        with open('datasets.pkl', 'wb') as pklfile:
            pickle.dump(datasets, pklfile, -1)


    if args.skip_accesses:
        myprint('loading accesses cache')
        accesses = pickle.load(open('accesses.pkl', 'rb')) 
        myprint('-> fetched %i accesses'%(len(accesses)))
    else:
        accesses = {}
        myprint('querying accesses')
        for r in db.query_accesses(dynamo)[0]:
            ts = time.mktime(r[2].timetuple())
            if ts < year_ago:
                continue
            if r[0] not in accesses:
                accesses[r[0]] = {}
            if r[1] not in accesses[r[0]]:
                accesses[r[0]][r[1]] = []
            accesses[r[0]][r[1]].append( (ts, r[3]) )
        with open('accesses.pkl', 'wb') as pklfile:
            pickle.dump(accesses, pklfile, -1)


    myprint('merging access and transfer history')
    for dk,dv in accesses.iteritems():
        did = dname_to_id[dk]
        try:
            ds = datasets[did]
            for sk,sv in dv.iteritems():
                sid = sname_to_id[sk] # haha
                for i_access in sv:
                    ds.add_access(sid, *i_access)
        except KeyError:
            pass

    with open('merged.pkl', 'wb') as pklfile:
        pickle.dump(datasets, pklfile, -1)



datatiers = {
             'XAODX'    : '.*AOD.*', 
             'XAOD'     : '.*AOD$', 
             'RECO'     : '^RECO$', 
             'MINIAODX' : '^MINIAOD.*',
             'AODX'     : '^AOD.*',
             }
datatiers = dict([(k,re.compile(v)) for k,v in datatiers.iteritems()])

sitetiers = {
             'T1'  : '^T1.*',
             'T2'  : '^T2.*',
             'T12' : '^T[12].*',
             }
sitetiers = dict([(k,re.compile(v)) for k,v in sitetiers.iteritems()])
sitemasks = {}
for k,v in sitetiers.iteritems():
    sitemasks[k] = set([])
    for sname,sid in sname_to_id.iteritems():
        if v.match(sname):
            sitemasks[k].add(sid)

end_2017 = time.mktime(time.strptime('2017-12-31','%Y-%m-%d')) 
all_times = {'now' : [
                    ('12m', now-12*30*86400, now),
                    ('6m', now-6*30*86400 , now),
                    ('3m', now-3*30*86400 , now),
                ],
            'end2017' : [
                    ('12m', end_2017-12*30*86400, end_2017),
                    ('6m', end_2017-6*30*86400 , end_2017),
                    ('3m', end_2017-3*30*86400 , end_2017),
                ]
            }


bins = np.arange(-1,15)
def bin(usage, age, threshold):
    if usage == 0:
        if age > threshold:
            return 0
        else:
            return -1
    else:
        return min(14, int(usage)) 

myprint('creating plots')
basedir = '/home/snarayan/public_html/dynpop/latest/'

ticklabels = ['0 old', '0 new'] + [str(x) for x in bins[2:].tolist()]

for end_label, times in all_times.iteritems():
    outputbase = basedir + '/' + end_label
    system('mkdir -p %s/txt/'%outputbase)
    plots = {}
    for tlabel, start,end in times:
        for d, drx in datatiers.iteritems():
            for s, mask in sitemasks.iteritems():
                ftxt = open(outputbase + '/txt/%s_%s_%s.txt'%(tlabel, d, s),'w') 
                ftxt.write('%5s %15s %15s %15s %s\n'%('bin','volume','naccess','nfiles','dataset'))
                content = [0]*len(bins)
                myprint('-> %s %s %s'%(tlabel, d, s))
                for _,ds in datasets.iteritems():
                    v = ds.volume(start, end, drx, mask)
                    u = ds.usage(start, end, drx, mask)
                    x = bin(u, ds.created, start) + 1
                    content[x] += v
                    ftxt.write('%5s %15f %15f %15f %s\n'%(ticklabels[x], v, u, ds.nfiles, ds.name))
                plt.close("all")
                fig, ax = plt.subplots()
                ax.set_xlabel('Number of accesses')
                ax.set_ylabel('Disk volume [PB]')
                ax.hist(bins, bins=bins, weights=content)
                ax.set_xticks(bins+0.45)
                ax.set_xticklabels(ticklabels, rotation=45)
                print '--> integral = %.3f'%(np.sum(content))
                output = outputbase + '/%s_%s_%s'%(tlabel, d, s)
                plt.savefig(output+'.png',bbox_inches='tight',dpi=300)
                plots[(tlabel, d, s)] = content
                ftxt.close()


    for s in sitemasks:
        for d in datatiers:
            plt.close("all")
            fig, ax = plt.subplots()
            ax.set_xlabel('Number of accesses')
            ax.set_ylabel('Disk volume [PB]')
            width = 0.2
            offset = 0
            bars = {}
            for (tlabel,_,__),color in zip(times, ['r','g','b']):
                bars[tlabel] = ax.bar(bins+offset, plots[(tlabel, d, s)], width, color=color)
                offset += width
            ax.set_xticks(bins+0.3)
            ax.set_xticklabels(ticklabels, rotation=45)
            ax.legend([bars[x[0]] for x in times], [x[0] for x in times])
            output = outputbase + '/stacked_%s_%s'%(d, s)
            plt.savefig(output+'.png',bbox_inches='tight',dpi=300)
        xls.write_xls(label=s,
                      outdir=outputbase,
                      templdir='/home/snarayan/dynamo-popularity/templ/', # TODO - don't hardcode
                      plots={'%s_%s'%(d,t) for d in in datatiers
                                           for (tlabel,_,__) in times})

# cache just a few things 
cachedir = basedir.replace('latest',strftime('%Y%m%d',time.gmtime()))
for tlabel in all_times:
    system('mkdir -p %s/%s'%(cachedir,tlabel))
    system('cp %s/%s/*xlsx %s/%s'%(basedir,tlabel,cachedir,tlabel))
