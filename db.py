#!/usr/bin/python

import sys
import os, re, MySQLdb
import sqlite3
import shutil

def get_cursor(db='dynamo'):
    db = MySQLdb.connect(
            read_default_file = '/etc/my.cnf', 
            read_default_group = 'mysql-dynamo', 
            db = db
            )
    return db.cursor()


def query_sites(cursor=None):
    if not cursor:
        cursor = get_cursor('dynamohistory')
    sql = ' SELECT `id`, `name` FROM `sites`;'
    cursor.execute(sql)
    results = cursor.fetchall()
    print '-> query fetched %i results'%(len(results))
    keys = ['id', 'name']
    keys = dict([(keys[i],i) for i in xrange(len(keys))])
    return results, keys

def query_datasets(cursor=None):
    if not cursor:
        cursor = get_cursor('dynamohistory')
    sql = ' SELECT d.`id`, d.`name` FROM `datasets` AS d;'
    cursor.execute(sql)
    results = cursor.fetchall()
    print '-> query fetched %i results'%(len(results))
    keys = ['id', 'name']
    keys = dict([(keys[i],i) for i in xrange(len(keys))])
    return results, keys

def query_datasets_ext(cursor=None):
    if not cursor:
        cursor = get_cursor('dynamo')
    sql = ' SELECT d.`id`, d.`name`, d.`last_update`, SUM(b.`num_files`) FROM `datasets` AS d INNER JOIN `blocks` AS b ON d.`id` = b.`dataset_id` GROUP BY d.`id`;'
    cursor.execute(sql)
    results = cursor.fetchall()
    print '-> query fetched %i results'%(len(results))
    keys = ['id', 'name', 'last_update', 'num_files']
    keys = dict([(keys[i],i) for i in xrange(len(keys))])
    return results, keys

def query_accesses(cursor=None):
    if not cursor:
        cursor = get_cursor('dynamo')
    sql = 'SELECT d.`name`, s.`name`, da.`date`, da.`num_accesses` FROM `dataset_accesses` AS da INNER JOIN `datasets` AS d ON d.`id` = da.`dataset_id` INNER JOIN `sites` AS s ON s.`id` = da.`site_id` ORDER BY da.`date` ASC ;'
    cursor.execute(sql)
    results = cursor.fetchall()
    print '-> query fetched %i results'%(len(results))
    keys = ['ds_name', 'site_name', 'date', 'num_accesses']
    keys = dict([(keys[i],i) for i in xrange(len(keys))])
    return results, keys

### how to read archives ###
def valid_runs(cursor=None):
    if not cursor:
        cursor = get_cursor('dynamohistory')
    sql = 'SELECT id, time_start FROM `runs` WHERE `partition_id`=10 AND `operation`=\'deletion\' ORDER BY id ASC;'
    cursor.execute(sql)
    results = cursor.fetchall()
    keys = ['id', 'time_start']
    keys = dict([(keys[i],i) for i in xrange(len(keys))])
    return results, keys


def read_run(run, archive_dir='/mnt/hadoop/dynamo/dynamo/detox_snapshots'):
    if run%10 == 0 or True:
        print '-> un-archiving run',run 
    str_1 = '%.3i'%(int((run - run%1e6) / 1e6))
    str_2 = '%.3i'%(int((run%1e6 - run%1e3) / 1e3))
    archive_dir = '/'.join([archive_dir, str_1, str_2])

    try:
        db_file = archive_dir + '/snapshot_%.9i.db.xz'%run
        for ext in ['.db', '.db.xz']:
            try:
                os.remove('/tmp/monitor_%.9i'%run+ext)
            except OSError:
                pass
        shutil.copyfile(db_file, '/tmp/monitor_%.9i.db.xz'%run)
        os.system('unxz /tmp/monitor_%.9i.db.xz'%run)

        db = sqlite3.connect('/tmp/monitor_%.9i.db'%run)
        cursor = db.cursor()
        sql = 'SELECT `site_id`, `dataset_id`, `size`, `decision_id` FROM replicas;'
        cursor.execute(sql)
        results = cursor.fetchall()
        keys = ['site_id', 'ds_id', 'size', 'decision_id']
        keys = dict([(keys[i],i) for i in xrange(len(keys))])
        for ext in ['.db', '.db.xz']:
            try:
                os.remove('/tmp/monitor_%.9i'%run+ext)
            except OSError:
                pass
        return results, keys
    except IOError:
        return [], {}
