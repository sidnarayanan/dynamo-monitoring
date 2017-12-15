#!/usr/bin/python

import sys
import os, re, MySQLdb
import sqlite3
import shutil

def query_cursor(db='dynamo'):
        db = MySQLdb.connect(
                read_default_file = '/etc/my.cnf', 
                read_default_group = 'mysql-dynamo', 
                db = db
                )
        return db.cursor()


def query_sites(cursor=None):
    if not cursor:
        cursor = query_cursor('dynamohistory')
    sql = ' SELECT `id`, `name` FROM `sites`;'
    cursor.execute(sql)
    results = cursor.fetchall()
    return results

def query_datasets(cursor=None):
    if not cursor:
        cursor = query_cursor('dynamohistory')
    sql = ' SELECT d.`id`, d.`name` FROM `datasets` AS d;'
    cursor.execute(sql)
    results = cursor.fetchall()
    print '-> query fetched %i results'%(len(results))
    return results

def query_accesses(cursor=None):
    if not cursor:
        cursor = query_cursor('dynamo')
    sql = 'SELECT d.`name`, s.`name`, da.`date`, da.`num_accesses` FROM `dataset_accesses` AS da INNER JOIN `datasets` AS d ON d.`id` = da.`dataset_id` INNER JOIN `sites` AS s ON s.`id` = da.`site_id` ORDER BY da.`date` ASC ;'
    cursor.execute(sql)
    return cursor.fetchall()

### how to read archives ###
def valid_runs(cursor=None):
	if not cursor:
	    cursor = query_cursor('dynamohistory')
	sql = 'SELECT id, time_start FROM `runs` WHERE `partition_id`=10 AND `operation`=\'deletion\' ORDER BY id ASC;'
	cursor.execute(sql)
	results = cursor.fetchall()
	return results


def read_run(run, archive_dir='/mnt/hadoop/dynamo/dynamo/detox_snapshots'):
    print '-> un-archiving run',run 
    str_1 = '%.3i'%(int((run - run%1e6) / 1e6))
    str_2 = '%.3i'%(int((run%1e6 - run%1e3) / 1e3))
    archive_dir = '/'.join([archive_dir, str_1, str_2])

    try:
	db_file = archive_dir + '/snapshot_%.9i.db.xz'%run
	for ext in ['.db', '.db.xz']:
	    try:
		os.remove('/tmp/monitor'+ext)
	    except OSError:
		pass
	shutil.copyfile(db_file, '/tmp/monitor.db.xz')
	os.system('unxz /tmp/monitor.db.xz')

	db = sqlite3.connect('/tmp/monitor.db')
	cursor = db.cursor()
	sql = 'SELECT `site_id`, `dataset_id`, `size`, `decision_id` FROM replicas;'
	cursor.execute(sql)
	return cursor.fetchall()
    except IOError:
	return []
