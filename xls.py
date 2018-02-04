from sys import argv
import os
import xml.etree.ElementTree as ET
from string import ascii_uppercase
import time

def _get_row(name):
    rows = {
        'XAODX' : [3,4,5],
        'XAOD' : [8,9,10],
        'XAODSIM' : [13,14,15],
        'MINIAODX' : [18,19,20],
        'RECO' : [23,24,25]
    }
    ll = name.split('_')
    tier = ll[0]
    period = ll[1]
    if '3m' in period:
        iM = 0
    elif '6m' in period:
        iM = 1
    else:
        iM = 2
    return rows[tier][iM]

def write_xls(label,outdir,templdir,plots):
    user = os.environ['USER']

    os.system('mkdir -p /tmp/%s/'%user)
    os.system('rm -rf /tmp/%s/xlstempl/'%(user))
    os.system('cp -r %s /tmp/%s/xlstempl'%(templdir,user))

    tree = ET.parse('/tmp/%s/xlstempl/xl/worksheets/sheet2.xml'%(user))
    root = tree.getroot()

    cells = {}

    for child in root.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}sheetData'):
        for grandchild in child:
                cells[grandchild.attrib['r']] = grandchild.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v')

    for name, content in plots.iteritems():
        try:
            row = _get_row(name)
        except KeyError:
            print 'could not find',name
            continue
        for iB, val in enumerate(content):
            column = ascii_uppercase[iB+1]
            cell = cells['%s%i'%(column,row)]
            cell.text = str(val)
            cell.set('updated','yes')

    tree.write('/tmp/%s/xlstempl/xl/worksheets/sheet2.xml'%user)

    os.system('cd /tmp/%s/xlstempl/; find . -type f | xargs zip new.xlsx; cd -'%user)

    mvcmd = 'mv /tmp/%s/xlstempl/new.xlsx %s/%s.xlsx'%(user,outdir,label)
    print mvcmd
    os.system(mvcmd)


