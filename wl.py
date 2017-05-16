import csv
import random
from operator import itemgetter

policy_shared_rate = 2

flavors = []
flavors_cpu_low = []
flavors_cpu_mid = []
flavors_cpu_hi  = []
flavors_numa    = []
flavors_non_numa = []
flavors_pin = []
flavors_non_pin = []
f_hi_numa_pin = []
f_hi_numa = []
f_hi_pin = []
f_hi = [] #no numa, no pin
f_low_numa_pin = []
f_low_numa = []
f_low_pin = []
f_low =[]
f_mid_numa = []
f_mid_numa_pin = []
f_mid_pin = []
f_mid = []


fid = 0
with open('flavors', 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in spamreader:
        flavor = {}
        flavor['id'] = fid
        flavor['cpu'] = int(row[0])
        if flavor['cpu'] <= 2:
            flavors_cpu_low.append(fid)
        elif flavor['cpu'] <= 8:
            flavors_cpu_mid.append(fid)
        elif flavor['cpu'] > 8:
            flavors_cpu_hi.append(fid)
        flavor['ram'] = int(row[1])
        if row[2] == '1':
            flavor['numa'] = True
            flavors_numa.append(fid)
        else:
            flavor['numa'] = False
            flavors_non_numa.append(fid)
        if row[3] == '1':
            flavor['pin'] = True
            flavors_pin.append(fid)
        else:
            flavor['pin'] = False
            flavors_non_pin.append(fid)
        fid += 1
        flavors.append(flavor)
flavors_hi_numa_pin = set(flavors_pin).intersection(
    set(flavors_numa)).intersection(set(flavors_cpu_hi))
flavors_hi_numa_pin = list(flavors_hi_numa_pin)
flavors_hi_numa = set(flavors_cpu_hi).intersection(
    set(flavors_numa))
flavors_hi_numa = list(flavors_hi_numa)
flavors_hi_pin = set(flavors_cpu_hi).intersection(
    set(flavors_pin))
flavors_hi_pin = list(flavors_hi_pin)
flavors_hi = set(flavors_cpu_hi).intersection(
    set(flavors_non_pin)).intersection(set(flavors_non_numa))
flavors_hi =  list(flavors_hi)

flavors_low_numa_pin = set(flavors_pin).intersection(
    set(flavors_numa)).intersection(set(flavors_cpu_low))
flavors_low_numa_pin = list(flavors_low_numa_pin)
flavors_low_numa = set(flavors_cpu_low).intersection(
    set(flavors_numa))
flavors_low_numa = list(flavors_low_numa)
flavors_low_pin = set(flavors_cpu_low).intersection(
    set(flavors_pin))
flavors_low_pin = list(flavors_low_pin)
flavors_low = set(flavors_cpu_low).intersection(
    set(flavors_non_pin)).intersection(set(flavors_non_numa))
flavors_low =  list(flavors_low)

flavors_mid_numa_pin = set(flavors_pin).intersection(
    set(flavors_numa)).intersection(set(flavors_cpu_mid))
flavors_mid_numa_pin = list(flavors_mid_numa_pin)
flavors_mid_numa = set(flavors_cpu_mid).intersection(
    set(flavors_numa))
flavors_mid_numa = list(flavors_mid_numa)
flavors_mid_pin = set(flavors_cpu_mid).intersection(
    set(flavors_pin))
flavors_mid_pin = list(flavors_mid_pin)
flavors_mid = set(flavors_cpu_mid).intersection(
    set(flavors_non_pin)).intersection(set(flavors_non_numa))
flavors_mid =  list(flavors_mid)

#compute-16,SHARED-CPU,40,2,512
servers = []
sid = 0
with open('servers', 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in spamreader:
        svr = {}
        svr['id'] = sid
        svr['name'] = row[0]
        svr['cfg'] = row[1]
        svr['cores'] = int(row[2])
        svr['sockets'] = int(row[3])
        svr['ram'] = int(row[4])
        sid += 1
        servers.append(svr)

#Generate work load
WORKLOAD_HI = {
    'CPU_LOW': 20,
    'CPU_MID': 40,
    'CPU_HI': 40,
    'NUMA': 30, # non NUMA :(
    'PINNING': 1, # non Pin :(
    'AFFINITY': 20,
    'ANTIAFF' : 20
}
#Generate work load
WORKLOAD_HI2 = {
    'CPU_LOW': 10,
    'CPU_MID': 30,
    'CPU_HI': 60,
    'NUMA': 30, # non NUMA :(
    'PINNING': 1, # non Pin :(
    'AFFINITY': 20,
    'ANTIAFF' : 20
}
#Generate work load
WORKLOAD_HI3 = {
    'CPU_LOW': 25,
    'CPU_MID': 25,
    'CPU_HI': 50,
    'NUMA': 30, # non NUMA :(
    'PINNING': 1, # non Pin :(
    'AFFINITY': 20,
    'ANTIAFF' : 20
}

WORKLOAD_MID1 = {
    'CPU_LOW': 30,
    'CPU_MID': 50,
    'CPU_HI': 20,
    'NUMA': 30, # non NUMA :(
    'PINNING': 1, # non Pin :(
    'AFFINITY': 10,
    'ANTIAFF' : 20
}
WORKLOAD_MID2 = {
    'CPU_LOW': 20,
    'CPU_MID': 60,
    'CPU_HI': 20,
    'NUMA': 30, # non NUMA :(
    'PINNING': 1, # non Pin :(
    'AFFINITY': 10,
    'ANTIAFF' : 20
}
WORKLOAD_MID3 = {
    'CPU_LOW': 10,
    'CPU_MID': 70,
    'CPU_HI': 20,
    'NUMA': 30, # non NUMA :(
    'PINNING': 1, # non Pin :(
    'AFFINITY': 10,
    'ANTIAFF' : 20
}



WL_NUM  = 360 #total cpu cores
wls = []
wls_numa = []

def createWorkLoad():
    wl_low = []
    wl_mid = []
    wl_hi  = []
    wl_numa = []
    wl_pinning = []
    wl_affinity = []
    wl_antiaff  = []

    wl_profile = WORKLOAD_MID3
    
    cores = 0
    wl_id = 0
    while cores < WL_NUM:
        wl = {}
        wl['fid'] = []
        rq_cpu = random.randint(1, 100)
        rq_numa = random.randint(1, 100)
        rq_pinning = random.randint(1, 100)
        rq_affinity = random.randint(1, 100)
        rq_antiaff = random.randint(1, 100)
        if (rq_cpu < wl_profile['CPU_LOW'] and
            rq_numa < wl_profile['NUMA'] and
            rq_pinning < wl_profile['PINNING']):
            wl['fid'].append( flavors_low[random.randint(0, len(flavors_low)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (rq_cpu < wl_profile['CPU_LOW'] and
            rq_numa >= wl_profile['NUMA'] and
            rq_pinning < wl_profile['PINNING']):
            wl['fid'].append( flavors_low_numa[random.randint(0, len(flavors_low_numa)-1)])
            wl['fid'].append( flavors_low_numa[random.randint(0, len(flavors_low_numa)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (rq_cpu < wl_profile['CPU_LOW'] and
            rq_numa < wl_profile['NUMA'] and
            rq_pinning >= wl_profile['PINNING']):
            wl['fid'].append(flavors_low_pin[random.randint(0, len(flavors_low_pin)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (rq_cpu < wl_profile['CPU_LOW'] and
            rq_numa >= wl_profile['NUMA'] and
            rq_pinning >= wl_profile['PINNING']):
            wl['fid'].append(flavors_low_numa_pin[random.randint(
                             0, len(flavors_low_numa_pin)-1)])
            wl['fid'].append(flavors_low_numa_pin[random.randint(
                             0, len(flavors_low_numa_pin)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (rq_cpu >= wl_profile['CPU_LOW'] and
            rq_cpu < wl_profile['CPU_MID'] +  wl_profile['CPU_LOW'] and
            rq_numa < wl_profile['NUMA'] and
            rq_pinning < wl_profile['PINNING']):
            wl['fid'].append(flavors_mid[random.randint(0, len(flavors_mid)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (rq_cpu >= wl_profile['CPU_LOW'] and
            rq_cpu < wl_profile['CPU_MID'] +  wl_profile['CPU_LOW'] and
            rq_numa >= wl_profile['NUMA'] and
            rq_pinning < wl_profile['PINNING']):
            wl['fid'].append(flavors_mid_numa[random.randint(0, len(flavors_mid_numa)-1)])
            wl['fid'].append(flavors_mid_numa[random.randint(0, len(flavors_mid_numa)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (rq_cpu >= wl_profile['CPU_LOW'] and
            rq_cpu < wl_profile['CPU_MID'] +  wl_profile['CPU_LOW'] and
            rq_numa < wl_profile['NUMA'] and
            rq_pinning >= wl_profile['PINNING']):
            wl['fid'].append(flavors_mid_pin[random.randint(0, len(flavors_mid_pin)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (rq_cpu >= wl_profile['CPU_LOW'] and
            rq_cpu < wl_profile['CPU_MID'] +  wl_profile['CPU_LOW'] and
            rq_numa >= wl_profile['NUMA'] and
            rq_pinning >= wl_profile['PINNING']):
            wl['fid'].append(flavors_mid_numa_pin[random.randint(
                             0, len(flavors_mid_numa_pin)-1)])
            
            wl['fid'].append(flavors_mid_numa_pin[random.randint(
                             0, len(flavors_mid_numa_pin)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (
            rq_cpu >= wl_profile['CPU_MID'] +  wl_profile['CPU_LOW'] and
            rq_numa < wl_profile['NUMA'] and
            rq_pinning < wl_profile['PINNING']):
            wl['fid'].append(flavors_hi[random.randint(0, len(flavors_hi)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (
            rq_cpu >= wl_profile['CPU_MID'] +  wl_profile['CPU_LOW'] and
            rq_numa >= wl_profile['NUMA'] and
            rq_pinning < wl_profile['PINNING']):
            wl["Affinity"] = []
            wl['fid'].append(flavors_hi_numa[random.randint(0, len(flavors_hi_numa)-1)])
            wl['fid'].append(flavors_hi_numa[random.randint(0, len(flavors_hi_numa)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (
            rq_cpu >= wl_profile['CPU_MID'] +  wl_profile['CPU_LOW'] and
            rq_numa < wl_profile['NUMA'] and
            rq_pinning >= wl_profile['PINNING']):
            wl['fid'].append(flavors_hi_pin[random.randint(0, len(flavors_hi_pin)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        elif (
            rq_cpu >= wl_profile['CPU_MID'] +  wl_profile['CPU_LOW'] and
            rq_numa >= wl_profile['NUMA'] and
            rq_pinning >= wl_profile['PINNING']):
            wl['fid'].append(flavors_hi_numa_pin[random.randint(
                             0, len(flavors_hi_numa_pin)-1)])
            wl['fid'].append(flavors_hi_numa_pin[random.randint(
                             0, len(flavors_hi_numa_pin)-1)])
            wl["Affinity"] = []
            wl["antiAff"] = []
        wl_id += 1
        wl['id'] = wl_id
        wls.append(wl)
        cores += [x['cpu'] for x in flavors if x['id']==wl['fid'][0]][0]
        if len(wl['fid']) > 1:
            cores += [x['cpu'] for x in flavors if x['id']==wl['fid'][1]][0]
    return cores


createWorkLoad()
import json
def saveWorkload():
    with open('data.txt', 'w') as outfile:
        json.dump(wls, outfile)
def sortWorkload():
    return sorted(wls, key=lambda k: sum(k['fid']))
def loadWorkload():
    with open('data.txt') as json_data:
        return json.load(json_data)
def printWorkload(wls=wls):
    cores = 0
    ram = 0
    for wl in wls:
        print wl
        for i in range(0, len(wl['fid'])):
            c,r = [(x['cpu'], x['ram']) for x in flavors if x['id']==wl['fid'][i]][0]
            ram += r
            cores += c
    print 'workload: ', 'ram:', ram, ' cores:', cores  
from itertools import izip
def workload2WL_numa():
    wls_numa = []
    iterwl = iter(wls)
    nwl = izip(iterwl, iterwl)
    for i,j in nwl:
        wl_numa = []
        ci, ri = [(x['cpu'], x['ram']) for x in flavors if x['id']==i['fid']][0]
        cj, rj = [(y['cpu'], y['ram']) for y in flavors if y['id']==j['fid']][0]
        wl_numa.append({'cpu':ci,'ram':ri})
        wl_numa.append({'cpu':cj,'ram':rj})

        wls_numa.append(wl_numa)
    print wls_numa
    return wls_numa


def printWorkLoad_numa():
    cores1,cores2 = 0,0
    ram1,ram2 = 0,0
    for wl in wls_numa:
        print wl
        n1,n2 = wl[0],wl[1]
        cores1 += n1['cpu']
        cores2 += n2['cpu']
        ram1 += n1['ram']
        ram2 += n2['ram']
    print ('workload_numa: ', 'ram1:', ram1, ' cores1:',
           cores1, 'ram2:', ram2, ' cores2:', cores2)
wls = saveWorkload()    
wls = loadWorkload()
wls = sortWorkload()
print wls
printWorkload(wls)

