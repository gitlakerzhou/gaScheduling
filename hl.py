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
WORKLOAD_MID = {
    'CPU_LOW': 30,
    'CPU_MID': 50,
    'CPU_HI': 20,
    'NUMA': 30, # non NUMA :(
    'PINNING': 1, # non Pin :(
    'AFFINITY': 10,
    'ANTIAFF' : 20
}



WL_NUM  = 300 #total cpu cores
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

    wl_profile = WORKLOAD_MID
    
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
#wls = saveWorkload()    
wls = loadWorkload()
wls = sortWorkload()
print wls
printWorkload(wls)

### update resource providors ####
rps = []
rps_dedicated = []
rps_shared = []

def rp_init():
    rps = []
    rps_dedicated = []
    rps_shared = []
    for s in servers:
        rp = {}
        rp['SharedCPU'] = (s['cfg'] == 'SHARED-CPU')
        rp['id'] = s['id']
        cpu = int(s['cores'])
        rp['cpu'] = []
        socket = int(s['sockets'])
        for i in range(0, socket):
            if rp['SharedCPU']:
                rp['total_cpu'] = cpu*policy_shared_rate
                rp['av_cpu'] = cpu*policy_shared_rate
                rp['cpu'].append(cpu*policy_shared_rate/socket)
            else:
                rp['total_cpu'] = cpu
                rp['av_cpu'] = cpu
                rp['cpu'].append(cpu/socket)
        rp['ram'] = int(s['ram'])
        rps.append(rp)
        if not rp['SharedCPU']:
            rps_dedicated.append(rp)
        else:
            rps_shared.append(rp)
    return rps_dedicated, rps_shared

rps_dedicated, rps_shared = rp_init()

def rp_update(rp, numa, cores, ram):
    placement = {}
    rp['ram'] -= sum(ram)
    placement['svr_id'] = rp['id']
    placement['ram'] = sum(ram)
    new_numa_node = []
    pnuma = [] #init with the existing deployment
    pnuma.extend(rp['cpu'])
        
    if numa:
        done = False
        for ni in range(0, len(rp['cpu'])):
            if (rp['cpu'][ni] < cores[ni] or done):
                print 'error state:', rp['cpu'][ni], cores[ni] 
                #new_numa_node.append(nn)
                break
            else:
                rp['cpu'][ni] -= cores[ni]
                new_numa_node.append(cores[ni])
                #done = True
    else:
        r = cores[0] - cores[0]/len(rp['cpu'])*len(rp['cpu'])
        for nn in rp['cpu']:
            if r > 0:
                if nn >= cores[0]/len(rp['cpu'])+1:
                    nn -= cores[0]/len(rp['cpu'])+1
                    r -= 1
                else:
                   nn -= cores[0]/len(rp['cpu'])
            else:
                nn -= cores[0]/len(rp['cpu'])
            new_numa_node.append(nn)
    del rp['cpu'][:]
    rp['cpu'].extend(new_numa_node)
    rp['av_cpu'] -= cores[0]
    for i in range(0,len(rp['cpu'])):
        pnuma[i] -= rp['cpu'][i]
    placement['fid']=[]
    placement['fid'].extend(pnuma)
    return placement

# Allocate resource according t the placementPlan
# return placemnets if it is done successfully
# return None otherwise
def rp_allocate_ga_planed(wls, placementPlan):
    placements = []
    allocatedCpus = 0
    unallocatedCpus = 0
    allocatedRam = 0
    unallocatedRam = 0
    unallocated = []

    '''init resource allocation '''
    rps_dedicated, rps_shared = rp_init()
    for i in range(0, len(wls)):
        cpu = []
        ram = []
        numa = False
        pin = False
        ''' find the workload'''
        for f in flavors:
            for ni in range(0,len(wls[i]['fid'])):
                if f['id']==wls[i]['fid'][ni]:
                    cpu.append(f['cpu'])
                    ram.append(f['ram'])
                    pin = f['pin'] #not used, not accurate
                    if ni > 0:
                        numa = True
                    else:
                        numa = False

        fit = False
        ''' find the server '''
        rp = [s for s in rps_dedicated if s['id'] == placementPlan[i]][0]
        #print rp
        #TODO make sure server id is for all servers not only dedicated
        if (rp['ram'] < sum(ram) or
            rp['av_cpu'] < sum(cpu)):
                fit = False
        elif numa:
            fit = True
            for ni in range(0, len(rp['cpu'])):
                if rp['cpu'][ni] < cpu[ni]:
                    fit = False
        else:
            fit = True
            for nn in rp['cpu']:
                if nn < cpu[0] / len(rp['cpu']):
                    fit = False

        if not fit:
            unallocatedCpus += sum(cpu)
            unallocatedRam += sum(ram)
            unallocated.append(wls[i])
            continue
        else:
            placement = rp_update(rp, numa, cpu, ram)
            allocatedCpus += sum(cpu)
            allocatedRam += sum(ram)
            placement['wl_id'] = wls[i]['id']
            ##return placement
    return allocatedCpus,allocatedRam, unallocated



# Allocate resource to a request
# return placement if it is done successfully
# return None otherwise
def rp_allocate_sorted(wl, isHLF=True):
    placement = None
    cpu = []
    ram = []
    #get cpu and ram from wl['fid'] and flavors
    for f in flavors:
        for i in range(0,len(wl['fid'])):
            if f['id']==wl['fid'][i]:
                cpu.append(f['cpu'])
                ram.append(f['ram'])
                pin = f['pin'] #not used, not accurate
                if i > 0:
                    numa = True
                else:
                    numa = False
    '''
    if pin:
        rppool = rps_dedicated
    else:
        rppool = rps_shared
    '''
    """Sort the provider pool using available CPUs"""
    n = sorted(rps_dedicated, key=itemgetter('av_cpu'), reverse=isHLF)

    """walk through the sorted list, find the first available:
    1. if NUMA, then allocate all the most avialble numa node
    2. if not NUMA, even out the request to all numa node
    """
    fit = False
    unallocatedCpus = 0
    for rp in n:
        fit = False
        if (rp['ram'] < sum(ram) or
            rp['av_cpu'] < sum(cpu)):
            continue
        elif numa:
            fit = True
            for ni in range(0,len(rp['cpu'])):
                if rp['cpu'][ni] < cpu[ni]:
                    fit = False
        else: # non-numa case place the load evenly
            fit = True
            for nn in rp['cpu']:
                if nn < cpu[0]/len(rp['cpu']):
                    fit = False
                    continue

        if not fit:
            unallocatedCpus +=sum( cpu)
            continue
        else:
            placement = rp_update(rp, numa, cpu, ram)
            placement['wl_id'] = wl['id']
            break

    return placement




deploymentPlan = []
notDeployed=[]
# Heaviest Loaded First
def workloadPlace_HLF(): 
    for wl in wls:
        p = rp_allocate_sorted(wl, isHLF=True)
        if p == None:
            notDeployed.append(wl)
        else:
            deploymentPlan.append(wl)

# Least Loaded First
def workloadPlace_LLF():
    for wl in wls:
        p = rp_allocate_sorted(wl, isHLF=False)
        if p == None:
            notDeployed.append(wl)
        else:
            deploymentPlan.append(wl)



workloadPlace_HLF()
print '----- deployed ------'
print deploymentPlan
print '----- not deployed ------'
print notDeployed
printWorkload(deploymentPlan)
if len(notDeployed) > 1:
    printWorkload(notDeployed)





from deap import base
from deap import creator
from deap import tools, algorithms
import numpy

creator.create("FitnessMax", base.Fitness, weights=(10.0,1.0,0.1))
creator.create("Individual", list, fitness=creator.FitnessMax)
rps_dedicated, rps_shared = rp_init()
toolbox = base.Toolbox()
seq_dedicated = [x['id'] for x in rps_dedicated]
SRV_MIN, SRV_MAX = min(seq_dedicated), max(seq_dedicated)
N_CYCLES = len(wls)
toolbox.register("attr_srv", random.randint, SRV_MIN, SRV_MAX)
toolbox.register("individual", tools.initRepeat, creator.Individual,
                 toolbox.attr_srv, n=N_CYCLES)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)

def evalPlacement(individual, prt=False):
    res_on_servers = []
    for i in range(0, len(rps_dedicated)):
        res_on_servers.append(0)
    req = 0
    for s in individual:
       flv = wls[req]['fid']
       for fi in range(0, len(flv)): 
           res_on_servers[int(s-SRV_MIN)] += int(flavors[flv[fi]]['cpu'])
       req += 1

    deviation = numpy.std(numpy.array([res_on_servers]))
    fit,ram,notfit = rp_allocate_ga_planed(wls, individual)
    if prt:
        print notfit 
    return (fit,ram,deviation)

def crossPlacement(ind1, ind2):
    EXCHANGE_RATE = 20
    for i in range(0, len(ind1)-1):
        r_nochange = random.randint(1, 100)
        if ind1[i] == ind2[i]:
            continue
        else:
            if r_nochange < EXCHANGE_RATE:
                ind1[i],ind2[i] = ind2[i],ind1[i]
    return ind1,ind2

def mutPlacement(ind):
    MUTE_RATE = int(0.1*100)
    for i in range(0, len(ind)):
        m = random.randint(1, 100)
        if m < MUTE_RATE:
            ind[i] = random.randint(SRV_MIN, SRV_MAX)
    return ind,

toolbox.register("evaluate", evalPlacement)
toolbox.register("mate", crossPlacement)
toolbox.register("mutate", mutPlacement)
toolbox.register("select", tools.selTournament, tournsize=3)

def main():
    random.seed(64)

    NGEN = 3000
    MU = 500
    LAMBDA = 100
    CXPB = 0.2
    MUTPB = 0.1
    
    pop = toolbox.population(n=MU)
    hof = tools.ParetoFront()
    stats = tools.Statistics(lambda ind: ind.fitness.values)
    stats.register("avg", numpy.mean, axis=0)
    stats.register("std", numpy.std, axis=0)
    stats.register("min", numpy.min, axis=0)
    stats.register("max", numpy.max, axis=0)
    
    algorithms.eaMuPlusLambda(pop, toolbox, MU, LAMBDA, CXPB, MUTPB, NGEN, stats, halloffame=hof)
    
    return pop, stats, hof

'''
if __name__ == "__main__":
    p,s,h = main()

    #print s
    print h
    for hh in h:
        print '------------------'
        print evalPlacement(h[0],prt=True)
    printWorkload(wls)
'''
