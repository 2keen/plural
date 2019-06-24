from googleapiclient import discovery
import google.cloud.resource_manager as rm
from googleapiclient import errors
import os


def buildClient():
    return rm.Client()


def projectList(client):
    p_list = list(client.list_projects())
    returnList = []
    for p in p_list:
        returnList.append(p.project_id)
    return returnList


def buildCompute():
    return discovery.build('compute', 'v1')


def listZones(compute, project):
    returnZones = []
    zones = compute.zones().list(project=project).execute()
    for z in zones['items']:
        returnZones.append(z['name'])
    return returnZones    


def listVMs(compute, project, zone):
    returnVMs = []
    vms = compute.instances().list(project=project, zone=zone).execute()
    if not 'items' in vms:
        return None
    for v in vms['items']:
        returnVMs.append(v['name'])
    return returnVMs






def allVMs(compute, p_list):
    VMs = []
    for pi, p in enumerate(p_list, 1):
        os.system('cls')
        print('project: {0} of {1}\nzone:'.format(pi, len(p_list)))
        try:
            zones = listZones(compute, p)
        except errors.HttpError as err:
            VMs.append({
                'project': p,
                'vm': None
            })
            continue
        for zi, z in enumerate(zones):
            os.system('cls')
            print('project: {0} of {1}\nzone: {2} of {3}'.format(pi, len(p_list), zi, len(zones)))
            #print('zone: {0} of {1}'.format(zi, len(zones)))
            try:
                vms = listVMs(compute, p, z)
            except errors.HttpError as err:
                VMs.append({'project': p,
                    'vm': None
                })
                continue
            if vms == None:
                continue
            for v in vms:
                VMs.append({'project': p,
                    'vm': v
                })
    return VMs

client = buildClient()
p_list = projectList(client)
compute = buildCompute()
vm_list = allVMs(compute, p_list)


with open('vm_list.txt', 'w') as f:
         csvwriter = csv.writer(f)
         v = VMs[0]
         header = v.keys()
         csvwriter.writerow(header)
         for v in VMs:
             csvwriter.writerow(v.values())

    
fw = compute.firewalls()
fw.list(project='pr-ca-osc').execute()


p_list = [
'hd-titan',
'hdtechlab',
'hd-engineering',
'hd-cloud-services',
'pr-homeservices',
'io1-workbenchapp-prod',
'pr-vendor-collab-thd',
'analytics-amp-thd',
'analytics-ca-pr-ecom',
'io1-datalake-views',
'hd-srch-prod',
'pr-inventory-commons',
'hd-mkt-data-platform-prod',
'analytics-ugr-thd',
'hd-perf-engineering',
'hd-sr-engineering',
'st-sc1-distributionserv-thd',
'pr-threat-detection-thd',
'pr-com-203715',
'pr-ca-schain-events-thd-207813',
'pr-projectresetreporting-thd',
'pr-srcoptimization',
'pr-pricing-competitive-thd',
'pr-pcf1-19090210',
'pr-quotecenter',
'pr-mkt-blog',
'pr-forseti-security-thd',
'hd-microsite-toolcontent',
'hd-publishing-prod',
'io1-datalake',
'hd-datascience-prod',
'hd-onlineqe-prod',
'pr-design-systems-td-mw',
'bubbly-fuze-118217',
'hd-corp-pr',
'sdp-prod18090200',
'hd-search-cce-prod',
'hd-jxh3956',
'io1-homeservices-prod',
'pr-hcm-integration-thd',
'analytics-gcc-thd',
'hd-ux-team',
'hd-personalization-prod',
'io1-svc',
'hd-digitaldecor-prod',
'pr-ca-common',
'hd-consumerapps-prod',
'hd-paintcolor-prod',
'pr-securityassurance-thd',
'hd-automotive-prod',
'pr-faststorelabor-thd',
'hd-jxo0517',
'server-on-demand',
'hd-www-static',
'hd-budgeting',
'hd-opstool-pr',
'io1-snowclone-prod',
'analytics-pr-ca-pricing',
'pr-ca-hrl-wbt',
'hd-dotcomsec-prod',
'io1-datalake-services',
'hd-contactctr-prod',
'pr-transportation',
'pr-salesforecasting',
'rc-replenishment-thd',
'pr-security-datalake-views-thd',
'pr-security-datalake-thd',
'hd-digitalassetmgmt-prod',
'pr-ent-accounting-thd',
'pr-ca-osc',
'hd-merchcommon-prod'
]