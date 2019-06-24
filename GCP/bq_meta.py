
import csv
import os
import sys
import io
import time
import ctypes
import pandas as pd #Note: need to install pyarrow for dataframe upload to BigQuery
from datetime import datetime
from googleapiclient import discovery
from google.cloud import resource_manager
from google.cloud import bigquery as bq
from google.api_core import exceptions as gcore_ex
from google.auth import exceptions as gauth_ex
from googleapiclient import errors

default_project = 'analytics-amp-thd'



@property
def client():
    return resource_manager.Client()


@property
def bqclient(projectId):
    return bq.Client(project=projectId)


def project_list(projectId):
    client = client()
    p_list = list(client.list_projects())
    p_list.sort(key=lambda x: x.project_id)
    os.system('cls')
    print("projects found: '{0}'\nfirstproject: '{1}'\nexecuted: {2}".format((len(p_list)), p_list[0].project_id, datetime.today().strftime('%y_%m_%d')))
    return p_list[:]
    



##########################################################

default_project = 'analytics-amp-thd'

p_iter = None
p_ct = None
d_iter = None
d_ct = None
t_iter = None
t_ct = None


def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


def print_update(pi=p_iter, pc=p_ct, di=d_iter, dc=d_ct, ti=t_iter, tc=t_ct):
    print('project: {} of {} dataset: {} of {} table: {} of {}'.format(pi, pc, di, dc, ti, tc))

class org_meta:
    def __init__(self, default_project='analytics-amp-thd'):
        self._default_project = default_project
        #self.client = resource_manager.Client()
        self.project_list = self.get_project_list()
        self.project_items = []#self.get_all_project_items()
        self.dataset_details = []
        self.dataset_access = []
        self.table_details = []
        self.table_schemas = []

    @property
    def client(self):
        return resource_manager.Client()


    def bqclient(self, projectId):
        return bq.Client(project=projectId)
    

    def get_project_list(self):
        p_list = list(self.client.list_projects())
        p_list.sort(key=lambda x: x.project_id)
        os.system('cls')
        print("projects found: '{0}'\nfirstproject: '{1}'\nexecuted: {2}".format((len(p_list)), p_list[0].project_id, datetime.today().strftime('%y_%m_%d')))
        return p_list[:]

    
    def get_project_item(self, projectListItem):
        return project_meta(projectListItem)


    def get_all_project_items(self, p_start=0, p_end=None):
        items = []
        print('pstart: {}\tpend: {}'.format(p_start, p_end))
        P_list = self.project_list[int(p_start):int(p_end)]
        for pi, p in enumerate(P_list, 1):
            print('project: {} of {} - {}'.format(pi, len(P_list), p.project_id))
            p0 = self.get_project_item(p)
            self.project_items.append(p0)
            self.dataset_details.extend(p0.dataset_details)
            #self.dataset_access.extend(p0.dataset_access)
            self.table_details.extend(p0.table_details)
            self.table_schemas.extend(p0.table_schemas)
            items.append(p0)
        print('Returned {} projects'.format(len(items)))
        
        return items


    def projects_to_table(self, dest_table='GCP_PROJECT_INFO'):
        list_of_details = []
        key_list = set()
        for p in self.project_items:
            list_of_details.append(p.details)
            for key, val in p['labels'].items():
                key_list.add(key)
        export=pd.DataFrame(list_of_details)
        cols = export.columns.tolist()
        export = export[cols[-3:] + cols[:-3]]
        schema = [bq.SchemaField('number', 'STRING'),
            bq.SchemaField('projectId', 'STRING'),
            bq.SchemaField('status', 'STRING')]
        for k in key_list:
            schema.append(bq.SchemaField(k, 'STRING'))
        self.to_bq_table_from_df(data=export, dest_table=dest_table)


    def projects_iam_to_table(self, dest_table='GCP_IAM_POLICIES'):
        err_list = []
        p_list = []
        for p in self.project_list:
            p_list.append(p.project_id)
        service = discovery.build('cloudresourcemanager', 'v1')
        IAM_list = []
        data = []
        for pi, proj in enumerate(p_list, 1):
            os.system('cls')
            print('{0}\nproject: {1} of {2}'.format(proj, pi, len(p_list)))
            try:
                response = service.projects().getIamPolicy(resource = proj).execute()
            except errors.HttpError as err:
                err_list.append({'object': err, 'project': proj})
                continue
            for policy in response['bindings']:
                IAM_list.append({u'projectId':proj,u'role':policy['role'],u'members':policy['members']})
                for m in policy['members']:
                    data.append(proj + '|' + policy['role'] + '|' + m[:m.find(':')] + '|' + m[m.find(':')+1:])
        print('rows: ' + str(len(data)))
        schema = [
            bq.SchemaField('projectId', 'STRING'),
            bq.SchemaField('role', 'STRING'),
            bq.SchemaField('member_type', 'STRING'),
            bq.SchemaField('member', 'STRING')
        ]
        self.to_bq_table_new(data, dest_table=dest_table, schema=schema)
        return err_list


    def to_bq_table_append_from_df(self, data, dest_proj='analytics-amp-thd', dest_dataset='PROJECT_IT', dest_table=''):
        client = bq.Client(dest_proj)
        table_ref = client.dataset(dest_dataset).table(dest_table+ '_' + str(datetime.today().strftime('%y_%m_%d')))
        iter = 0
        job_config = bq.job.LoadJobConfig(autodetect = True)
        job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)
        is_done = job.done()
        while is_done == False:
            is_done = job.done
        print('Job Complete')
        print(job.errors)


    def to_bq_table_from_df(self, data, dest_proj='analytics-amp-thd', dest_dataset='PROJECT_IT', dest_table=''):
        client = self.bqclient(dest_proj)
        table_ref = client.dataset(dest_dataset).table(dest_table+ '_' + str(datetime.today().strftime('%y_%m_%d')))
        iter = 0
        while True:
            try:
                client.get_table(table_ref)
                iter+=1
                table_ref = client.dataset(dest_dataset).table(dest_table+ '_' + str(datetime.today().strftime('%y_%m_%d')) + '_v' + str(iter))
                continue
            except gcore_ex.NotFound:
                break
        job_config = bq.job.LoadJobConfig(autodetect = True)
        job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)
        is_done = job.done()
        while is_done == False:
            is_done = job.done
        print('Job Complete')
        print(job.errors)


    def to_bq_table_new(self, data, delim='|', dest_proj='analytics-amp-thd', dest_dataset='PROJECT_IT', dest_table='', schema=None):
        client = self.bqclient(dest_proj)
        table_ref = client.dataset(dest_dataset).table(dest_table+ '_' + str(datetime.today().strftime('%y_%m_%d')))
        table = bq.Table(table_ref, schema=schema)
        try:
            client.create_table(table)
        except gcore_ex.Conflict:
            client.delete_table(table)
            client.create_table(table)
        buff = io.StringIO()
        buff.writelines([line + '\n' for line in data])
        buff.seek(0)
        job_config = bq.job.LoadJobConfig()
        job_config.field_delimiter = delim
        if schema==None: job_config.autodetect = True
        job = client.load_table_from_file(buff, table_ref, job_config=job_config, rewind=True)
        is_done = job.done()
        while is_done == False:
            is_done = job.done
        print('Job Complete')
        print(job.errors)

    
class project_meta:
    def __init__(self, project):
        if type(project) is not resource_manager.project.Project:
            raise ValueError(
                "'project' must be of type 'google.cloud.resource_manager.project.Project' not type '{}'".format(type(project))
            )
        self.project = project # needs to be type google.cloud.resource_manager.project.Project
        self.dataset_list = self.get_dataset_list()
        self.dataset_items = self.get_dataset_items()
        self.table_details = self.get_table_details()
        self.table_schemas = self.get_table_schemas()


    @property
    def project_id(self):
        return self.project.project_id


    @property
    def project_details(self):
        p_details = {}
        p_details['number'] = self.project.number
        p_details['project_id'] = self.project.project_id
        p_details['status'] = self.project.status
        p_details['labels'] = self.project.labels
        return flatten_json(p_details)


    def get_dataset_list(self):
        bqclient = bq.Client(project=self.project.project_id)
        try:
            d_list = list(bqclient.list_datasets())
        except gcore_ex.NotFound as ex:
            self.dataset_list = [{'project': self.project.project_id,
                                   'error': ex.code.name,
                                   'message': ex.message}]
            return
        except gcore_ex.BadRequest as ex:
            self.dataset_list = [{'project': self.project.project_id,
                                   'error': ex.code.name,
                                   'message': ex.message}]
            return
        self.dataset_list = d_list
        return d_list


    def get_dataset_items(self):
        items = []
        try:
            for di, d in enumerate(self.dataset_list, 1):
                print('\tdataset: {} of {} - {}'.format(di, len(self.dataset_list), d.dataset_id))
                if type(d) is dict:
                    #items.append(d)
                    continue
                items.append(dataset_meta(d.reference))
        except TypeError:
            pass
        return items


    @property
    def dataset_details(self):
        details = []
        for d in self.dataset_items:
            details.extend(d.dataset_details)
        return details

    @property
    def dataset_access(self):
        details = []
        for d in self.dataset_items:
            details.extend(d.dataset_access)
        return details


    def get_table_details(self):
        details = []
        for d in self.dataset_items:
            details.extend(d.table_details)
        return details


    def get_table_schemas(self):
        details = []
        for d in self.dataset_items:
            details.extend(d.table_schemas)
        return details


class dataset_meta:
    def __init__(self, datasetReference):
        self.datasetReference = datasetReference
        self.dataset = self.get_dataset_item()
        self.table_list = self.get_table_list()
        self.table_items = self.get_table_items()
        self.table_details = self.get_table_details()
        self.table_schemas = self.get_table_schemas()
        self.errors = []


    @property
    def project_id(self):
        return self.datasetReference.project


    @property
    def dataset_id(self):
        return self.datasetReference.dataset_id


    @property
    def dataset_details(self):
        d_details = {}
        d_details['projectId'] = self.dataset.project
        d_details['datasetId'] = self.dataset.dataset_id
        d_details['tables'] = len(self.table_list)
        d_details['created'] = self.dataset.created.strftime('%Y-%m-%d')
        d_details['modified'] = self.dataset.modified.strftime('%Y-%m-%d')
        d_details['description'] = self.dataset.description
        d_details['etag'] = self.dataset.etag
        d_details['labels'] = self.dataset.labels
        return flatten_json(d_details)


    @property
    def dataset_access(self):
        d_details = {}
        d_details['projectId'] = self.dataset.project
        d_details['datasetId'] = self.dataset.dataset_id
        d_details['tables'] = len(self.table_list)
        d_details['created'] = self.dataset.created.strftime('%Y-%m-%d')
        d_details['modified'] = self.dataset.modified.strftime('%Y-%m-%d')
        d_details['description'] = self.dataset.description
        d_details['etag'] = self.dataset.etag
        for x in self.dataset.access_entries:
            d_details['access_type'] = x.entity_type
            if x.role is None:
                d_details['access_id'] = x.entity_id['projectId']+':'+ x.entity_id['datasetId']+':'+ x.entity_id['tableId']
            else:
                d_details['access_id'] = x.entity_id
            d_details['access_role'] = x.role
        return d_details


    def get_dataset_item(self):
        bqclient = bq.Client(project=self.project_id)
        for i in range(3): # retries up to 3 times in case of refresh error
            try:
                dset = bqclient.get_dataset(self.datasetReference)
                break
            except gauth_ex.RefreshError as rf:
                print('Refresh Error, retrying...')
                if i == 3:
                    print('Refresh Error, cannot access ' + d.full_dataset_id)
                time.sleep(3)
                continue
            except gcore_ex.NotFound as nf:
                print('\t\tNotFound: {}'.format(self.datset_id))
                break
        return dset
    
    
    def get_table_list(self):
        bqclient = bq.Client(project=self.project_id)
        try:
            t_list = list(bqclient.list_tables(dataset=self.datasetReference))
        except gcore_ex.NotFound as ex:
            self.errors = [{'project': self.project_id,
                                   'error': ex.code.name,
                                   'message': ex.message}]
            return []
        except gcore_ex.BadRequest as ex:
            self.errors = [{'project': self.project_id,
                                   'error': ex.code.name,
                                   'message': ex.message}]
            return []
        except gcore_ex.Forbidden as ex:
            self.errors = [{'project': self.project_id,
                    'error': ex.code.name,
                    'message': ex.message}]
            return []
        return t_list


    def get_table_items(self):
        items = []
        if self.dataset_id == 'data_access_logs_v2':
            return []
        if type(self.table_list) is None:
            return
        for ti, t in enumerate(self.table_list, 1):
            print_str = '                table: {} of {} - {}'.format(ti, len(self.table_list), t.table_id)
            print_str = '\r{}{}'.format(print_str, ' '*(os.get_terminal_size()[0] - len(print_str)))
            if ti == len(self.table_list):
                print(print_str)
            else:
                print(print_str,end='')
            if type(t) is dict:
                continue
            items.append(table_meta(t.reference))
        return items


    def get_table_details(self):
        detail_list = []
        for ti, t in enumerate(self.table_items, 1):
            detail_list.append(t.table_details)
        return detail_list
    
    
    def get_table_schemas(self):
        detail_list = []
        for ti, t in enumerate(self.table_items, 1):
            detail_list.extend(t.schema_details)
        return detail_list


class table_meta:
    def __init__(self, tableReference):
        self.tableReference = tableReference
        self.table = self.get_table_item()
        self.errors = []
     

    @property
    def project_id(self):
        return self.tableReference.project


    @property
    def dataset_id(self):
        return self.tableReference.dataset_id


    @property
    def table_id(self):
        return self.tableReference.table_id


    def get_table_item(self):
        bqclient = bq.Client(project=self.project_id)        
        table_item = None
        for i in range(3): # retries up to 3 times in case of refresh error
            try:
                table_item = bqclient.get_table(self.tableReference)
                break
            except gauth_ex.RefreshError as rf:
                print('Refresh Error, retrying...')
                if i == 3:
                    print('Refresh Error, cannot access ' + d.full_dataset_id)
                    table_item = rf
                time.sleep(3)
                continue
            except gcore_ex.NotFound as nf:
                break
        return table_item


    @property
    def table_details(self):
        details = {}
        details['projectId'] = self.project_id
        details['datasetId'] = self.dataset_id
        details['tableId'] = self.table_id
        if self.table == None:
            return details
        details['type'] = self.table.table_type
        details['createDate'] = self.table.created.strftime('%Y-%m-%d')
        details['lastModified'] = self.table.modified.strftime('%Y-%m-%d')
        details['numBytes'] = self.table.num_bytes
        details['numRows'] = self.table.num_rows
        details['etag'] = self.table.etag
        return details
    
    @property
    def schema_details(self):
        schema_list = []
        for field in self.table.schema:
            field_details = {}
            field_details['projectId'] = self.project_id
            field_details['datasetId'] = self.dataset_id
            field_details['tableId'] = self.table_id
            if self.table == None:
                return details
            field_details['table_type'] = self.table.table_type
            field_details['createDate'] = self.table.created.strftime('%Y-%m-%d')
            field_details['lastModified'] = self.table.modified.strftime('%Y-%m-%d')
            field_details['numBytes'] = self.table.num_bytes
            field_details['numRows'] = self.table.num_rows
            field_details['etag'] = self.table.etag
            for key, val in field.to_api_repr().items():
                field_details[key] = val
            schema_list.append(field_details)
        return schema_list




def main(p_start, p_end):
    ctypes.windll.kernel32.SetConsoleTitleW('projects: {} through {}'.format(p_start, p_end))
    org = org_meta()
    org.get_all_project_items(p_start=p_start, p_end=p_end)
    schema_df = pd.DataFrame(org.table_schemas)
    #cols = schema_df.columns.tolist()
    cols2 = ['projectId', 'datasetId','tableId',
             'numBytes', 'numRows',
             'createDate', 'lastModified',
             'table_type', 'etag',
             'name', 'type', 'mode', 'description']
    schema_df = schema_df[cols2]
    org.to_bq_table_append_from_df(schema_df, dest_table='TABLE_SCHEMAS')

def get_all_datasets(meatbq_obj):
    a = metabq_obj
    a.all_projects()

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])        