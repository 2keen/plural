import os
import sys
import time
import pandas as pd #Note: need to install pyarrow for dataframe upload to BigQuery
from datetime import datetime
from googleapiclient import discovery
from google.cloud import resource_manager
from google.cloud import bigquery as bq
from google.api_core import exceptions as gcore_ex
from google.auth import exceptions as gauth_ex
from googleapiclient import errors


class org_meta:
    def __init__(self, default_project='analytics-amp-thd'):
        self._default_project = default_project
        #self.client = resource_manager.Client()
        self.project_list = self.get_project_list()
        self.project_items = []#self.get_all_project_items()
        self.dataset_details = []
        self.dataset_access = []
        self.table_details = []


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


    def project_iam(self, project):
        proj = project.project_id
        service = discovery.build('cloudresourcemanager', 'v1')
        #IAM_list = []
        data = []
        try:
            response = service.projects().getIamPolicy(resource = proj).execute()
        except errors.HttpError as err:
            return {'object': err, 'project': proj}
        for policy in response['bindings']:
            #IAM_list.append({u'projectId':proj,u'role':policy['role'],u'members':policy['members']})
            for member in policy['members']:
                data.append({'projectId': proj,
                    'role': policy['role'],
                    'member_type': member[:member.find(':')],
                    'member': member[member.find(':')+1:]
                })
                #data.append(proj + '|' + policy['role'] + '|' + member[:member.find(':')] + '|' + member[member.find(':')+1:])
        print('rows: ' + str(len(data)))
        df = pd.DataFrame(data)
        return df


    def all_projects_iam(self):
        for pi, proj in enumerate(self.project_list,1):
            os.system('cls')
            print('project: {0} of {1}\n    {2}'.format(pi, len(self.project_list), proj))
            for _ in range(3):
                iam = self.project_iam(proj)
                if type(iam) == pd.core.frame.DataFrame:
                    break
                if type(iam) == dict:
                    if type(iam['object']) == gauth_ex.RefreshError:
                        time.sleep(3)
                        iam = self.project_iam(proj)
                        continue
                    if type(iam['object']) == gcore_ex.NotFound:
                        break
                    else:
                        raise iam['object']
            yield iam


    def all_projects_iam_df(self):
        data = []
        for i in self.all_projects_iam():
            if type(i) == dict:
                continue
            data.append(i)
        return pd.concat(data)


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


def main():
    org = org_meta()
    df = org.all_projects_iam_df()
    org.to_bq_table_from_df(data=df, dest_table='PROJECT_IAM_LISTING')


if __name__ == '__main__':
    main() 
        