from typing import Tuple
from group import GroupMonad
import pandas as pd
class GroupProcess(GroupMonad):
    def run(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df_big, df_m = self.append_df(df1, df2)
        df_reset = self.reset_index(df_big)
        return df_reset, df_m

    def append_df(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        result = pd.concat([df1, df2])
        return result, df1

    def reset_index(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.reset_index()
    
class DagProcess:
    def __init__(self):
        self.pd_process_1 = GroupProcess('process_1').run
        self.go = GroupProcess('process_2')
    def run(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df_o1 = self.pd_process_1(df1, df2)
        df_o2 = self.pd_process_1(df1, df2)
        return df_o1, df_o2
    def pd_process_2(self, d1: pd.DataFrame, d2: pd.DataFrame) -> pd.DataFrame:
        return self.go.run(d1, d2)

"""
TODO: 

class DagProcess:
    def __init__(self):
        self.pd_process_1 = GroupProcess('process_1').run
        self.go = GroupProcess('process_2')
    def run(self, df1: TableUnit, df2: TableUnit) -> Tuple[TableUnit, TableUnit]:
        df_o1 = self.pd_process_1(df1, df2)
        df_o2 = self.pd_process_1(df1, df2)
        return df_o1, df_o2
    def pd_process_2(self, d1: TableUnit, d2: TableUnit) -> TableUnit:
        return self.go.run(d1, d2)

>> 

class DagProcess:
    def __init__(self):
        self.pd_process_1 = GroupProcess('process_1').run
        self.go = GroupProcess('process_2')
    def run(self, df1: TableUnit, df2: TableUnit) -> Tuple[TableUnit, TableUnit]:
        df_o1 = self.bind(self.pd_process_1)(df1, df2)
        df_o2 = self.bind(self.pd_process_1)(df1, df2)
        return df_o1, df_o2
    def pd_process_2(self, d1: TableUnit, d2: TableUnit) -> TableUnit:
        return self.go.run(d1, d2)

>> 

DagProcess().execute() # execute on aicloud (single pod)
DagProcess('process_x', dag) # a dag on airflow 

Questions - what should the features of dag monad be? 

1. DAG connecting: 
 - 1.1. Checking that the input table units match with the inputs defined in the GroupProcess
 - 1.2. Connecting the task_group of group process to the ancestor task_group(s)
 - 1.3. A `main_task` which return a `task_group` containing all `task_groups`(s) (more easily extended with additional task)
2. Local Run; 
 - 2.1. Append the execution unit of each binded method and run them one by one in a `run_locally` method 
 - 2.2. Add logging of start & finish for every execution of GroupProcess
3. Defining of input & output in TableUnit
4. Allow the airflow task of a particular output to be selected to later connection 
 

"""

def create_dummy_df():
    df = pd.DataFrame(columns = ['Name', 'Articles', 'Improved'])
    df = df.append({'Name' : 'Ankit', 'Articles' : 97, 'Improved' : 2200},
            ignore_index = True)

    df = df.append({'Name' : 'Aishwary', 'Articles' : 30, 'Improved' : 50},
            ignore_index = True)

    df = df.append({'Name' : 'yash', 'Articles' : 17, 'Improved' : 220},
          ignore_index = True)
    return df


if __name__ == '__main__':
    df = create_dummy_df()
    from datetime import datetime, timedelta
    from airflow.models import DAG
    default_args = {
        'owner': 'esb21375',                     # 請填寫 PM 的員編
        'start_date': datetime(2021, 9, 1),      # 盡量使用絕對時間
        'retries': 1,                            # 失敗 retry 多少次
        'retry_delay': timedelta(minutes=1),     # 失敗後間隔多久 retry
        'email': 'test@esunbank.com.tw',         # 至少填一個
        'execution_timeout': timedelta(hours=3)
    }

    dag = DAG(
        dag_id='demo',
        schedule_interval=None,
        catchup=False,                           # 當 backfill 時只執行最近一次
        max_active_runs=1,                       # DAG 最大同時執行數
        default_args=default_args,               # task level 參數
        description='if_polaris demo DAG，示範將顧客明細轉為顧客特徵 ETL DAG',  # 詳細描述這個 DAG 在做什麼
        access_control={
            'esun_default': {
                'can_read'
                # 'can_edit'
            }
        }
    )
    ###########################################
    #         GroupMonad Demo                 #
    ###########################################
    print('#### GroupMonad Demo:####')
    process = GroupProcess('process', dag=dag)
    print('run with airflow binded')
    print(process.run.__annotations__)
    return_obj = process.return_cls()
    return_obj.set_content(df)
    result = process.binded_run(return_obj, return_obj)
    print(result)
    print('pure run')
    print(process.run(df, df))
    print('run with decoration')
    result = process.decorated_run(df, df)
    print(result)
    ###########################################
    #         DagMonad Demo                   #
    ###########################################
    print('#### DagMonad Demo ####')
    dag_process = DagProcess()
    print(dag_process.run(df, df))
    
    