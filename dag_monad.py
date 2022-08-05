from group import GroupMonad
import pandas as pd
class PDProcess(GroupMonad):
    def run(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        df_big = self.append_df(df1, df2)
        df_reset = self.reset_index(df_big)
        return df_reset

    def append_df(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        result = pd.concat([df1, df2])
        return result

    def reset_index(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.reset_index()
    
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
    process = PDProcess('process', dag=dag)
    print(process.run.__annotations__)
    return_obj = process.return_cls()
    return_obj.set_content(df)
    result = process.target_main_func(return_obj, return_obj)
    print(result)
    print(process.run(df, df))