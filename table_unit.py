from pathlib import Path
import yaml
import logging
logging.basicConfig(level=logging.INFO)

class TableUnit:
    def __init__(self, table_id):
        self.addr=f'tables/{table_id}.yaml'

        logger = logging.getLogger(__name__)
        # check if the yaml file exists
        if not Path(self.addr).exists():
            logger.info('yaml file does not exist')
        else:
            with open(self.addr, 'r') as f:
                self.yaml_file = yaml.load(f)
    
    @property
    def columns(self):
        '''
        欄位列表
        '''
        
        return list(self.yaml_file['columns'])

    @property
    def column_name(self):
        '''
        欄位中文名稱
        '''

        res = {}
        for k in self.columns:
            res[k] = self.yaml_file['columns'][k]['cn_name']
        return res
    
    @property
    def column_types(self):
        '''
        欄位屬性
        '''

        res = {}
        for k in self.columns:
            res[k] = self.yaml_file['columns'][k]['type']
        return res
    
    @property
    def schema_name(self):
        '''
        Schema 名稱
        '''

        return self.yaml_file['schema_name']
    
    # table 名稱
    @property
    def table_name(self):
        '''
        table 名稱
        '''

        return self.yaml_file['table_name']
    
    @property
    def create_sql(self):
        '''
        建立 table sql
        '''

        query = f'CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name}('
        for k in self.columns:
            query += '\n\t{col_name}\t{col_type},'.format(
                col_name = k,
                col_type = self.yaml_file['columns'][k]['type']
            )
        query = query[:-1] + ');'

        if self.yaml_file['indexes']:
            query += '\nCREATE INDEX ON {schema_name}.{table_name} ({indexes});'.format(
                schema_name = self.schema_name,
                table_name = self.table_name,
                indexes = str(self.yaml_file['indexes'])[1:-1]
            )
        return query
   
    @property
    def select_sql(self):
        '''
        選取資料
        '''

        return f'SELECT {str(self.columns)[1:-1]} FROM {self.schema_name}.{self.table_name};'
    
    @property
    def drop_sql(self):
        '''
        丟棄table
        '''

        return f'DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}'

    # 提供權限
    @property
    def grant_sql(self):
        '''
        提供權限
        '''

        query = 'GRANT ALL ON {schema_name}.{table_name} TO {users}'.format(
            schema_name = self.schema_name,
            table_name = self.table_name,
            users = str(self.yaml_file['users'])[1:-1]
        )
        return query

'''
    def check(self):
        '''