from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
import clickhouse_connect
from _datetime import datetime, timedelta, time
import pandas as pd
import requests
import json
import time

class SimpleCHClient:
    def __init__(self) -> None:
        self.host = 'rc1a-dqkj593ugddfodop.mdb.yandexcloud.net'
        self.port = 8443
        self.database = 'U6_web'
        self.username = 'js_u6_cl_user'
        self.password = 'QV8veWm7b8LQ'
        self.ca_cert = '/home/ivan/airflow/dags/mindbox_report_procesing/YandexInternalRootCA.crt'
        self.clickhouse_table = 'ym_orders'

    def connect_to_clickhouse(self):
        return clickhouse_connect.get_client(
            database=self.database,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            ca_cert=self.ca_cert
        )

    def send_sql_query(self, query):
        client = self.connect_to_clickhouse()
        result = client.query(query)
        result_rows = result.result_rows
        return result_rows

    def upload(self, df):
        client = self.connect_to_clickhouse()
        client.insert(self.clickhouse_table, df)


class Main:
    def __init__(self, ch_client) -> None:
        self.ch_client = ch_client
        self.API_HOST = 'https://api-metrika.yandex.ru'
        self.COUNTER_ID = '23062705'
        self.SOURCE = 'visits'
        self.header_dict = {'Authorization': f'OAuth y0_AgAAAAAuSQAYAAWW4AAAAAEEMeXIAABfGgsWyT1MVauR5G_Yl-aJJvbFzA',
                            'Content-Type': 'application/x-yametrika+json'
                            }
        self.API_FIELDS = (
            'ym:s:clientID',
            'ym:s:counterUserIDHash',
            'ym:s:visitID',
            'ym:s:date',
            f'ym:s:lastsignTrafficSource',
            f'ym:s:lastsignAdvEngine',
            'ym:s:UTMMedium',
            'ym:s:UTMSource',
            'ym:s:UTMCampaign',
            'ym:s:goalsID',
            'ym:s:goalsSerialNumber',
            'ym:s:productsBrand',
            'ym:s:productsCategory1',
            'ym:s:productsCategory2',
            'ym:s:productsID',
            'ym:s:productsName',
            'ym:s:productsPrice',
            'ym:s:productsPurchaseID',
            'ym:s:productsQuantity',
            'ym:s:productsVariant',
            'ym:s:purchaseID',
            'ym:s:purchaseAffiliation',
            'ym:s:purchaseProductQuantity',
            'ym:s:purchaseRevenue',
            'ym:s:purchaseDateTime'
        )

    def create_request(self):
        start_date = ch_client.send_sql_query('select max(date) + INTERVAL 1 day from ym_orders')
        start_date = start_date[0][0]
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        end_date = yesterday.strftime('%Y-%m-%d')
        date1 = pd.to_datetime(start_date)
        date2 = pd.to_datetime(end_date)
        if date2 >= date1:
            url_params = dict(
                date1=start_date,
                date2=end_date,
                source=self.SOURCE,
                fields=','.join(self.API_FIELDS)
            )
            url = f'{self.API_HOST}/management/v1/counter/{self.COUNTER_ID}/logrequests'
            try:
                r = requests.post(url, headers=self.header_dict, params=url_params)
                json.loads(r.text)['log_request']
                request_id = json.loads(r.text)['log_request']['request_id']
            except:
                print('⮾', end=' ')
                print('Ошибка на этапе создания запроса', end=' ')
                print(r.text)
            return (request_id)
        else:
            print('⮾', end=' ')
            print('Данные уже обновлены', end=' ')

    def wait_answer(self, **kwargs):
        try:
            task_instance = kwargs['ti']
            request_id = task_instance.xcom_pull(task_ids='create_request')
            status = 'created'
            while status == 'created':
                time.sleep(10)
                url = f'{self.API_HOST}/management/v1/counter/{self.COUNTER_ID}/logrequest/{request_id}'
                r = requests.get(url, headers=self.header_dict)
                if r.status_code == 200:
                    status = json.loads(r.text)['log_request']['status']
                else:
                    raise (BaseException(r.text))
        except:
            print('⮾', end=' ')
            print('Ошибка на этапе ожидания ответа LogsAPI')
        return (url)

    def request_a_file(self, **kwargs):
        task_instance = kwargs['ti']
        url = task_instance.xcom_pull(task_ids='wait_answer')
        try:
            r = requests.get(url, headers=self.header_dict)
            parts = json.loads(r.text)['log_request']['parts']
        except Exception as ex:
            print('⮾', end=' ')
            print('Ошибка на этапе запроса к готовому файлу')
            try:
                print(r.text)
            except:
                print(ex)
        return parts

    def upload_file(self, **kwargs):
        task_instance = kwargs['ti']
        request_id = task_instance.xcom_pull(task_ids='create_request')
        parts = task_instance.xcom_pull(task_ids='request_a_file')
        try:
            df = []
            for i in parts:
                url = f'{self.API_HOST}/management/v1/counter/{self.COUNTER_ID}/logrequest/{request_id}/part/{i["part_number"]}/download'
                r = requests.get(url, headers=self.header_dict)
                arr = r.text.strip().split('\n')
                for j in arr:
                    df.append(j.strip().replace(r'\s*', '').split('\t'))
        except:
            print('⮾', end=' ')
            print('Ошибка на этапе загрузки файлов')
        return df

    def uploading_to_clickhouse(self, **kwargs):
        task_instance = kwargs['ti']
        df = task_instance.xcom_pull(task_ids='upload_file')
        columns = [
            'clientID',
            'counterUserIDHash',
            'visitID',
            'date',
            'lastsignTrafficSource',
            'lastsignAdvEngine',
            'UTMMedium',
            'UTMSource',
            'UTMCampaign',
            'goalsID',
            'goalsSerialNumber',
            'productsBrand',
            'productsCategory1',
            'productsCategory2',
            'productsID',
            'productsName',
            'productsPrice',
            'productsPurchaseID',
            'productsQuantity',
            'productsVariant',
            'purchaseID',
            'purchaseAffiliation',
            'purchaseProductQuantity',
            'purchaseRevenue',
            'purchaseDateTime'
        ]
        df = pd.DataFrame(df[1:])
        df.columns = columns
        df = df.loc[(df['purchaseID'] != '[]')]
        df.purchaseDateTime = df['purchaseDateTime'].apply(lambda x: x.replace('\\', ''))
        df['productsName'] = df['productsName'].apply(lambda x: x.replace('\"', ''))
        def upload(table, content, data_format='TabSeparatedWithNames'):
            content = content.to_csv(sep='\t', encoding='UTF-8', index=False).replace('\r', '')
            content = content.encode('utf-8')
            query_dict = {
                'query': 'INSERT INTO {table} FORMAT {data_format} '.format(table=table, data_format=data_format),
                'user': 'js_u6_cl_user',
                'password': 'QV8veWm7b8LQ'
            }
            r = requests.post(f'https://rc1a-dqkj593ugddfodop.mdb.yandexcloud.net:8443', data=content, params=query_dict, verify='/home/ivan/airflow/dags/mindbox_report_procesing/YandexInternalRootCA.crt')
            result = r.text
            if r.status_code == 200:
                return result
            else:
                raise ValueError(r.text)
        try:
            upload(f'U6_web.ym_orders', df)
        except Exception as ex:
            df.to_csv('df.csv', sep=';')
            print('⮾', end=' ')
            print(f'Ошибка на этапе выгрузки готового файла в базу данных: {ex}')
        print('✓')

    def create_orders_new(self):
        insert_query = '''
        insert into U6_web.orders_new
        select 
        max(visitID) as visitID, 
        max(clientID) as clientID,
        max(PNR) as PNR,
        max(surname) as LastName,
        max(orderID) as PaymentID,
        max(lastsignTrafficSource) as TrafficSource,
        max(UTMSource) as UTMSource,
        max(UTMCampaign) as UTMCampaign,
        countIf(productsCategory = 'ticket') as ticket,
        countIf(productsCategory = 'seat') as seat,
        countIf(productsCategory = 'meal') as meal,
        countIf(productsCategory = 'luggage') as luggage,
        countIf(productsCategory = 'alfaInsurance') as alfaInsurance,
        countIf(productsCategory = 'sealineInsurance') as sealineInsurance,
        countIf(productsCategory = 'alfaHealth') as alfaHealth,
        countIf(productsCategory = 'sberhealth') as sberhealth,
        countIf(productsCategory = 'aeroexpress') as aeroexpress,
        countIf(productsCategory = 'pets') as pets,
        countIf(productsCategory = 'sportEquipment') as sportEquipment,
        countIf(productsCategory = 'weapon') as weapon,
        countIf(productsCategory = 'packing') as packing,
        countIf(productsCategory = 'businessLounge') as businessLounge,
        countIf(productsCategory = 'autoCheckin') as autoCheckin,
        countIf(productsCategory = 'smsNotifications') as smsNotifications,
        countIf(productsCategory = 'timeToThink') as timeToThink,
        countIf(productsCategory = 'unaccompaniedMinor') as unaccompaniedMinor,
        max(purchaseRevenue1) as Revenue,
        first_value(splitByChar('-', direction)[1]) as departure,
        first_value(splitByChar('-', direction)[2]) as arrival,
        max(variant) as tariff,
        max(RTorOW) as type,
        max(date(purchaseDateTime1)) as purchaseDate,
        max(splitByChar(' ', toString(purchaseDateTime1))[2]) as purchaseTime
        from (
          select 
          splitByChar('&', productsPurchaseID1)[1] as orderID,
          splitByChar('&', productsPurchaseID1)[2] as PNR,
          splitByChar('&', productsPurchaseID1)[3] as surname,
          case
              when direction != '' then
                case
                  when has(productsID, concat(splitByChar('-', direction)[2], '-', splitByChar('-', direction)[1])) 
                  then 'RT'
                  else 'OW'
              end
            else ''
          end as RTorOW,
          * from (
            select  
            arrayElement(productsCategory1, purchsID2) as productsCategory,
            case
                when arrayElement(productsCategory1, purchsID2) = 'ticket' 
                then arrayElement(productsID, purchsID2)
                else ''
            end as direction,
            case
                when arrayElement(productsCategory1, purchsID2) = 'ticket' 
                then arrayElement(productsVariant, purchsID2)
                else ''
            end as variant,
            *
            from (
              select count(*) over (partition by visitID order by (select NULL) rows between unbounded preceding and current row) as purchsID2,
              * from (
                select
                arrayElement(purchaseRevenue, purchsID) as purchaseRevenue1,
                arrayElement(purchaseDateTime, purchsID) as purchaseDateTime1,
                arrayJoin(productsPurchaseID) as productsPurchaseID1,
                *
                from (
                  select count(*) over (partition by visitID order by (select NULL) rows between unbounded preceding and current row) as purchsID, * from (
                    select visitID, clientID, lastsignTrafficSource, UTMSource, UTMCampaign, date, productsCategory1, productsID, productsPurchaseID, productsVariant, productsQuantity, purchaseID, purchaseRevenue, purchaseDateTime, arrayJoin(purchaseID) as purchaseID2 from U6_web.ym_orders
                  )
                )
                where productsPurchaseID1 = purchaseID2
              )
            )
          )
          WHERE PNR != '' 
        )
        group by productsPurchaseID1
        '''
        create_query = '''
        create table U6_web.orders_new (
            visitID Int64,
            clientID Int64,
            PNR String,
            LastName String,
            PaymentID String,
            TrafficSource String,
            UTMSource String,
            UTMCampaign String,
            ticket Int8,
            seat Int8,
            meal Int8,
            luggage Int8,
            alfaInsurance Int8,
            sealineInsurance Int8,
            alfaHealth Int8,
            sberhealth Int8,
            aeroexpress Int8,
            pets Int8,
            sportEquipment Int8,
            weapon Int8,
            packing Int8,
            businessLounge Int8,
            autoCheckin Int8,
            smsNotifications Int8,
            timeToThink Int8,
            unaccompaniedMinor Int8,
            Revenue Float64,
            departure String,
            arrival String,
            tariff String,
            type String,
            purchaseDate Date,
            purchaseTime String
            
        ) ENGINE = MergeTree() ORDER BY (visitID) SETTINGS index_granularity=8192
        '''
        drop_query = '''
        drop table U6_web.orders_new
        '''

        ch_client.send_sql_query(drop_query)
        ch_client.send_sql_query(create_query)
        ch_client.send_sql_query(insert_query)


# Параметры DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 20),
    'retries': 1,
}

dag = DAG('ym_orders_and_new_report_processing', default_args=default_args, schedule_interval='0 21 * * 6')

# Создание экземпляра клиента SimpleChClient

ch_client = SimpleCHClient()
report_processor = Main(ch_client)

# Определение задач

task1 = PythonOperator(

    task_id='create_request',

    python_callable=report_processor.create_request,

    dag=dag,

)

task2 = PythonOperator(

    task_id='wait_answer',

    python_callable=report_processor.wait_answer,

    provide_context=True,

    dag=dag,

)

task3 = PythonOperator(

    task_id='request_a_file',

    python_callable=report_processor.request_a_file,

    provide_context=True,

    dag=dag,

)

task4 = PythonOperator(

    task_id='upload_file',

    python_callable=report_processor.upload_file,

    dag=dag,

)

task5 = PythonOperator(

    task_id='uploading_to_clickhouse',

    python_callable=report_processor.uploading_to_clickhouse,

    provide_context=True,

    dag=dag,
)

task6 = PythonOperator(

    task_id='create_orders_new',

    python_callable=report_processor.create_orders_new,

    provide_context=True,

    dag=dag,
)

# Определение порядка выполнения задач

task1 >> task2 >> task3 >> task4 >> task5 >> task6
