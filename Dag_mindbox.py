from airflow import DAG

from airflow.operators.python_operator import PythonOperator

import datetime as dt

import requests as req

import pandas as pd

import json

import time

import os


class SimpleChClient:

    def __init__(self, CH_HOST, CH_CASERT):

        self.CH_HOST = CH_HOST

        self.CH_CASERT = CH_CASERT

        self.DBuser = 'js_u6_cl_user'

        self.DBpassw = 'QV8veWm7b8LQ'

    def get_clickhouse_data(self, query, connection_timeout=1500):

        r = req.post(self.CH_HOST, params={'query': query, 'user': self.DBuser, 'password': self.DBpassw},

                     timeout=connection_timeout, verify=self.CH_CASERT)

        if r.status_code == 200:

            return r.text

        else:

            raise ValueError(r.text)

    def upload(self, table, content, data_format='TabSeparatedWithNames'):

        content = content.to_csv(sep='\t', encoding='UTF-8', index=False, float_format='%.0f').replace('\r', '')

        content = content.encode('utf-8')

        query_dict = {

            'query': f'INSERT INTO {table} FORMAT {data_format}',

            'user': self.DBuser,

            'password': self.DBpassw

        }

        r = req.post(self.CH_HOST, data=content, params=query_dict, verify=self.CH_CASERT)

        result = r.text

        if r.status_code == 200:

            return result

        else:

            raise ValueError(r.text)


class MindboxReportProcessor:

    def __init__(self, ch_client):

        self.ch_client = ch_client

        self.key = None

        self.dict_ = {

            'ACTIVE_E-mail_Transaction_Напоминание о начале онлайн-регистрации за 24 часа: Общая c Билетиками': '24-hours-checkin-reminder-mail',

            'Active_E-mail_Transaction_Напоминание о скором вылете за 48 часов: Общая- с одной кнопкой на все услуги': '48-hours-services-upsale-mail',

            'Подборка выгодных билетов': 'promo-destinations-sale-20240320-happiness-day',

            'Летим в Стамбул': 'new-destination-dme-ist',

            'Летим в Пекин': 'new-destinations-pekin',

            '02.04 Подборка выгодных билетов': 'promo-destinations-sale-20240402-fresh-view-to-the-world',

            'Подборка выгодных SVX': 'promo-destinations-sale-20240410-profitable-to-fly-for-ekb',

            'Подборка выгодных без SVX': 'promo-destinations-sale-20240410-profitable-to-fly',

            '😇 Делать добро и передавать его в фонды': 'loyalty-program-charity-philanthropist-day',

            'Бизнес-класс в апреле': 'promo-destinations-sale-20240415-business-class',

            '🎁 Сделайте себе подарок в майские праздники': 'promo-destinations-sale-20240425-may-holidays-gift',

            'Предупреждение Домодедово': 'сheck-in-closes-60-minutes-before-departure',

            '💨 Успевайте путешествовать в майские праздники': 'promo-destinations-sale-20240305-have-time-to-travel-in-may-holidays',

            '👨‍👩‍👦 Выгодные билеты в международный день семьи': 'promo-destinations-sale-20240515-family-in-flight',

            '🤩 Собрали для вас самые популярные выгодные билеты': 'promo-destinations-sale-20240523-the-most-popular-discount-tickets-this-year',

            '🏄‍♂️ Врываемся в лето с новой подборкой выгодных билетов!': 'promo-destinations-sale-20240528-were-rushing-into-summer',

            '🏰 Летим в Калининград': 'promo-destinations-flying-to-kaliningrad',

            'Пекин от 8 400₽ и другие выгодные билеты!': 'mass-promo-routes-20240605-into-pekin',

            '🎁 Разыгрываем путешествие мечты вместе с сервисом бронирования «Отелло»': 'promo-otello-u6-telegram-travel-contest',

            '⚡️Снижаем цены на Стамбул для Екатеринбурга!': 'promo-destination-sale-20240607-stambul-time-to-save-money',

            'Новая подборка авиабилетов по выгодным ценам': 'promo-destinations-20240613-its-profitable-to-fly',

            '🌊 Ловите волну с новой подборкой выгодных билетов!': 'promo-destinations-sale-20240619-you-hunt-summer',

            '🌞 А вот и июль! Успевайте насладиться летом': 'promo-destinations-sale-20240627-time-for-best-tickets',

            'Travelty-IOS-приглашение установить': 'travelty-mail-segment-20231121',

            '🤗 Выгодные авиабилеты ко дню семьи, любви и верности!': 'promo-destinations-sale-20240704-family-day',

            'Ташкент от 11 741 ₽ и другие выгодные билеты!': 'promo-destinations-sale-20240712-flying-in-tashkent',

            'Ереван от 11 883 ₽ и другие выгодные билеты!': 'promo-destinations-sale-20240719-flying-in-erevan',

            '💨 Внезапная выгодная подборка топовых направлений': 'promo-destinations-sale-20240722-urgent-and-profitable',

            'Лето заканчивается, а выгодные билеты - нет!': 'promo-destinations-sale-20240816-last-summer-days',

            'Не упустите шанс попрощаться с летом со скидкой!': 'promo-destinations-sale-20240823-goodbye-summer',

            'Калининград от 2289 ₽ и еще много выгодных билетов!': 'promo-destinations-sale-20240828-flying-to-kaliningrad',

            'Перевернули календарь, а там новые осенние путешествия': 'promo-destinations-sale-20240906-new-autumn-travel',

            'Теплая осень с подборкой выгодных билетов': 'promo-destinations-sale-20240911-warm-autumn',

            '⚡️Снижаем цены на перелёты из Екатеринбурга! Только до 15 сентября': 'promo-destinations-sale-20240913-lightning',

            'Отправляйтесь в яркую осень с нашей подборкой выгодных авиабилетов!': 'promo-destinations-sale-20240920-bright-golden-autumn',

            'День Туризма — отличный повод приобрести авиабилеты!': 'promo-destinations-sale-20240927-happy-tourism-day',

            'Выгодные билеты во всемирный день улыбки': 'promo-destinations-sale-20241004-worldwide-smile-day',

            'Согревайтесь путешествиями!': 'promo-destinations-sale-20241011-keep-warm-with-travel'

        }

        self.dict2 = {

            'ACTIVE_E-mail_Transaction_Напоминание о начале онлайн-регистрации за 24 часа: Общая c Билетиками',

            'Active_E-mail_Transaction_Напоминание о скором вылете за 48 часов: Общая- с одной кнопкой на все услуги'

        }

    def convert(self, a):

        return self.dict_.get(a, '')

    def send_request(self):

        query = json.loads(

            req.post(

                url='https://api.mindbox.ru/v3/operations/sync?endpointId=Uralairlines.Export&operation=ExportDashboardMailingsDynamics',

                headers={

                    'Content-Type': 'application/json; charset=utf-8',

                    'Accept': 'application/json',

                    'Authorization': 'SecretKey APW6qy9hLY626cCo9S0Uzjlszd9UY8Ys'},

                json={

                    "sinceDate": "2023-06-01",

                    "tillDate": str(dt.date.today()-dt.timedelta(days=1))

                }).text)

        print(query['exportId'])

        return query['exportId']  # Возврат ID для использования в следующем запросе

    def wait_for_report(self, **kwargs):

        task_instance = kwargs['ti']

        export_id = task_instance.xcom_pull(task_ids='send_request')

        while True:

            result = json.loads(

                req.post(

                    'https://api.mindbox.ru/v3/operations/sync?endpointId=Uralairlines.Export&operation=ExportDashboardMailingsDynamics',

                    headers={

                        'Content-Type': 'application/json; charset=utf-8',

                        'Accept': 'application/json',

                        'Authorization': 'SecretKey APW6qy9hLY626cCo9S0Uzjlszd9UY8Ys'},

                    json={

                        'exportId': export_id

                    }).text)

            if result['exportResult']['processingStatus'] == 'Ready':
                url = result['exportResult']['urls'][0]

                break

            time.sleep(5)

        return url  # Возврат URL для скачивания отчета

    def download_csv(self, **kwargs):

        task_instance = kwargs['ti']

        url = task_instance.xcom_pull(task_ids='wait_for_report')

        resp = req.get(url)

        resp.encoding = 'UTF-8'

        with open('/home/ivan/airflow/dags/mindbox_report_procesing/file.csv', 'w', encoding='UTF-8') as file:
            for line in resp.text.split('\n'):
                file.write(line)

    def process_report(self):

        df = pd.read_csv('/home/ivan/airflow/dags/mindbox_report_procesing/file.csv', sep=';', encoding='UTF-8')

        df['ReportDate'] = pd.to_datetime(df['ReportDate'], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')

        df['ReportStartDate'] = pd.to_datetime(df['ReportStartDate'], format='%d.%m.%Y %H:%M').dt.strftime(

            '%Y-%m-%d %H:%M')

        df = df.drop(columns=['ReportOrders', 'ReportMessagesTotal', 'ReportDeliveryRate', 'ReportOpenRate',

                              'ReportClickRate', 'ReportCTOR', 'ReportConversionRate', 'ReportUnsubscribeRate',

                              'ReportSpamRate', 'ReportBounceRate', 'ReportConversions', 'ReportConversionsRevenue',

                              'ReportAverageOrderValue', 'ReportConversionRevenuePerRecipient', 'ReportSpam',

                              'ReportBounced', 'ReportTemplate', 'ReportFolder', 'ReportTags', 'ReportBrand',

                              'ReportHeatMap', 'ReportMailingVariantNum'])

        df['ReportRevenue'] = df['ReportMessageName'].apply(self.convert)

        df['ReportSubject'] = df['ReportMessageName'].apply(lambda x: 'email' if x in self.dict2 else '')

        df['ReportMessageLink'] = df['ReportMessageName'].apply(lambda x: 'booking' if x in self.dict2 else '')

        df.columns = [

            'date', 'mailing_name', 'utm_source', 'utm_medium', 'utm_campaign',

            'send', 'delivered', 'opened', 'clicked', 'unsubscribed',

            'start_date', 'channel', 'campaign', 'category', 'mailing_id'

        ]

        df = df.sort_values(by='date')

        return df  # Возврат DataFrame для загрузки

    def upload_to_clickhouse(self, **kwargs):

        df = kwargs['ti'].xcom_pull(task_ids='process_report')

        table = 'U6_web.mindbox_stats'

        self.ch_client.get_clickhouse_data(f'DROP TABLE IF EXISTS {table}')  # DROP TABLE

        self.ch_client.get_clickhouse_data(f'''

            CREATE TABLE {table} (

                date Date,

                mailing_name String,

                utm_source String,

                utm_medium String,

                utm_campaign String,

                send Int64,

                delivered Int64,

                opened Int64,

                clicked Int64,

                unsubscribed Int64,

                start_date String,

                channel String,

                campaign String,

                category String,

                mailing_id String

            ) ENGINE = MergeTree() 

            ORDER BY (date) 

            SETTINGS index_granularity=8192

        ''')  # CREATE TABLE

        self.ch_client.upload(table, df)  # UPLOAD


# Параметры DAG

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 10, 20),
    'retries': 1,
}

dag = DAG('mindbox_report_processing', default_args=default_args, schedule_interval='0 21 * * *')

# Создание экземпляра клиента SimpleChClient

ch_client = SimpleChClient(CH_HOST='https://rc1a-dqkj593ugddfodop.mdb.yandexcloud.net:8443',
                           CH_CASERT='/home/ivan/airflow/dags/mindbox_report_procesing/YandexInternalRootCA.crt')

report_processor = MindboxReportProcessor(ch_client)

# Определение задач

task1 = PythonOperator(

    task_id='send_request',

    python_callable=report_processor.send_request,

    dag=dag,

)

task2 = PythonOperator(

    task_id='wait_for_report',

    python_callable=report_processor.wait_for_report,

    provide_context=True,

    dag=dag,

)

task3 = PythonOperator(

    task_id='download_csv',

    python_callable=report_processor.download_csv,

    provide_context=True,

    dag=dag,

)

task4 = PythonOperator(

    task_id='process_report',

    python_callable=report_processor.process_report,

    dag=dag,

)

task5 = PythonOperator(

    task_id='upload_to_clickhouse',

    python_callable=report_processor.upload_to_clickhouse,

    provide_context=True,

    dag=dag,

)

# Определение порядка выполнения задач

task1 >> task2 >> task3 >> task4 >> task5
