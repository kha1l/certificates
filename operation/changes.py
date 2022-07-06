import pandas as pd
from datetime import timedelta


def get_changes(rest_id):
    df_order = pd.read_excel(f'./operation/export/orders_{rest_id}.xlsx', skiprows=7)
    df_delivery = pd.read_excel(f'./operation/export/delivery_{rest_id}.xlsx', skiprows=4)
    df_cert = pd.read_excel(f'./operation/export/certificates_{rest_id}.xlsx', skiprows=4)
    df_handover = pd.read_excel(f'./operation/export/handover_{rest_id}.xlsx', skiprows=6)
    if df_cert.shape[0] != 0:
        df_order = df_order[['Время', '№ заказа', 'Статус заказа', 'Курьер', 'Адрес']]
        df_order['№ заказа'] = df_order['№ заказа'].apply(lambda x: x.split(' - ')[0])
        df_order['Время'] = df_order['Время'].apply(lambda x: str(x).split(' ')[-1])
        df_cert = df_cert[
            ['Дата и время', '№ заказа', 'Крайнее время доставки', 'Примерный срок доставки', 'Отметка курьера']]
        df_cert['Дата и время'] = df_cert['Дата и время'].apply(lambda x: x.split(' ')[-1])
        df_cert.rename(columns={'Дата и время': 'Время', '№ заказа': 'orders'}, inplace=True)

        def change(t):
            tm = t.split(':')
            if len(tm[0]) != 1:
                pass
            else:
                tm[0] = '0' + tm[0]
            return ':'.join(tm)

        df_cert['Время'] = df_cert['Время'].apply(change)
        counter = 0
        for i in df_delivery['Начало']:
            df_type = df_delivery.loc[df_delivery['Начало'] == i]
            if df_type.iloc[0]['Тип'] == 'На заказе':
                df_time = df_delivery.loc[df_delivery['Окончание'] == i]
                try:
                    time = df_time.iloc[0]['Начало']
                except IndexError:
                    time = 0
                df_delivery.at[counter, 'Постановка в очередь'] = time
            else:
                df_delivery.at[counter, 'Постановка в очередь'] = 0
            counter += 1
        df_delivery = df_delivery.loc[df_delivery['Тип'] == 'На заказе']

        df_orders_rec = pd.DataFrame()
        counter = 0
        for i in df_delivery['№ заказа']:
            try:
                m = i.split(', ')
            except AttributeError:
                m = []
            for j in m:
                df_orders_rec = pd.concat([df_orders_rec, df_delivery[df_delivery['№ заказа'] == i]], ignore_index=True)
                df_orders_rec.at[counter, 'orders'] = j
                counter += 1
        df_orders_rec = df_orders_rec[
            ['Начало', 'Длительность', 'Прогнозное время', '№ заказа', 'Количество заказов', 'Постановка в очередь',
            'orders']]
        df_orders_rec['orders'] = pd.to_numeric(df_orders_rec['orders'], errors='coerce')
        df_orders_rec['Постановка в очередь'] = df_orders_rec['Постановка в очередь'].apply(lambda x: str(x).split(' ')[-1])

        def deltatime(t):
            if t < timedelta(0):
                t = timedelta(days=1) - t
                t = str(t).split(' ')[-1]
                t = '-' + t
            else:
                t = str(t).split(' ')[-1]
            return t

        def deltatimedelta(x):
            try:
                x = timedelta(hours=x.hour, minutes=x.minute, seconds=x.second)
            except AttributeError:
                x = timedelta(0)
            return x

        df_orders_rec['Длительность'] = df_orders_rec['Длительность'].apply(deltatimedelta)
        df_orders_rec['Прогнозное время'] = df_orders_rec['Прогнозное время'].apply(deltatimedelta)

        df_orders_rec['Разница поездки'] = df_orders_rec['Прогнозное время'] - df_orders_rec['Длительность']
        df_orders_rec['Разница поездки'] = df_orders_rec['Разница поездки'].apply(deltatime)

        df_orders = df_order[df_order['Статус заказа'] == 'Просрочен']
        df_res = df_orders.merge(df_cert, on='Время', how='left')
        df_res = df_res[['Курьер', 'orders', 'Крайнее время доставки', 'Примерный срок доставки', 'Отметка курьера']]
        df_total = df_res.merge(df_orders_rec, on='orders', how='left')
        df_total['Начало'] = df_total['Начало'].apply(lambda x: str(x).split(' ')[-1])
        counter = 0
        for i in df_total['№ заказа']:
            try:
                m = i.split(', ')
            except AttributeError:
                m = []
            adrs = ''
            for j in m:
                df_adrs = df_order.loc[df_order['№ заказа'] == j]
                a = df_adrs.iloc[0]['Адрес']
                adrs += a + '\n'
            df_total.at[counter, 'Адрес'] = adrs
            counter += 1
        df_total = df_total[
            ['Курьер', 'orders', 'Крайнее время доставки', 'Примерный срок доставки',
            'Отметка курьера', 'Постановка в очередь', 'Начало', 'Длительность',
            'Прогнозное время', 'Разница поездки', 'Количество заказов', 'Адрес']
        ]
        df_total.rename(
            columns={'orders': 'Номер заказа', 'Начало': 'Начало поездки', 'Длительность': 'Длительность поездки',
                    'Прогнозное время': 'Прогнозное время поездки', 'Количество заказов': 'Количество заказов в поездке'},
            inplace=True)
        df_handover = df_handover[['Номер заказа', 'Дата и время', 'Ожидание', 'Приготовление', 'Ожидание на полке']]
        df_handover['Номер заказа'] = df_handover['Номер заказа'].apply(lambda x: x.split('-')[0])
        df_handover['Дата и время'] = df_handover['Дата и время'].apply(lambda x: x.replace(microsecond=0))
        df_handover['Ожидание'] = df_handover['Ожидание'].apply(deltatimedelta)
        df_handover['Приготовление'] = df_handover['Приготовление'].apply(deltatimedelta)
        df_handover['Ожидание на полке'] = df_handover['Ожидание на полке'].apply(deltatimedelta)
        df_handover['Ожидание на полке'] = df_handover['Ожидание на полке'].apply(
            lambda x: str(x).split(' ')[-1])
        df_handover['Приготовление заказа'] = df_handover['Ожидание'] + df_handover['Приготовление']
        df_handover['Приготовление заказа'] = df_handover['Приготовление заказа'].apply(lambda x: str(x).split(' ')[-1])
        df_handover['Дата и время'] = df_handover['Дата и время'].apply(lambda x: str(x).split(' ')[-1])
        df_handover = df_handover[['Номер заказа', 'Дата и время', 'Приготовление заказа', 'Ожидание на полке']]
        df_handover['Номер заказа'] = df_handover['Номер заказа'].astype('int64')
        df_certificates = df_total.merge(df_handover, on='Номер заказа', how='left')
        df_certificates = df_certificates.fillna('0')
        df_certificates = df_certificates[
            ['Курьер', 'Номер заказа', 'Дата и время',
            'Приготовление заказа', 'Ожидание на полке',
            'Постановка в очередь', 'Начало поездки', 'Прогнозное время поездки',
            'Длительность поездки', 'Разница поездки', 'Крайнее время доставки', 'Примерный срок доставки',
            'Отметка курьера', 'Количество заказов в поездке', 'Адрес']]
        df_certificates['Прогнозное время поездки'] = df_certificates['Прогнозное время поездки'].apply(
            lambda x: str(x).split(' ')[-1])
        df_certificates['Длительность поездки'] = df_certificates['Длительность поездки'].apply(
            lambda x: str(x).split(' ')[-1])
        return df_certificates
    else:
        df_certificates = pd.DataFrame()
        return df_certificates
