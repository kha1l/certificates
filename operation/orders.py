import requests
import fake_useragent


def get_order(rest_id, long_id, login, password, date):
    session = requests.Session()
    user = fake_useragent.UserAgent().random
    log_data = {
        'CountryCode': 'Ru',
        'login': login,
        'password': password
    }
    header = {
        'user-agent': user
    }
    log_link = 'https://auth.dodopizza.ru/Authenticate/LogOn'
    session.post(log_link, data=log_data, headers=header)
    orders_data = {
        'handover': {
            'link': 'https://officemanager.dodopizza.ru/Reports/OrderHandoverTime/Export',
            'data': {
                "unitsIds": long_id,
                "beginDate": date,
                "endDate": date,
                "orderTypes": "Delivery",
                "Export": "Экспорт+в+Excel"
            }
        },
        'orders': {
            'link': 'https://officemanager.dodopizza.ru/Reports/Orders/Export',
            'data': {
                "filterType": "OrdersFromPickupAndDelivery",
                "unitsIds": rest_id,
                "OrderSources": [
                    "Telephone",
                    "Site",
                    "Restaurant",
                    "DefectOrder",
                    "Mobile",
                    "Pizzeria",
                    "Aggregator"
                ],
                "beginDate": date,
                "endDate": date,
                "orderTypes": "Delivery"
            }
        },
        'delivery': {
            'link': 'https://officemanager.dodopizza.ru/Reports/CourierTasks/Export',
            'data': {
                "unitId": rest_id,
                "beginDate": date,
                "endDate": date,
                "statuses": [
                    "Queued",
                    "Ordering",
                    "Paused"
                ]
            }
        },
        'certificates': {
            'link': 'https://officemanager.dodopizza.ru/Reports/BeingLateCertificates/Export',
            'data': {
                "unitsIds": rest_id,
                "beginDate": date,
                "endDate": date
            }
        }
    }
    for order in orders_data:
        responce = session.post(orders_data[order]['link'], data=orders_data[order]['data'], headers=header)
        try:
            with open(f'./operation/export/{order}_{rest_id}.xlsx', 'wb') as file:
                file.write(responce.content)
                file.close()
        except FileNotFoundError as fne:
            print(str(fne))
    session.close()
