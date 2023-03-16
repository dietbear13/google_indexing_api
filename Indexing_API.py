import json
import time
import traceback
from xlsxwriter import Workbook

import httplib2
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

SCOPES = ["https://www.googleapis.com/auth/indexing"]
ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

key = 'YOUR_KEY'  # УКАЗЫВАЕМ СВОЙ КЛЮЧ
project_name = 'YOUR_NAME'  # УКАЗЫВАЕМ НАЗВАНИЕ ПРОЕКТА


table_with_urls_for_recrawl = pd.read_excel(f'{project_name}.xlsx')
def send_pages_to_google_for_recrawl(data):
    json_key_file = f"{key}.json"
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key_file,
                                                                   scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())

    sent_urls_for_recrawl_set = set()
    for url in data:

        urls = {
            'url': '{}'.format(url),
                'type': 'URL_UPDATED'
        }

        response, content = http.request(ENDPOINT, method="POST",
                                         body=json.dumps(urls))

        time.sleep(1)

        if response['status'] != '200':
            # raise Exception("invalid status")
            break

        sent_urls_for_recrawl_set.add(url)

    sent_urls_set_len = len(sent_urls_for_recrawl_set)
        if sent_urls_set_len != 0:
        print(f"На переобход отправлено: {sent_urls_set_len} страниц")
        with open(f'{project_name}_logs.txt', 'a') as file:
            file.write(f"\n{date} — на переобход отправлено: {sent_urls_set_len} страниц")
    else:
        print(f"На переобход отправлено: {sent_urls_set_len} страниц")
        with open(f'{project_name}_logs.txt', 'a') as file:
            file.write(f"\n{date} — {sent_urls_set_len} страниц в таблице {project_name}.xlsx или сработал лимит")

   
    return sent_urls_for_recrawl_set


def delete_sent_urls_and_export_new_table(main_urls_set, sent_urls_for_recrawl_set):
    main_urls_set_without_sent_urls = main_urls_set - sent_urls_for_recrawl_set

    rest_urls_list = list(main_urls_set_without_sent_urls)

    data = pd.DataFrame({'urls': rest_urls_list})

    export_data_to_excel(data)


def send_pages_to_google(data):
    main_urls_set = set(data['urls'].to_list())

    sent_urls_for_recrawl_set = send_pages_to_google_for_recrawl(main_urls_set)

    delete_sent_urls_and_export_new_table(main_urls_set, sent_urls_for_recrawl_set)


def error_report():
    print('Ошибка')
    traceback.print_exc()


def export_data_to_excel(data):
    project_report = pd.ExcelWriter(f'{project_name}.xlsx', engine='xlsxwriter')
    data.to_excel(project_report, index=False)
    project_report.save()

send_pages_to_google(table_with_urls_for_recrawl)
