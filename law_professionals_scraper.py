import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import numpy as np
import argparse
import time


def parse_elements_in_soup(soup, 
                           index = False, 
                           chinese_dialect_list = ['Chinese', 'Mandarin', 'Cantonese']):

    main_content = soup.find('div', id="main-content")

    # Select the header in main content

    main_content_header = main_content.find('header')

    '''information to export'''

    try:
        contact_name = main_content_header.find('h1').get_text()
    except:
        contact_name = np.nan

    try:
        profession = main_content_header.find('p').contents[0].strip()
    except:
        profession = np.nan

    try:
        date_admitted = main_content_header.find(
            'p').contents[1].get_text().strip()
    except:
        date_admitted = np.nan

    try:
        sra_id = main_content_header.find('dl').contents[3].get_text().strip()
    except:
        sra_id = np.nan

    try:
        profession_related = main_content_header.find(
            'dl').contents[5].get_text()
    except:
        profession_related = np.nan

    # Select the details in main content

    main_content_details = main_content.find('div', id='details-accordion')

    # First Section: Contact and Organisation

    section_contact = main_content_details.find('section')

    '''information to export'''

    try:
        contact_tel = section_contact.find(
            'dt', string='Tel:').find_next_sibling('dd').get_text()
    except:
        contact_tel = np.nan

    try:
        contact_email = section_contact.find(
            'dt', id='Email').find_next_sibling().get_text().strip()
    except:
        contact_email = np.nan

    try:
        contact_organisation = section_contact.find(
            'dt', string='Consultant at:').find_next_sibling('dd').find('a').get_text().strip()
    except:
        contact_organisation = np.nan

    try:
        contact_address = ' '.join([item.strip() for item in section_contact.find(
            'dt', string='Consultant at:').find_next_sibling('dd').contents[4:-4:2]])
    except:
        contact_address = np.nan

    try:
        contact_dx_code = section_contact.find(
            'dd', class_='feature highlight').get_text()
    except:
        contact_dx_code = np.nan

    try:
        roles_at_contact_organisation = section_contact.find(
            'dt', string='Roles at this organisation').find_next_sibling('dd').get_text().strip()
    except:
        roles_at_contact_organisation = np.nan

    # Third Section: Languages spoken

    try:
        languages_spoken_list = main_content_details.find(
            'div', id="languages-spoken-accordion").get_text().strip().split('\n')
        
        is_speaking_chinese = any(dialect in languages_spoken_list for dialect in chinese_dialect_list)
        languages_spoken = ', '.join(languages_spoken_list)
        
    except:
        is_speaking_chinese = False
        languages_spoken = np.nan
        
    if index:
        entry_index = index
    else:
        entry_index = np.nan

    data = {'entry_index': entry_index,
            'contact_name': contact_name,
            'profession': profession,
            'date_admitted': date_admitted,
            'sra_id': sra_id,
            'profession_related': profession_related,
            'contact_tel': contact_tel,
            'contact_email': contact_email,
            'contact_organisation': contact_organisation,
            'contact_address': contact_address,
            'contact_dx_code': contact_dx_code,
            'roles_at_contact_organisation': roles_at_contact_organisation,
            'languages_spoken': languages_spoken,
            'is_speaking_chinese': is_speaking_chinese}

    df_entry = pd.DataFrame.from_records([data])

    return df_entry


def save_entry_to_db(df_entry, table_name = 'law_professionals_table', dbname = 'law_professionals_db'):

    conn = sqlite3.connect(dbname + '.sqlite')
    df_entry.to_sql(name=table_name, con=conn, if_exists='append')
    conn.close()
    
    
def drop_table_in_db(table_name = 'law_professionals_table', dbname = 'law_professionals_db'):

    conn = sqlite3.connect(dbname + '.sqlite')   
    cursor = conn.cursor()
    
    dropTableStatement = "DROP TABLE " + table_name

    cursor.execute(dropTableStatement)
    conn.close()
    

def scrape_data_to_db(url, index=False):

    try:
        page = requests.get(url)

    except:

        entry_status = 3
        print('Error ', str(entry_status), ': \t',
              'Network connection failed!')

        return entry_status

    entry_status = 0

    # 0: 404,
    # 1: 200 + don't speak Chinese,
    # 2: 200 + speak Chinese,
    # 3: network connection issue,
    # 4: database connection issue, law_professionals_table,
    # 5: database connection issue, chinese_speaking_law_professionals_table

    if page.status_code == 200:

        entry_status += 1

        soup = BeautifulSoup(page.content, 'html.parser')

        df_entry = parse_elements_in_soup(soup, index=index)

        try:

            save_entry_to_db(df_entry, table_name='law_professionals_table')

        except:

            entry_status = 4
            print('Error ', str(entry_status), ': \t',
                  'Database connection issue, law_professionals_table!')

            return entry_status

        is_speaking_chinese = df_entry['is_speaking_chinese'].any()

        if is_speaking_chinese:

            entry_status += 1

            try:
                save_entry_to_db(
                    df_entry, table_name='chinese_speaking_law_professionals_table')

            except:

                entry_status = 5
                print('Error ', str(entry_status), ': \t',
                      'Database connection issue, chinese_speaking_law_professionals_table!')

    return entry_status


def scrape_pages_in_range(*arg,
                          trials=50,
                          reset_table_all=False,
                          reset_table_chinese=False,
                          base_url="https://solicitors.lawsociety.org.uk/person/"):

    if len(arg) == 0:

        range_start, range_end = 0, 10

    elif len(arg) == 1:

        range_start, range_end = 0, arg[0]

    else:

        range_start, range_end = arg[0], arg[1]

    if reset_table_all:

        try:
            drop_table_in_db(table_name='law_professionals_table')
        except:
            pass

    if reset_table_chinese:

        try:
            drop_table_in_db(
                table_name='chinese_speaking_law_professionals_table')
        except:
            pass

    status_labels = ['---', '-n-', '-y-']
    status_counts = [0, 0, 0]
    # [network, database_table_all, database_table_chinese]
    errors_statistic = [0, 0, 0]

    for index in range(range_start, range_end):

        url = base_url + str(index)

        while trials:

            entry_status = scrape_data_to_db(url, index=index)

            if entry_status < 3:

                break

            time.sleep(5)
            errors_statistic[entry_status-3] += 1
            trials -= 1
            print('Errors statistic: ', str(errors_statistic),
                  '\t', 'Remaining number of trials: ', str(trials))

        status_counts[entry_status] += 1

        print(str(index).rjust(6) + ":", '-'+ str(trials).zfill(2)+'-', 
              status_labels[entry_status], str(status_counts))
        

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Command Line Arguments Explanation!')
    
    parser.add_argument("range_params", type=int, nargs='*', help="Parameter(s) for the index range() function")
    
    parser.add_argument("-a", "--reset_table_all", default=False, action='store_true', help="Reset the table which has all the contacts")
    parser.add_argument("-c", "--reset_table_chinese", default=False, action='store_true', help="Reset the table which has all the Chinese speaking contacts")

    args = parser.parse_args()
    
    scrape_pages_in_range(*args.range_params, reset_table_all=args.reset_table_all, reset_table_chinese=args.reset_table_chinese)