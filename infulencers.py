from bs4 import BeautifulSoup as bs
import pandas as pd
import requests
import ssl
import os

# Pandas sometimes cannot read the data without SSL certificate
ssl._create_default_https_context = ssl._create_unverified_context

timestamp = f"{pd.Timestamp('today'):%Y-%m-%d %I-%M %p}"

cwd = os.getcwd()
csv_path = ''.join((cwd, timestamp, '.csv'))


def country_list():
    """helper function : get list of all countries in list format to scrape"""
    countries = []
    page = 'https://starngage.com/app/global/zone'
    req = requests.get(page)

    soup = bs(req.text, 'html.parser')
    find = soup.find_all('div', {'class': 'col-12'})
    for l in find:
        country_list = l.find_all(
            'li', {'class': 'list-inline-item col-6 col-lg-2'})
        for m in country_list:
            a_tag = m.find('a').text
            proper = a_tag.lower().replace(' ', '-')
            countries.append(proper)
    return countries


def counutry_tag_links():
    """helper function : joins countries with tags to make proper ULR to scrape and get data"""
    countries = country_list()
    link = 'https://starngage.com/app/global/influencer/ranking'
    tags = ['jewelry', 'jewelry-&-watches']
    main = []
    for each in countries:
        for individual in tags:
            clean = f'{link}/{each}/{individual}'
            main.append(clean)
    return main


def get_df():
    """Follow each URL, find data if any, and append to make master db"""
    first_df = pd.DataFrame()
    url_list = counutry_tag_links()
    for l in url_list:
        try:
            dfs = pd.read_html(l)
            first_df = first_df.append(dfs)
        except ValueError:
            pass
    return first_df


def clean_df():
    """Cleans the data : extract Instagram username & sperate name, remove unnamed & # columns, rearrange columns and remove duplicates before saving """
    inward_df = get_df()

    inward_df['Name'] = inward_df['@Username'].str.extract(
        r'(.*)@')
    inward_df['Username'] = inward_df['@Username'].str.extract(
        r'(@.*)')
    df = inward_df.drop(['#', '@Username'], axis=1)

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df[['Name', 'Username', 'Followers',
             'Engagement Rate', 'Country', 'Topics']]
    df = df.drop_duplicates()
    df.to_csv(csv_path, index=False)


if __name__ == '__main__':
    clean_df()
