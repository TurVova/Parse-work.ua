import requests, csv, random, time
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
from multiprocessing import Pool

BASIC_URL = 'https://www.work.ua'
UA = UserAgent()

def get_html(url, headers=None):
    resp = requests.get(url, headers=headers)
    return resp.text


def random_sleep():
    time.sleep(random.uniform(2, 5))


def get_total_pages(html):
    soup = bs(html, 'lxml')
    pages = soup.find('ul', class_='pagination').find_all('a')[-2].get('href')
    total_page = pages.split('=')[1]
    return int(total_page)


def get_all_cities(html):
    soup = bs(html, 'lxml')
    divs = soup.find('div', class_='emblems-search-by-cities'). \
        find_all('div', class_='col-xs-6 col-sm-3 col-md-2')
    links_cities = []
    for div in divs:
        a = div.find('a').get('href')
        link_city = BASIC_URL + a
        links_cities.append(link_city)
    return links_cities


def get_all_posts(html):
    soup = bs(html, 'lxml')
    divs = soup.find('div', class_='wordwrap'). \
        find_all('p', class_='overflow')
    links_posts = []
    for div in divs:
        a = div.find('a').get('href')
        link_post = BASIC_URL + a
        links_posts.append(link_post)
    return links_posts


def get_all_data(html):
    soup = bs(html, 'lxml')
    try:
        position = soup.find('h1', class_='wordwrap').text.strip()
    except:
        position = ''
    try:
        salary = soup.find('h3', class_='wordwrap').text.strip()
    except:
        salary = ''
    try:
        company = soup.find('dl', class_='dl-horizontal'). \
            find('b').text.strip()
    except:
        company = ''
    try:
        contacts = soup.find('dl', class_='dl-horizontal')('dd')[2].text.strip()
    except:
        contacts = ''
    try:
        city = soup.find('dl', class_='dl-horizontal')('dd')[3].text.strip()
    except:
        city = ''
    try:
        employment = soup.find('dl', class_='dl-horizontal')('dd')[4].text.strip()
    except:
        employment = ''
    try:
        requirements = soup.find('dl', class_='dl-horizontal')('dd')[5].text.strip()
    except:
        requirements = ''
    data = {'city': city,
            'salary': salary,
            'position': position,
            'company': company,
            'contacts': contacts,
            'employment': employment,
            'requirements': requirements,
            }
    return data


def write_csv(data):
    with open('work_ua.csv', 'a') as file:
        writer = csv.writer(file)
        writer.writerow((data['city'],
                         data['salary'],
                         data['position'],
                         data['company'],
                         data['contacts'],
                         data['employment'],
                         data['requirements'],
                         ))


def data_pool(url):
    headers = {'User-Agent': UA.random, }
    html = get_html(url, headers)
    data = get_all_data(html)
    write_csv(data)


def main():
    url = 'https://www.work.ua/jobs/by-region/'
    html = get_html(url)
    all_cities = get_all_cities(html)
    for url_city in all_cities:
        page_part = '?page='
        html = get_html(url_city)
        total_page = get_total_pages(html)
        for url in range(1, total_page):
            url_gen = url_city + page_part + str(url)
            html = get_html(url_gen)
            all_posts = get_all_posts(html)
            print(url_gen)
            with Pool(14) as p:
                p.map(data_pool, all_posts)
                random_sleep()
                


if __name__ == '__main__':
    main()
