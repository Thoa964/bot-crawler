import requests
from bs4 import BeautifulSoup
import csv
import subprocess

baseUrl = 'https://vieclam24h.vn'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}


def fetch_page_html(url):
    try:
        response = subprocess.check_output(['curl', url])
        response_text = response.decode('utf-8')
        return response_text
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        return None

def extract_job_details(soup):
    job_listings = soup.find_all(attrs={"data-track-content": 'true'})
    job_details = []

    for job in job_listings:
        job_detail_link = job['data-content-target']
        detail_response = requests.get(baseUrl + job_detail_link, headers=headers)
        soup = BeautifulSoup(detail_response.text, 'html.parser')
        
        main = soup.find('main')
        container = main.find('div', {'class': 'items-start'})
        
        try_time_ele = main.find('i', {'class': 'svicon-calendar-alt'})
        info_ele = container.find('h1').find_next_sibling().findAll('p')
        
        company = container.find('h3').text.strip()
        job_title = container.find('h1').text.strip()
        print("Getting information for " + job_title)
        salary = info_ele[1].text.strip() if info_ele and info_ele[1] else ''
        end_date = info_ele[3].text.strip() if info_ele and info_ele[3] else ''
        start_date = main.findAll('i', {'class': 'svicon-calendar-day'})[1].next_element.findAll('p')[1].text.strip()
        try_time = try_time_ele.next_element.findAll('p')[1].text.strip() if try_time_ele else '0'
        level = main.find('i', {'class': 'svicon-medal'}).next_element.findAll('p')[1].text.strip()
        hiring_number = main.find('i', {'class': 'svicon-users'}).next_element.findAll('p')[1].text.strip()
        work_type = main.find('i', {'class': 'svicon-hard-hat'}).next_element.findAll('p')[1].text.strip()
        exp_require = main.find('i', {'class': 'svicon-experience-user'}).next_element.findAll('p')[1].text.strip()
        JD = main.find('h4', string='Mô tả công việc').next_sibling
        job_description = ''.join(str(child) for child in JD.contents)
        JR = main.find('h4', string='Yêu cầu công việc').next_sibling
        job_requirement = ''.join(str(child) for child in JR.contents)
        JB = main.find('h4', string='Quyền lợi').next_sibling
        job_benefit = ''.join(str(child) for child in JB.contents)
        JL = main.find('h4', string='Địa điểm làm việc').next_sibling
        job_location = ''.join(str(child) for child in JL.contents)
        
        job_details.append({
            'job_title': job_title,
            'company': company,
            'end_date': end_date,
            'salary': salary,
            'start_date': start_date,
            'try_time': try_time,
            'level': level,
            'hiring_number': hiring_number,
            'work_type': work_type,
            'exp_require': exp_require,
            'job_description': job_description,
            'job_requirement': job_requirement,
            'job_benefit': job_benefit,
            'job_location': job_location
            })

    return job_details

def get_last_page(soup):
    last_page_element = soup.find_all(attrs={'data-page': True})[-1]
    last_page = int(last_page_element.text)
    return last_page

def read_fieldnames_from_file(filename):
    with open(filename, 'r') as file:
        fieldnames = file.read().strip().split(',')
    return fieldnames

def main():
    starting_url = 'https://vieclam24h.vn/viec-lam-da-nang-p104.html'
    output_csv_file = 'vieclam24h_danang.csv'
    fieldnames = read_fieldnames_from_file('fields.txt')
    page = 1

    while True:
        url = f'{starting_url}?page={page}'
        page_html = fetch_page_html(url)
        print(f"Reading page {page}")
        
        if page_html is None:
            a = 1
            # break

        soup = BeautifulSoup(page_html, 'html.parser')
        job_details = extract_job_details(soup)
        last_page = get_last_page(soup)

        if page == 1:
            with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

        with open(output_csv_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerows(job_details)

        page += 1
        if page > last_page:
            break

if __name__ == '__main__':
    main()
