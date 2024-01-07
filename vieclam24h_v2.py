import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import csv
import subprocess
import mysql.connector

baseUrl = 'https://vieclam24h.vn'
masothue_url = 'https://masothue.com/Search'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/58.0.3029.110 Safari/537.3'
}

# MySQL database configuration
mysql_config = {
    'host': '0.0.0.0',
    'port': 3308,
    'user': 'root',
    'password': 'secret',
    'database': 'jobs_board_dana'
}


def parse_date(date_str, input_format='%d/%m/%Y', output_format='%Y-%m-%d'):
    if date_str:
        return datetime.strptime(date_str, input_format).strftime(output_format)
    return None


def fetch_page_html(url):
    try:
        response = subprocess.check_output(['curl', url])
        response_text = response.decode('utf-8')
        return response_text
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        return None


def get_by_name(name, table, column, cursor):
    name_pattern = f"%{name}%"
    query = f'SELECT * FROM {table} WHERE {column} LIKE %s'
    cursor.execute(query, (name_pattern,))
    result = cursor.fetchone()
    return result


def import_to_tbl_bai_dang(job_details, user, nganh_nghe, cursor):
    ma_nganh_nghe = nganh_nghe['ma_nganh_nghe']
    tai_khoan = user['tai_khoan']
    thoi_gian_bat_dau = parse_date(job_details.get('ngay_bat_dau', ''))
    thoi_gian_ket_thuc = parse_date(job_details.get('ngay_ket_thuc', ''))
    query = """
        insert into tbl_bai_dang (ma_nganh_nghe, tai_khoan, tieu_de, mo_ta, dia_diem_lam_viec, thoi_gian_bat_dau,
                          thoi_gian_ket_thuc, kinh_nghiem, hinh_thuc_lam_viec, chuc_vu, so_luong, yeu_cau_ung_vien,
                          quyen_loi, cach_thuc_ung_tuyen, job_cao_du_lieu, trang_thai)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, 1)
    """

    data = (
        ma_nganh_nghe,
        tai_khoan,
        job_details.get('tieu_de', ''),
        job_details.get('mo_ta', ''),
        job_details.get('dia_chi', ''),
        thoi_gian_bat_dau,
        thoi_gian_ket_thuc,
        job_details.get('yeu_cau_kinh_nghiem', ''),
        job_details.get('hinh_thuc_lam_viec', ''),
        job_details.get('chuc_vu', ''),
        job_details.get('so_luong', ''),
        job_details.get('yeu_cau_ung_vien', ''),
        job_details.get('quyen_loi', ''),
        job_details.get('cach_thuc_ung_tuyen', '')
    )

    cursor.execute(query, data)


def generate_laravel_bcrypt():
    return '$2y$12$oAmAyV34K.G5cYQSk21diOfwy5qSH/j1OoINWDl7mEGnwsanWXsSK'  # Convert bytes to string


def create_tai_khoan(job, cursor):
    current_timestamp = int(time.time())
    email = f'company_{current_timestamp}@vieclam24h.vn'

    while get_by_name(email, 'tbl_tai_khoan', 'tai_khoan', cursor):
        current_timestamp = int(time.time())
        email = f'company_{current_timestamp}@vieclam24h.vn'

    ten = job.get('ten_cong_ty', '')
    dia_chi = job.get('ten_cong_ty', '')
    ten_cong_ty = job.get('ten_cong_ty', '')
    mat_khau = generate_laravel_bcrypt()

    query = """INSERT INTO tbl_tai_khoan (tai_khoan, ten, email, mat_khau, ma_quyen, trang_thai, dia_chi, ten_cong_ty, created_at)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())"""

    data = (email, ten, email, mat_khau, 3, 1, dia_chi, ten_cong_ty)
    try:
        cursor.execute(query, data)
        return get_by_name(ten, 'tbl_tai_khoan', 'ten', cursor)
    except Exception as e:
        print(f"Error creating tai_khoan: {e}")
        return None


def create_nganh_nghe(ten_nganh_nghe, cursor):
    query = """INSERT INTO tbl_nganh_nghe (ten_nganh_nghe, is_crawl_data) VALUES(%s, 1)"""
    data = (ten_nganh_nghe,)
    cursor.execute(query, data)
    return get_by_name(ten_nganh_nghe, 'tbl_nganh_nghe', 'ten_nganh_nghe', cursor)


def extract_job_details(soup, connection):
    job_listings = soup.find_all(attrs={"data-track-content": 'true'})
    job_details = []

    for job in job_listings:
        with connection.cursor(dictionary=True, buffered=True) as cursor:
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
            start_date = main.findAll('i', {'class': 'svicon-calendar-day'})[1].next_element.findAll('p')[
                1].text.strip()
            try_time = try_time_ele.next_element.findAll('p')[1].text.strip() if try_time_ele else '0'
            level = main.find('i', {'class': 'svicon-medal'}).next_element.findAll('p')[1].text.strip()
            hiring_number = main.find('i', {'class': 'svicon-users'}).next_element.findAll('p')[1].text.strip()
            work_type = main.find('i', {'class': 'svicon-hard-hat'}).next_element.findAll('p')[1].text.strip()
            exp_require = main.find('i', {'class': 'svicon-experience-user'}).next_element.findAll('p')[1].text.strip()
            job_nganh_nghe = main.find('i', {'class': 'svicon-suitcase'}).next_element.findAll('p')[1].text.strip()
            JD = main.find('h4', string='Mô tả công việc').next_sibling
            job_description = ''.join(str(child) for child in JD.contents)
            JR = main.find('h4', string='Yêu cầu công việc').next_sibling
            job_requirement = ''.join(str(child) for child in JR.contents)
            JB = main.find('h4', string='Quyền lợi').next_sibling
            job_benefit = ''.join(str(child) for child in JB.contents)
            JL = main.find('h4', string='Địa điểm làm việc').next_sibling
            job_location = ''.join(str(child) for child in JL.contents)
            company_location = soup.find('div', {'id': 'test'}).find('i', {
                'class': 'svicon-map-marker-alt'}).parent.next_sibling.text.strip()

            job = {
                'tieu_de': job_title,
                'ten_cong_ty': company,
                'ngay_ket_thuc': end_date,
                'muc_luong': salary,
                'ngay_bat_dau': start_date,
                'thoi_gian_thu_viec': try_time,
                'chuc_vu': level,
                'so_luong': hiring_number,
                'hinh_thuc_lam_viec': work_type,
                'yeu_cau_kinh_nghiem': exp_require,
                'mo_ta': job_description,
                'yeu_cau_ung_vien': job_requirement,
                'quyen_loi': job_benefit,
                'dia_chi': job_location,
                'nganh_nghe': job_nganh_nghe,
                'dia_chi_cong_ty': company_location
            }

            tai_khoan = get_by_name(company, 'tbl_tai_khoan', 'ten', cursor)
            nganh_nghe = get_by_name(job.get('nganh_nghe', ''), 'tbl_nganh_nghe', 'ten_nganh_nghe', cursor)

            if not tai_khoan:
                tai_khoan = create_tai_khoan(job, cursor)

            if not nganh_nghe:
                nganh_nghe = create_nganh_nghe(job.get('nganh_nghe', ''), cursor)

            if not tai_khoan:
                continue

            import_to_tbl_bai_dang(job, tai_khoan, nganh_nghe, cursor)
            job_details.append(job)

    return job_details


def get_last_page(soup):
    last_page_element = soup.find_all(attrs={'data-page': True})[-1]
    last_page = int(last_page_element.text)
    return last_page


def read_fieldnames_from_file(filename):
    with open(filename, 'r') as file:
        fieldnames = file.read().strip().split(',')
    return fieldnames


def delete_job_cao_du_lieu(cursor):
    query = "DELETE FROM tbl_bai_dang WHERE job_cao_du_lieu = 1"
    cursor.execute(query)


def delete_old_tai_khoan(cursor):
    pattern = f"%@vieclam24h.com%"
    query = f'DELETE FROM tbl_tai_khoan WHERE tai_khoan like %s AND created_at IS NULL'
    cursor.execute(query, (pattern,))


def delete_old_nganh_nghe(cursor):
    query = f'DELETE FROM tbl_nganh_nghe WHERE is_crawl_data IS TRUE'
    cursor.execute(query)


def main():
    starting_url = 'https://vieclam24h.vn/viec-lam-da-nang-p104.html'
    output_csv_file = 'vieclam24h_danang.csv'
    fieldnames = read_fieldnames_from_file('fields.txt')
    page = 1

    connection = mysql.connector.connect(**mysql_config)

    with connection.cursor(dictionary=True) as cursor:
        delete_job_cao_du_lieu(cursor)
        delete_old_tai_khoan(cursor)
        delete_old_nganh_nghe(cursor)
        connection.commit()
        cursor.close()

    while True:
        url = f'{starting_url}?page={page}'
        page_html = fetch_page_html(url)
        print(f"Reading page {page}")

        if page_html is None:
            a = 1
            # break

        soup = BeautifulSoup(page_html, 'html.parser')
        job_details = extract_job_details(soup, connection)
        last_page = get_last_page(soup)
        connection.commit()

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

    if connection is not None:
        connection.close()


def close_cursor(cursor):
    try:
        cursor.close()
    except Exception as e:
        print(f"Error closing cursor: {e}")


if __name__ == '__main__':
    main()
