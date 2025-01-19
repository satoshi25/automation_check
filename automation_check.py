from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from google.auth.exceptions import TransportError
from google.oauth2 import service_account


import os
import time
import gspread 
import pandas as pd
import traceback
import requests
import json
import backoff

# .env 파일 로드
load_dotenv()

# 환경 변수 사용
# mall_id = os.getenv("MALL_ID")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
login_page = os.getenv("LOGIN_PAGE")
dashboard_page = os.getenv("DASHBOARD_PAGE")
shipping_page = os.getenv("SHIPPING_PAGE")
json_str = os.getenv("JSON_STR")
sheet_key = os.getenv("SHEET_KEY")
store_api_key = os.getenv("STORE_API_KEY")
store_basic_url = os.getenv("STORE_BASIC_URL")


class GoogleSheetManager:
    def __init__(self):
        self.gc = None
        self.doc = None
        self.initialize_connection()

    @backoff.on_exception(
        backoff.expo,
        (TransportError, requests.exceptions.RequestException),
        max_tries=5
    )
    def initialize_connection(self):
        try:
            credentials_info = json.loads(json_str)
            if 'private_key' in credentials_info:
                pk = credentials_info['private_key']
                pk = pk.replace('\\n', '\n')
                credentials_info['private_key'] = pk
            print("JSON 파싱 성공")
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            self.gc = gspread.authorize(credentials)
            self.doc = self.gc.open_by_key(sheet_key)
        except Exception as e:
            print(f"연결 초기화 실패: {e}")
            raise

    def get_worksheet(self, sheet_name):
        try:
            return self.doc.worksheet(sheet_name)
        except Exception as e:
            print(f"get_worksheet 실패: {e}")
            self.initialize_connection()  # 연결 재시도
            return self.doc.worksheet(sheet_name)

    @backoff.on_exception(
        backoff.expo,
        (TransportError, requests.exceptions.RequestException),
        max_tries=5
    )
    def get_sheet_data(self, sheet_name):
        worksheet = self.get_worksheet(sheet_name)
        try:
            header = worksheet.row_values(1)
            data = worksheet.get_all_records()

            if not data:
                df = pd.DataFrame(columns=header)
            else:
                df = pd.DataFrame(data)
            
            return df
        except Exception as e:
            print(f"시트 데이터 가져오기 실패: {e}")
            raise

# sheet_manager = GoogleSheetManager()
# service_worksheets = sheet_manager.get_worksheet('market_service_list')
# order_worksheets = sheet_manager.get_worksheet('market_store_order_list')
# manual_order_worksheets = sheet_manager.get_worksheet('manual_order_list')

# service_sheet_data = sheet_manager.get_sheet_data('market_service_list')
# order_sheet_data = sheet_manager.get_sheet_data('market_store_order_list')
# manual_order_sheet_data = sheet_manager.get_sheet_data('manual_order_list')


class StoreAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = store_basic_url

    def create_order(self, service_id, link, quantity, runs=None, interval=None):

        params = {
            'key': self.api_key,
            'action': 'add',
            'service': service_id,
            'link': link,
            'quantity': quantity
        }

        try:
            response = requests.post(self.base_url, data=params)
            response.raise_for_status()  # HTTP 오류 체크
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"주문 생성 중 오류 발생: {e}")
            raise
    
    # 주문 상태 확인
    def get_order_status(self, order_id):

        params = {
            'key': self.api_key,
            'action': 'status',
            'order': order_id
        }

        try:
            response = requests.post(self.base_url, data=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"주문 상태 확인 중 오류 발생: {e}")
            raise

    # 여러 주문의 상태를 한 번에 확인
    def get_multiple_order_status(self, order_ids):

        params = {
            'key': self.api_key,
            'action': 'status',
            'orders': ','.join(map(str, order_ids))
        }

        try:
            response = requests.post(self.base_url, data=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"다중 주문 상태 확인 중 오류 발생: {e}")
            raise

    # 계정 잔액을 확인
    def get_balance(self):
        params = {
            'key': self.api_key,
            'action': 'balance'
        }

        try:
            response = requests.post(self.base_url, data=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"잔액 확인 중 오류 발생: {e}")
            raise

if not os.path.exists(json_str):
    print(f"JSON 키 파일이 존재하지 않습니다: {json_str}")

# gc = gspread.service_account(json_str)
# doc = gc.open_by_key(sheet_key)
# order_sheets = doc.worksheet('market_store_order_list')
# manual_order_sheets = doc.worksheet('manual_order_list')

def get_sheet_data(sheet):
    header = sheet.row_values(1)
    data = sheet.get_all_records()

    if not data:  # 데이터가 없는 경우
        # 빈 DataFrame을 생성하되, 컬럼은 명시적으로 지정
        df = pd.DataFrame(columns=header)
    else:
        df = pd.DataFrame(data)
    
    return df

def add_order_sheet(df, order):
    print()
    # store_order_num = order.get('store_order_num', '').get('order')
    try:
        row_data = [
            str(order.get('market_order_num', '')),
            str(order.get('store_order_num', '').get('order')),
            str(order.get('order_username', '')),
            str(order.get('service_num', '')),
            str(order.get('order_link', '')),
            str(order.get('order_edit_link', '')),
            str(order.get('quantity', '')),
            str(order.get('service_name', '')),
            str(order.get('order_time', '')),
            "주문완료",
        ]
        df.append_row(row_data)
        print(f"주문 정보가 시트에 추가되었습니다: {row_data}")
        return order
    
    except Exception as e:
        print(f"시트 추가 중 오류 발생: {str(e)}")
        traceback.print_exc()

# 1. Selenium WebDriver 설정
def init_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 브라우저 창 숨기기
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)
    return driver


# 2. Cafe24 로그인
def cafe24_login(driver, login_page, wait):
    driver.get(login_page)
    try:
        wait.until(EC.all_of(
            EC.presence_of_element_located((By.NAME, "loginId")),
            EC.presence_of_element_located((By.NAME, "loginPasswd"))
        ))
        driver.find_element(By.NAME, "loginId").send_keys(username)  # Admin ID 입력
        driver.find_element(By.NAME, "loginPasswd").send_keys(password)  # 비밀번호 입력
        try:
            login_btn = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.btnStrong.large")))
            driver.execute_script("arguments[0].click();", login_btn)
            pw_change_btn = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#iptBtnEm")))
            driver.execute_script("arguments[0].click();", pw_change_btn)
            wait.until(EC.url_to_be(dashboard_page))
        except Exception as e:
            print(f"클릭 중 오류 발생: {e}")
    except TimeoutException:
        print("20초 동안 버튼이 클릭 가능한 상태가 되지 않았습니다.")
    return driver


# 3. 배송중 주문 정보 크롤링
def scrape_orders(driver, shipping_order_page, wait):
    driver.get(shipping_order_page)

    # 주문 정보 크롤링
    orders = []
    order_list = []

    try:
        wait.until(EC.all_of(
            # 검색된 주문내역이 없습니다.
            # tbody class empty
            # td colspan = 9 검색된 주문내역이 없습니다.
            # 모든 조건을 만족하는 요소를 찾는다
            EC.presence_of_element_located((By.CSS_SELECTOR, "td.orderNum")),
            EC.presence_of_element_located((By.CSS_SELECTOR, ".chkbox")),
        ))
    except TimeoutException:
        print("20초 동안 어떤 조건도 만족하지 않았습니다.")
        return [[], '']

    order_element = driver.find_element(By.CSS_SELECTOR, "#searchResultList")
    orders = order_element.find_elements(By.CSS_SELECTOR, "tbody.center")
    eshipEnd_element = driver.find_element(By.CSS_SELECTOR, "#eShippedEndBtn")
    print('주문수량', len(orders))

    if len(orders) > 0:
        for order in orders:
            try:
                order_num_element = order.find_element(By.CSS_SELECTOR, "td.orderNum")
            except NoSuchElementException:
                continue
            try:
                order_chk = order.find_element(By.CSS_SELECTOR, ".chkbox")
            except NoSuchElementException:
                print('no chkbox')

            order_num_text = order_num_element.text
            order_num = order_num_text.split('\n')[1].split(' ')[0]

            order_list.append({
                "market_order_num": order_num,
                "check_element": order_chk,
            })
    else:
        print('검색된 주문내역이 없습니다.')

    print('배송중 주문목록 작성완료')
    return [order_list, eshipEnd_element]


async def check_order(orders, shipping_orders, store_api):
    processed_orders = []
    for order in orders:
        try:
            market_order_num = order.get('market_order_num')
            filtered_orders = shipping_orders[
                (shipping_orders['마켓주문번호'].str.contains(market_order_num, na=False)) &
                (shipping_orders['주문상태'] == '배송중')
            ]
            is_all_complete = False
            order_cnt = len(filtered_orders)
            if order_cnt == 1:
                complete_cnt = 0
                store_order_num = filtered_orders.iloc[0]['스토어주문번호']
                response = store_api.get_order_status(store_order_num)
                if response.get('status') == 'Completed':
                    complete_cnt += 1
                    print()
                    print('완료된 주문')
                    print(f"{store_order_num} - {filtered_orders.iloc[0]['마켓주문번호']}", response.get("status"))
                    print()
                else:
                    print()
                    print('완료되지 않은 주문')
                    print(f"{store_order_num} - {filtered_orders.iloc[0]['마켓주문번호']}", response.get("status"))
                    print()
            else:
                complete_cnt = 0
                for i in range(order_cnt):
                    store_order_num = filtered_orders.iloc[i]['스토어주문번호']
                    response = store_api.get_order_status(store_order_num)
                    if response.get('status') == 'Completed':
                        complete_cnt += 1
                        print()
                        print('완료된 주문')
                        print(f"{store_order_num} - {filtered_orders.iloc[i]['마켓주문번호']}", response.get("status"))
                        print()
                    else:
                        print()
                        print('완료되지 않은 주문')
                        print(f"{store_order_num} - {filtered_orders.iloc[i]['마켓주문번호']}", response.get("status"))
                        print()
                        break

            if complete_cnt == order_cnt:
                is_all_complete = True
            
            if is_all_complete:
                processed_orders.append(order)
                
        except Exception as e:
            print(f"주문 처리 중 오류 발생: url, 에러: {e}")
            traceback.print_exc()
        # print(order)
    print('-------------------------------')
    print(f"진행중인 전체 주문 수: {len(orders)}")
    print(f"완료된 주문 수: {len(processed_orders)}")
    print('-------------------------------')
    return processed_orders

def process_orders(shipping_order_sheets, orders):
    try:
        cell = shipping_order_sheets.find('주문상태')
        status_col = cell.col
        
        cnt = 0
        result = [False, orders]
        
        for order in orders:
            data = shipping_order_sheets.get_all_records()  # 매 주문마다 최신 데이터 조회
            market_order_num = order.get('market_order_num')
            
            # 한 번에 하나의 행만 업데이트
            for idx, row in enumerate(data):
                if (row['주문상태'] == '배송중' and 
                    market_order_num in row['마켓주문번호']):
                    row_num = idx + 2
                    
                    try:
                        # batch_update 대신 개별 업데이트
                        shipping_order_sheets.update_cell(row_num, status_col, '배송완료')
                        print(f"{market_order_num} - {row_num}행 배송완료로 변경 성공")
                        cnt += 1
                        time.sleep(0.5)  # API 요청 제한 고려
                    except Exception as e:
                        print(f"{row_num}행 업데이트 실패: {e}")
                        continue
            
            order["check_element"].click()
            time.sleep(1)

        if cnt > 0:
            result = [True, orders]
        return result

    except Exception as e:
        print(f"오류 발생: {e}")
        traceback.print_exc()
        return result

def process_eship(driver, orders, order_element, alert, wait):
    if orders[0]:
        driver.execute_script("arguments[0].click();", order_element)
        alert = wait.until(EC.alert_is_present())
        alert.accept()
        alert = wait.until(EC.alert_is_present())
        alert.accept()
    return

async def main():
    driver = init_driver()
    wait = WebDriverWait(driver, timeout=20)
    alert = Alert(driver)

    sheet_manager = GoogleSheetManager()
    # service_worksheets = sheet_manager.get_worksheet('market_service_list')
    shipping_order_worksheets = sheet_manager.get_worksheet('market_store_order_list')
    # manual_order_worksheets = sheet_manager.get_worksheet('manual_order_list')

    # service_sheet_data = sheet_manager.get_sheet_data('market_service_list')
    shipping_order_data = sheet_manager.get_sheet_data('market_store_order_list')
    # manual_order_sheet_data = sheet_manager.get_sheet_data('manual_order_list')

    store_api = StoreAPI(store_api_key)

    try:
        cafe24_login(driver, login_page, wait)
        order_list = scrape_orders(driver, shipping_page, wait)
        orders, shipping_complete_element = order_list

        processed_orders = await check_order(orders, shipping_order_data, store_api)

        print('-------------------------------')
        print('완료된 주문목록', processed_orders)
        print('-------------------------------')

        if len(processed_orders) > 0:
            check_orders = process_orders(shipping_order_worksheets, processed_orders)
            process_eship(driver, check_orders, shipping_complete_element, alert, wait)
        return processed_orders
    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        print('완료')
        driver.quit()
        # 비동기 세션 정리

if __name__ == "__main__":
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        orders = loop.run_until_complete(main())
    finally:
        loop.close()
