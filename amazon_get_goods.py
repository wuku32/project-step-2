import os
import time
from pprint import pprint

from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException
import pandas as pd
from pandas import DataFrame
import requests
from requests.exceptions import ConnectTimeout, ConnectionError

# 设置每次滚动的距离和滚动间隔时间
scroll_increment = 1000  # 每次滚动100像素
pause_time = 0.5  # 每次滚动后暂停0.5秒

# 导入驱动服务
path = './util/chromedriver-win64/chromedriver.exe'
s = Service(executable_path=path)

# 更改浏览器默认选项
# options = Options()
# options.add_argument('--headless')
# options.add_argument('--disable-gpu')

chrome_options = ChromeOptions()
chrome_options.add_experimental_option(
    name='debuggerAddress',
    value='127.0.0.1:6001'
)

# 新建浏览器对象，并赋以驱动和选项
driver = Chrome(options=chrome_options, service=s)


def page_roll_down():
    # 往下滚动页面，使页面元素加载完全
    # 获取页面的总高度
    last_height = driver.execute_script("return document.body.scrollHeight")
    # print(last_height)
    while True:
        # 滚动页面的高度
        driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_increment)

        # 等待页面加载
        time.sleep(pause_time)
        try:
            # 计算新的页面高度并与之前的高度进行比较
            new_height = driver.execute_script("return window.pageYOffset;")
        except TimeoutException:
            new_height = 0

        # 点击五点展开图标
        if new_height == scroll_increment:
            try:
                driver.find_element(By.XPATH, '//*[@id="productFactsToggleButton"]/a/i').click()
            except NoSuchElementException:
                pass
            except ElementNotInteractableException:
                pass

        if new_height == last_height:
            driver.execute_script("window.scrollTo({top: 0, behavior: 'smooth'});")
            time.sleep(pause_time * 2)
            break  # 如果页面高度没有变化，跳出循环

        last_height = new_height


def download_img(s: str, fn: str) -> None:
    headers = {
        'Priority': 'i',
        'Referer': 'https://www.amazon.de',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    }

    try:
        response = requests.get(s)
        with open(fn, mode='wb') as f:
            f.write(response.content)
    except ConnectTimeout:
        pass
    except ConnectionError:
        pass


def get_good_info(u: str) -> dict:
    while True:
        try:
            driver.execute_script(f"window.open('{u}', '_blank');")
            driver.switch_to.window(driver.window_handles[-1])
            break
        except TimeoutException:
            driver.close()
            driver.switch_to.window(driver.window_handles[-1])

    time.sleep(pause_time)
    page_roll_down()

    info = {
        'title': ' ',
        'desc': ' ',
        'bp_01': ' ',
        'bp_02': ' ',
        'bp_03': ' ',
        'bp_04': ' ',
        'bp_05': ' ',
    }

    uls = driver.find_elements(By.CSS_SELECTOR, '.a-unordered-list.a-vertical.a-spacing-small')
    for i, ul in enumerate(uls):
        bp = ul.find_element(By.CSS_SELECTOR, '.a-list-item.a-size-base.a-color-base').text
        info[f'bp_{i + 1:02d}'] = bp

    try:
        info['desc'] = driver.find_element(By.ID, 'productDescription').text
    except NoSuchElementException:
        info['desc'] = ' '

    info['title'] = driver.find_element(By.ID, 'productTitle').text

    driver.close()
    driver.switch_to.window(driver.window_handles[-1])
    time.sleep(pause_time)

    return info


# 访问网址
url = f'https://www.amazon.de/'
driver.get(url=url)
time.sleep(pause_time)

from util.widgets import generate_ym, generate_ymd, generate_i

ym = generate_ym()
g = 'cargohose_herren'
bd = f'{g}/{ym}'
b_d = f'{g}_{ym}'

ymd = generate_ymd()
i = generate_i()
bd_img = f'{bd}/{ymd}_{i}'

os.makedirs(bd, exist_ok=True)
os.makedirs(bd_img, exist_ok=True)

# 定位到搜索框，输入搜索词，回车
driver.find_element(By.ID, 'twotabsearchtextbox').send_keys(g.replace('_', ' '), Keys.ENTER)
time.sleep(pause_time)

sfs = {
    'default': 20,
    'asc': 10,
    'desc': 10,
    'cmt': 20,
    'newr': 20,
    'best': 20
}

fp_rawd = f'{bd}/{b_d}_rawd.xlsx'
ori_df: DataFrame = pd.read_excel(fp_rawd, sheet_name='goods')
goods = []

for c, (sf, qty) in enumerate(sfs.items()):
    bd_img_sf = f'{bd_img}/{sf}'
    os.makedirs(bd_img_sf, exist_ok=True)

    # 按新品榜单排序
    driver.find_element(By.CSS_SELECTOR, '.a-button.a-button-dropdown.a-button-small').click()
    time.sleep(pause_time)
    driver.execute_script(f"document.querySelector('#s-result-sort-select_{c}').click()")
    time.sleep(pause_time * 2)

    page_roll_down()

    # 获取搜索结果的产品url
    items = driver.find_elements(By.CSS_SELECTOR, '.sg-col-4-of-24.sg-col-4-of-12.s-result-item.s-asin.sg-col-4-of-16')
    total_items = len(items)
    print(total_items)

    for d, item in enumerate(items[0:qty]):
        print(f'***{sf}: {d + 1:02d}/{total_items}***')
        u = item.find_element(By.CSS_SELECTOR, '.a-link-normal.a-text-normal').get_attribute('href')
        # '//*[@id="search"]/div[1]/div[1]/div/span[1]/div[1]/div[5]'
        # '//*[@id="search"]/div[1]/div[1]/div/span[1]/div[1]/div[5]/div/div/span/div/div/div[1]/div/span/a/div/img'

        try:
            w = item.find_element(By.CSS_SELECTOR, '.a-price-whole').text
            f = item.find_element(By.CSS_SELECTOR, '.a-price-fraction').text
        except NoSuchElementException:
            w = 0
            f = 0

        time.sleep(pause_time)
        src = item.find_element(By.TAG_NAME, 'img').get_attribute('src')
        # print(src)
        download_img(src, f'{bd_img_sf}/{g}_{ymd}_{i}_{sf}_{d + 1:02d}.jpg')

        time.sleep(pause_time * 2)
        good = {
            'ym': ym,
            'ymd': ymd,
            'i': i,
            'from': sf,
            'asin': item.get_attribute('data-asin'),
            'price': f'{w}.{f}',
            'url': u,
            'src': src,
        }

        info = get_good_info(u)
        for k, v in info.items():
            good[k] = v

        # pprint(good)
        goods.append(good)
        new_df = pd.DataFrame(goods)
        df = pd.concat([ori_df, new_df], axis=0, ignore_index=True)

        with pd.ExcelWriter(fp_rawd, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='goods')

        if (d + 1) % 5 == 0:
            time.sleep(pause_time * 4)

    #     if d == 1:
    #         break
    # break
