from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from pyquery import PyQuery as pq
import pymysql
import time
import requests
from lxml import etree
from urllib.parse import urljoin
from multiprocessing import Pool
from  multiprocessing.dummy import Pool as ThreadPool
from functools import partial
import threading
import settings

start_url = 'https://music.taihe.com/artist/'
table = settings.TABLE
host = settings.HOST
user = settings.USER
password = settings.PASSWORD
port = settings.PORT
database = settings.DATABASE
sleep_time = settings.SLEEP_TIME

def index_to_detail(index_url):
	"""get the singer's url from homepage"""
	singer_url_list = []
	start_get = requests.get(index_url).text
	start_response = etree.HTML(start_get)
	index_sectors = start_response.xpath('//*[@id="subPage"]//li[@class="list-item"][position()>1]/descendant::li')
	for index_sector in index_sectors:
		try:
			base_singer_url = index_sector.xpath('./a/@href')[0]
			singer_url = urljoin(index_url, str(base_singer_url))
			singer_url_list.append(singer_url)
		except:
			None
	return singer_url_list

def get_singer_data(singer_url, sleep_time, db, browser):
	"""get information from singer's page"""
	try:
		browser.get(singer_url)
		wait = WebDriverWait(browser, 10)
		wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'artist-left')))
		singer_page_html = etree.HTML(browser.page_source)
		max_page_xpath = '//div[@class="music-main"]//div[@class="main-body"]//div[@class="list-box song-list-box active"]//div[@class="page_navigator-box"]//div[@class="page-inner"]/a[last()-1]/text()'
		max_page = singer_page_html.xpath(max_page_xpath)
		spider_data(browser.page_source, db)
	except:
		None
	try:
		for page in range(1, int(max_page[0])):
			next_button = browser.find_element_by_class_name('page-navigator-next')
			next_button.click()
			time.sleep(sleep_time)
			spider_data(browser.page_source, db)
	except:
		None

def spider_data(response, db):
	"""update data to mysql"""
	doc = pq(response)
	singer_name_item = doc('.sns .music-main')
	singer_name = singer_name_item.find('.artist-name').text()
	items = doc('#songList .song-list-wrap .songlist-item').items()
	for item in items:
		singer_data = {
			'singer_name': singer_name,
			'name': item.find('.songlist-title .songname .namelink').attr('title'),
			'album': item.find('.songlist-album').text().strip(),
			'time': item.find('.songlist-time').text().strip()
		}
		update_to_mysql(singer_data, db)

def update_to_mysql(data, db):
	data_keys = ', '.join(data.keys())
	data_values = ', '.join(['%s'] * len(data))
	cursor = db.cursor()
	sql = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE'.format(table=table, keys=data_keys, values=data_values)
	update = ','.join([" {key} = %s".format(key=key) for key in data])
	sql += update
	cursor.execute(sql, tuple(data.values())*2)
	db.commit()
	print(f'{data} update success !')

def run(singer_url, sleep_time):
	print("当前激活的线程有：",threading.active_count())
	db = pymysql.connect(host=host, user=user, password=password, port=port, db=database)
	print(f'\n{singer_url}, 开始爬取\n')
	service_args = []
	service_args.append('--load-images=no')
	service_args.append('--disk-cache=yes')
	service_args.append('--ignore-ssl-errors=true')
	service_args.append('--ssl-protocol=TLSv1')
	browser = webdriver.PhantomJS(service_args=service_args)
	get_singer_data(singer_url, sleep_time, db, browser)
	browser.quit()
	db.close()

def thread(sleep_time, singer_url_list):
	pool = ThreadPool()
	results = pool.map(partial(run, sleep_time=sleep_time), singer_url_list)
	pool.close()
	pool.join()
	print("\n歌曲爬取已全部完成！！\n")

if __name__ == '__main__':
	start_time = time.process_time()
	singer_url_list = index_to_detail(start_url)
	thread(sleep_time, singer_url_list)
	end_time = time.process_time()
	print("耗时：",end_time-start_time,"秒")
