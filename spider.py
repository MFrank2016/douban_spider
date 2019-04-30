#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
auth: Frank
date: 2019-04-27
desc: 爬取豆瓣读书评分9.0以上书籍并存入csv文件

目标URL：https://www.douban.com/doulist/1264675/?start=0&sort=seq&playable=0&sub_type=4

数据量：530

预计访问次数：22

数据存储：csv

抓取内容格式：书籍名称 作者 作者国籍 评分 评价人数 出版社 出版年 封面链接
"""
import logging
import os
import random
import time
import requests
import re
import chardet
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv


# 获取网页数据
def get_web_data(p_url, p_headers, p_proxies=None):
    if p_proxies is None:
        p_proxies = []
    try:
        web_info = requests.get(p_url, proxies=p_proxies, timeout=3, headers=p_headers)
    except requests.exceptions.ConnectionError as e:
        logging.error("请求错误，url:", p_url)
        logging.error("错误详情：", e)
        web_info = None
    except:
        logging.error("未知错误，url:", p_url)
        web_info = None
    return web_info


# 解析书籍数据
def parse_data(web_data):
    if web_data is None:
        return None
    # 处理编码
    charset = chardet.detect(web_data.content)
    web_data.encoding = charset['encoding']

    # 正则表达式匹配作者出版社信息
    author_pattern = re.compile(r'(作者: (.*))?[\s|\S]*出版社: (.*)[\s|\S]*出版年: (.*)')

    # 解析标签
    soup = BeautifulSoup(web_data.text, 'lxml')
    book_list = soup.find_all("div", class_="bd doulist-subject")
    list = []
    for book in book_list:
        book_map = {}
        book_name = book.find('div', class_='title').get_text().strip()
        book_map['book_name'] = book_name

        rate_point = book.find('div', class_='rating').find('span', class_='rating_nums').get_text().strip()
        book_map['rate_point'] = rate_point

        rate_number = book.find('div', class_='rating').find('span', class_='').get_text().strip()[1:-4]
        book_map['rate_number'] = rate_number

        tmp = book.find('div', class_='abstract').get_text().strip()
        m = author_pattern.match(tmp)
        author = m.group(1)
        if author is None:
            author = ''
        publisher = m.group(3)
        publish_date = m.group(4)
        book_map['author'] = author
        book_map['publisher'] = publisher
        book_map['publish_date'] = publish_date

        pic_link = book.find('div', class_='post').a.img['src']
        book_map['pic_link'] = pic_link
        list.append(book_map)
        logging.info("书名：《%s》，作者：%s，评分：%s，评分人数：%s，出版社：%s，出版年：%s，封面链接：%s",
                     book_name, author, rate_point, rate_number, publisher, publish_date, pic_link)
    return list


# 存入csv文件
def save_to_csv(filename, books):
    with open(filename, 'a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=books[0].keys())
        for tmp_book in books:
            writer.writerow(tmp_book)


# 获取代理数据
def get_proxies(proxy_url, dis_url, page=10):
    proxy_list = []
    for i in range(1, page + 1):
        tmp_ua = UserAgent()
        tmp_headers = {'User-Agent': tmp_ua.random}
        html_str = get_web_data(proxy_url + str(i), tmp_headers)
        soup = BeautifulSoup(html_str.content, "lxml")
        ips = soup.find('tbody').find_all('tr')
        for ip_info in ips:
            tds = ip_info.find_all('td')
            ip = tds[0].get_text()
            port = tds[1].get_text()
            ip_str = ip + ":" + port
            tmp = {"http": "http://" + ip_str}
            if check_proxy(dis_url, tmp):
                logging.info("ip:%s is available", ip_str)
                proxy_list.append(ip_str)
        time.sleep(1)
    return proxy_list


# 检测代理ip是否可用
def check_proxy(p_url, proxy):
    try:
        tmp_ua = UserAgent()
        tmp_headers = {'User-Agent': tmp_ua.random}
        res = requests.get(p_url, proxies=proxy, timeout=1, headers=tmp_headers)
    except:
        return False
    else:
        return True


def get_random_ip(ip_list):
    proxy = random.choice(ip_list)
    return {'http': 'http://' + proxy}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    url = 'https://www.douban.com/doulist/1264675/?start='
    file_path = os.path.dirname(os.path.realpath(__file__)) + os.sep + 'douban.csv'
    f = open(file_path, 'w')
    f.close()

    # 获取代理
    proxies = get_proxies("https://www.kuaidaili.com/free/intr/", url, 5)

    # 设置headers
    ua = UserAgent()

    result_list = []
    for num in range(0, 530, 25):
        headers = {'User-Agent': ua.random}
        logging.info('headers:%s', headers)
        data = get_web_data(url + str(num), headers, get_random_ip(proxies))
        book = parse_data(data)
        save_to_csv(file_path, book)
        time.sleep(1)