import requests
from bs4 import BeautifulSoup
import pymysql
import urllib.request
from multiprocessing import Pool
from datetime import datetime
from tools import *
from urls import WXUrl
import os
# from log_app import log


def get_html(url):
    """
    提取html
    :param url:
    :return: html
    """
    sleep_some_time(2)
    h = {'User-Agent': GetHeaders()}
    i = 0
    html = None
    while html == None and i<10:
        try:
            rep = requests.get(url, headers=h)
            if rep.status_code == 200:
                html = rep.content
                i = 0
                return html
        except Exception as e:
            print(e, "html重新提取", i, "URL:",url)
            # log.info("html重新提取",i,"URL:",url)
            i += 1
            html = None
            continue


def get_page_url(url):
    """
    提取文章url title
    :param url:
    :return: list
    """
    datas = []
    list = []
    html = None
    while(datas is [] or html is None):
        """
        循环,避免推文信息刷新不出的情况
        """
        html = get_html(url)
        try:
            html = BeautifulSoup(html, "html.parser")
        except TypeError:
            if html is None:
                continue
        try:
            datas = html.find_all("h4")
        except TypeError:
            continue
        datas = datas[1:6]
        for data in datas:
            mdict = {}
            mdict["url"] = data.find("a").attrs['href']
            mdict["title"] = data.get_text()[1:-1]
            list.append(mdict)
            print(mdict['title'])
    del datas
    return list


def get_article(dicts):
    """
    提取文章页面元素
    :param dicts:
    :return: list
    """
    try:
        html = get_html(dicts['url'])
        html = BeautifulSoup(html, "html.parser")
        dicts['date'] = html.find("em", id='post-date').get_text() + " 00:00:00"
        dicts['post_author'] = html.find('a', id='post-user').get_text()
        msg = html.find("div", id="js_content")
        imgs = msg.find_all("img")
        # imgs = imgs[1:-1]
        ulist = []
        if imgs is not []:
            for img in imgs:
                try:
                    ulist.append(img.attrs['data-src'])
                except Exception as e:
                    print("error:", e)
                    # log.error('图片地址解析出错:',img)
                    pass
        else:
            print("没有图片")
        dicts['content'] = msg.get_text(strip=True)
    except AttributeError:
        # log.error("文章页面错误,跳过:",dict['title'],dict['url'])
        return {}
    dicts['img_url'] = ulist
    print(dicts['title'],dicts['post_author'])
    return dicts


def into_db(lists):
    """

    :param lists:
    :return:
    """
    con = pymysql.connect(host='127.0.0.1', user='root',
                          passwd='root', db='wechat_article',
                          port=3306, charset="utf8")
    cur = con.cursor()
    values = []
    a = str(datetime.now())[:-7]
    for i in lists:
        try:
            values.append((i['title'], i['post_author'], i['content'], i['url'], i['date'], a))
        except KeyError as e:
            print("error:", e)
            continue
    try:
        cur.executemany("insert ignore into wechat(title, post_auth, content, page_url, post_time, into_db)\
                          VALUES (%s, %s, %s, %s, %s, %s)", values)
        con.commit()
    except Exception as e:
        print("error:", e)
        con.rollback()
        # log.error('插入数据库错误',e)
    con.close()


def save_img(dicts):
    """

    :param dicts:
    :return:
    """
    path = os.path.dirname(os.path.abspath('__file__'))
    try:
        ilist = dicts['img_url']
    except KeyError as e:
        ilist = []
        print("错误:::::::::::::::", e)
    if ilist is not []:
        i = 0
        for url in ilist:
            i += 1
            name = ''.join(dicts['title'].split("/"))+str(i)+".png"
            try:
                urllib.request.urlretrieve(url, path + "/img/" + name)
            except Exception as e:
                print("error:", e)
                continue
    else:
        print("无图:::::::::::::::::::::::::::::::::")
        pass


def io_image(lists):
    pool = Pool(processes=4)
    for dicts in lists:
        pool.apply_async(save_img, args=(dicts,))
    pool.close()
    pool.join()


def run():
    print("微信爬虫启动",str(datetime.now())[:-7])
    urls = WXUrl()
    lists = []
    url_list = []
    for url in urls:
        print("主::::::" + url)
        url_list += get_page_url(url)

    pool = Pool(processes=4)
    lists += pool.map(get_article, url_list)
    pool.close()
    pool.join()
    print("爬虫抓取完毕")
    into_db(lists)
    print("成功存入数据库")
    io_image(lists)
    print("图片保存完毕")
    print("爬虫关闭")


if __name__ == '__main__':
    run()


