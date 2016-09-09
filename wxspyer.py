import requests
from bs4 import BeautifulSoup
import pymysql
import urllib.request
from multiprocessing import Pool
from datetime import datetime
from tools import *
from urls import WXUrl
from log_app import log


def GetHtml(url):
    """
    提取html
    :param url:
    :return: html
    """
    sleep_some_time(2)
    h = {'User-Agent': GetHeaders()}
    i = 0
    html = None
    while html == None and i<5:
        try:
            rep = requests.get(url, headers=h)
            if rep.status_code == 200:
                html = rep.content
                i = 0
                return html
        except:
            log.info("html重新提取",i,"URL:",url)
            i+=1
            html = None
            continue


def GetPageUrl(url):
    """
    提取文章url title
    :param url:
    :return: list
    """
    datas = []
    html = None
    while(datas == [] or html == None):
    #循环,避免推文信息刷新不出的情况
        html = GetHtml(url)
        try:
            html = BeautifulSoup(html, "html.parser")
        except TypeError:
            if html == None:
                continue
        try:
            datas = html.find_all("h4")
        except TypeError:
            continue
        datas = datas[1:6]
        list = []
        for data in datas:
            dict = {}
            dict["url"] = data.find("a").attrs['href']
            dict["title"] = data.get_text()[1:-1]
            list.append(dict)
            del dict
        print(list)
    del datas
    return list


def GetArticle(dict):
    """
    提取文章页面元素
    :param dict:
    :return: list
    """
    try:
        html = GetHtml(dict['url'])
        html = BeautifulSoup(html, "html.parser")
        dict['date'] = html.find("em", id='post-date').get_text() + " 00:00:00"
        dict['post_author'] = html.find('a', id='post-user').get_text()
        msg = html.find("div", id="js_content")
        imgs = msg.find_all("img")
        # imgs = imgs[1:-1]
        ulist = []
        if imgs != []:
            for img in imgs:
                try:
                    ulist.append(img.attrs['data-src'])
                except:
                    log.error('图片地址解析出错:',img)
                    pass
        else:
            print("没有图片")
        dict['content'] = msg.get_text(strip=True)
    except AttributeError:
        log.error("文章页面错误,跳过:",dict['title'],dict['url'])
        return {}
    dict['img_url'] = ulist
    print(dict['date'], dict['post_author'],dict['img_url'])
    return dict


def IntoDB(Alist):
    """

    :param Alist:
    :return:
    """
    con = pymysql.connect(host='127.0.0.1', user='root',
                          passwd='root', db='wechat_article',
                          port=3306, charset="utf8")
    cur = con.cursor()
    values = []
    a = str(datetime.now())[:-7]
    for i in Alist:
        try:
            values.append((i['title'], i['post_author'], i['content'], i['url'], i['date'], a))
        except KeyError as e:
            continue
    print(values)
    try:
        cur.executemany("insert ignore into wechat(title, post_auth, content, page_url, post_time, into_db)\
                          VALUES (%s, %s, %s, %s, %s, %s)", values)
        con.commit()
    except Exception as e:
        con.rollback()
        log.error('插入数据库错误',e)
    con.close()




def SaveImg(dict):
    try:
        ilist = dict['img_url']
    except KeyError as e:
        ilist = []
        print("错误:::::::::::::::",e)
    if ilist != []:
        i = 0
        for url in ilist:
            i += 1
            name = ''.join(dict['title'].split("/"))+str(i)+".png"
            try:
                urllib.request.urlretrieve(url, "img/"+ name)
                print('图片',name)
            except:
                continue
    else:
        print("无图:::::::::::::::::::::::::::::::::")
        pass

def ioimage(lists):
    pool = Pool(processes=4)
    for dicts in lists:
        pool.apply_async(SaveImg, args=(dicts,))
    pool.close()
    pool.join()


def run():
    log.info("微信爬虫启动")
    urls = WXUrl()
    lists = []
    url_list = []
    for url in urls:
        print("主::::::",url)
        url_list += GetPageUrl(url)
        print("文章::::::::",url_list)

    pool = Pool(processes=4)
    lists += pool.map(GetArticle, url_list)
    pool.close()
    pool.join()
    log.info("爬虫抓取完毕")
    IntoDB(lists)
    log.info("成功存入数据库")
    ioimage(lists)
    log.info("图片保存完毕")
    log.info("爬虫关闭")


if __name__ == '__main__':
    run()



