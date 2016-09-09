import random
import time

def GetIP():
    ips = [
        "201.243.31.135:8080"
        "81.208.32.6:80",
        "103.240.241.145:80",
        "36.225.94.208:8080",
    ]
    ip = random.choice(ips)
    return ip


def GetHeaders():
    headers = [
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36",
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; InfoPath.1',
        'Mozilla/4.0 (compatible; GoogleToolbar 5.0.2124.2070; Windows 6.0; MSIE 8.0.6001.18241)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; Sleipnir/2.9.8)',
    ]
    header = random.choice(headers)
    return header


def sleep_some_time(times):
    """
    随机休眠几秒
    :return:
    """
    t = random.random()*times
    print("休眠" + str(t) + "秒")
    time.sleep(t)