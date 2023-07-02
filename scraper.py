import datetime
import json
import os
from random import randint
import sys
from threading import Thread
import requests
from urllib import parse
import re
from bs4 import BeautifulSoup

base_url="https://www.facebookwkhpilnemxj7asaniu7vnjjbiltxjqhye3mhbshg7kx5tfyd.onion"
if sys.platform=="linux":
    tor_port=9050
else:
    tor_port=9150
session=requests.Session()

class FakeResponse:
    status_code=0

def create_proxy():
    return {
        "http": "socks5h://{}:foo@localhost:{}".format(randint(100000,999999),tor_port),
        "https": "socks5h://{}:foo@localhost:{}".format(randint(100000,999999),tor_port),
    }

def request(*, method="GET", url:str, data={}, headers={}, params={}, json_data={}, use_tor=True):
    retries=8
    while retries:
        if use_tor:
            session.proxies=create_proxy()
        else:
            session.proxies=None
            print("requesting to {}".format(url))
            # print(json_data)
        try:
            return session.request(method=method, url=url, data=data, json=json_data, headers=headers, params=params, timeout=20)
        except:
            retries-=1
            continue
    return FakeResponse()

def get_page_info(page_unique_identifier:str):
    url="{}/{}".format(base_url,page_unique_identifier)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:102.0) Gecko/20100101 Firefox/102.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'TE': 'trailers'
    }
    response = request(url=url, headers=headers)
    page_id_regex=re.compile(r"(user|page).{0,1}id.{1,5}?(\d{5,})",re.IGNORECASE)
    page_id=page_id_regex.search(response.text)
    page_id=page_id.groups()[-1]
    page_id_2_regex=re.compile(r"(associated_page_id).{1,5}?(\d{5,})",re.IGNORECASE)
    page_id_2=page_id_2_regex.search(response.text)
    page_id_2=page_id_2.groups()[-1] if page_id_2 else page_id
    page_name=BeautifulSoup(response.text,"html.parser").find("meta", {"property": "og:title"}).attrs.get('content')
    page_data={
        "page_name": page_name,
        "page_id": int(page_id),
        "page_id_2": int(page_id_2),
        "page_url": "https://facebook.com/{}".format(page_id)
    }
    page=save_page(page_data)
    return page

def fetch_posts_data(page_id, cursor="", doc_id=6388659591195017):
    status=0
    retries=8
    while status != 200 and retries:
        url = "{}/api/graphql/".format(base_url)
        variables=json.dumps({"count":3,"cursor":cursor,"privacySelectorRenderLocation":"COMET_STREAM","scale":1,"id":page_id}).replace(" ","")
        payload = 'variables={}&doc_id={}'.format(parse.quote(variables),doc_id)
        headers = {
            'authority': 'www.facebook.com',
            'accept': '*/*',
            'accept-language': 'en,en-US;q=0.9',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': '{}'.format(base_url),
            'referer': '{}/FMalghad/'.format(base_url),
            'sec-ch-prefers-color-scheme': 'dark',
            'sec-ch-ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
            'sec-ch-ua-full-version-list': '"Google Chrome";v="113.0.5672.63", "Chromium";v="113.0.5672.63", "Not-A.Brand";v="24.0.0.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"15.0.0"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
            'x-fb-friendly-name': 'ProfileCometTimelineFeedRefetchQuery'
        }
        response = request(method="POST", url=url, headers=headers, data=payload)
        status=response.status_code
        retries-=1
    return response

def extract_posts(data:dict):
    assert data
    posts=list()
    edges=data.get('data',{}).get('node',{}).get('timeline_list_feed_units',{}).get('edges',[])
    for edge in edges:
        try:
            post_id=edge.get('node',{}).get('post_id')
            metadata=edge.get('node',{}).get('comet_sections',{})
            publish_time=metadata.get('context_layout',{}).get('story',{}).get('comet_sections',{}).get('metadata',[{}])[0].get('story',{}).get('creation_time',0)
            message=metadata.get('content',{}).get('story',{}).get('message',{}).get('text')
            reactions_parent=metadata.get('feedback',{}).get('story',{}).get('feedback_context',{}).get('feedback_target_with_context',{}).get('ufi_renderer',{}).get('feedback',{}).get('comet_ufi_summary_and_actions_renderer',{}).get('feedback',{})
            comments=reactions_parent.get('total_comment_count')
            views=reactions_parent.get('video_view_count')
            shares=reactions_parent.get('share_count',{}).get('count')
            reactions_edges=reactions_parent.get('cannot_see_top_custom_reactions',{}).get('top_reactions',{}).get('edges',[])
            reactions={}
            for reaction_edge in reactions_edges:
                reaction_count=reaction_edge.get('reaction_count',0)
                reaction_name=reaction_edge.get('node',{}).get('localized_name','unknown')
                reactions.update({
                    reaction_name: int(reaction_count)
                })
            page_id=int(data.get('data',{}).get('node',{}).get('id'))
            new_post=save_post(dict(page_id=page_id, post_id=post_id, publish_time=publish_time, message=message, comments=comments, views=views, shares=shares, reactions=reactions))
            posts.append(new_post)
        except Exception as e:
            print(e)
    return posts

def fetch_videos_data(page_id, cursor=None, doc_id="6123690421066719"):
    if cursor=='':
        cursor=None
    status=0
    retries=8
    while status != 200 and retries:
        url = "{}/api/graphql/".format(base_url)
        variables=json.dumps({"cursor":cursor,"pageID":page_id,"showReactions":True,"id":page_id})
        payload = 'fb_api_caller_class=RelayModern&fb_api_req_friendly_name=PagesCometChannelTabAllVideosCardImplPaginationQuery&variables={}&server_timestamps=true&doc_id={}'.format(parse.quote(variables),doc_id)
        headers = {
            'authority': 'www.facebook.com',
            'accept': 'application/json',
            'accept-language': 'en',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.facebook.com',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
            'x-fb-friendly-name': 'PagesCometChannelTabAllVideosCardImplPaginationQuery'
        }
        response = request(method="POST", url=url, headers=headers, data=payload)
        status=response.status_code
        retries-=1
    return response

def extract_videos(data:dict):
    assert data
    posts=[]
    videos_edges=data.get('data',{}).get('node',{}).get('all_videos',{}).get('edges',[])
    for edge in videos_edges:
        try:
            post_id=edge.get('node',{}).get('id')
            message=edge.get('node',{}).get('channel_tab_thumbnail_renderer',{}).get('video',{}).get('savable_description',{}).get('text',None)
            views=edge.get('node',{}).get('channel_tab_thumbnail_renderer',{}).get('video',{}).get('play_count',0)
            reactions={}
            reactions_edges=edge.get('node',{}).get('channel_tab_thumbnail_renderer',{}).get('video',{}).get('feedback',{}).get('cannot_see_top_custom_reactions',{}).get('top_reactions',{}).get('edges',[])
            for reaction_edge in reactions_edges:
                reaction_count=reaction_edge.get('reaction_count',0)
                reaction_name=reaction_edge.get('node',{}).get('localized_name','unknown')
                reactions.update({
                    reaction_name: int(reaction_count)
                })
                publish_time=edge.get('node',{}).get('channel_tab_thumbnail_renderer',{}).get('video',{}).get('publish_time',0)
            comments=str(edge.get('node',{}).get('channel_tab_thumbnail_renderer',{}).get('video',{}).get('feedback',{}).get('comment_count_reduced',0))
            if "k" in str(comments).lower():
                comments=int(1000*float(str(comments).lower().replace("k","")))
            if "m" in str(comments).lower():
                comments=int(1000000*float(str(comments).lower().replace("m","")))
            page_id=int(data.get('data',{}).get('node',{}).get('id'))
            new_post=save_post(dict(page_id=page_id, post_id=post_id, publish_time=publish_time, message=message, comments=comments, views=views, shares=None, reactions=reactions))
            posts.append(new_post)
        except Exception as e:
            print(e)
    return posts

def save_post(post):
    headers={
        "Content-Type":"application/json"
    }
    post=request(method="POST", url="http://localhost:45679/facebook/post",json_data=post,headers=headers, use_tor=False).json()
    return post

def save_page(page):
    headers={
        "Content-Type":"application/json"
    }
    page=request(method="POST", url="http://localhost:45679/facebook/page",json_data=page,headers=headers, use_tor=False).json()
    return page

def extract_next_cursor(raw_data_lines):
    for line in raw_data_lines:
        line=json.loads(line)
        if line.get('label')=="ProfileCometTimelineFeed_user$defer$ProfileCometTimelineFeed_user_timeline_list_feed_units$page_info":
            if line.get('data',{}).get('page_info',{}).get('has_next_page'):
                return line.get('data',{}).get('page_info',{}).get('end_cursor')
        else:
            if line.get('data',{}).get('node',{}).get('all_videos',{}).get('page_info',{}).get('has_next_page',False):
                return line.get('data',{}).get('node',{}).get('all_videos',{}).get('page_info',{}).get('end_cursor')

def scrape_page_posts(page_id, period=7):
    try:
        count=0
        next_cursor=""
        stop=False
        while next_cursor is not None and not stop:
            try:
                count+=1
                print(count)
                posts_raw_data=fetch_posts_data(page_id, cursor=next_cursor)
                posts=extract_posts(json.loads(posts_raw_data.text.split("\n")[0]))
                for post in posts:
                    if datetime.datetime.strptime(post.get("publish_time"),"%Y-%m-%d %H:%M:%S").timestamp()<datetime.datetime.now().timestamp()-3600*24*period:
                        stop=True
                        stop_target(page_id)
                next_cursor=extract_next_cursor(posts_raw_data.text.split("\n"))
            except Exception as e:
                print(e)
            yield count
    except Exception as e:
        print(e)
        
def scrape_page_videos(page_id, period=7):
    try:
        count=0
        next_cursor=''
        stop=False
        while next_cursor is not None and not stop:
            try:
                count+=1
                print(count)
                posts_raw_data=fetch_videos_data(page_id, cursor=next_cursor)
                posts=extract_videos(json.loads(posts_raw_data.text.split("\n")[0]))
                for post in posts:
                    if datetime.datetime.strptime(post.get("publish_time"),"%Y-%m-%d %H:%M:%S").timestamp()<datetime.datetime.now().timestamp()-3600*24*period:
                        stop=True
                        stop_target(page_id)
                next_cursor=extract_next_cursor(posts_raw_data.text.split("\n"))
            except Exception as e:
                print(e)
            yield count
    except Exception as e:
        print(e)

def stop_target(_id):
    headers={
        "Content-Type":"application/json"
    }
    params={
        "tracker_id": _id
    }
    page=request(method="DELETE", url="http://localhost:45679/tracker", params=params,headers=headers, use_tor=False).json()
    return page

if __name__=="__main__":
    tor_port=9150
    for page_unique_identifier in ["Thiqari"]:
        # "alrasheedmedia","sharqiyatv","altaghiertv","Honaalbasraradio","Thiqari","kurdistan24.official","sul2024","AlANBARMDYNTYY","k24Arabic.Official" ,"964kurdi","964english","964Arabic"
        page=get_page_info(page_unique_identifier)
        Thread(target=scrape_page_videos,args=(page.get("page_id_2"),)).start()
        Thread(target=scrape_page_posts, args=(page.get("page_id"),)).start()
