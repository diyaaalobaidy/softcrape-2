import datetime
from threading import Thread
import time
from typing import List
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship
from apscheduler.schedulers.background import BackgroundScheduler
from scraper import get_page_info, scrape_page_posts, scrape_page_videos

app=Flask("SOFTCRAPE")

app.config['SQLALCHEMY_DATABASE_URI']="sqlite:///test.db"#"postgresql://postgres:postgres@localhost:5432/softcrape"
db=SQLAlchemy(app)
class BaseModel(db.Model):
    __abstract__=True
    created_at=db.Column(db.DateTime, default=datetime.datetime.now)
    
    def save(self):
        """Save current record
        """
        try:
            db.session.add(self)
            db.session.commit()
        except Exception as e:
            print(e)
            db.session.rollback()
    
    def delete(self):
        try:
            db.session.delete(self)
            db.session.commit()
        except Exception as e:
            print(e)
            db.session.rollback()

class Page(BaseModel):
    __tablename__="facebook_pages"
    
    page_id=db.Column(db.String, primary_key=True)
    page_id_2=db.Column(db.String, unique=True)
    page_name=db.Column(db.String, nullable=False)
    page_url=db.Column(db.String, nullable=False)
    page_image=db.Column(db.String)
    tracker_enabled=db.Column(db.Boolean, default=True)
    tracker_period=db.Column(db.Integer, default=3)
    
    def get_json(self):
        return{
            "page_id": self.page_id,
            "page_id_2": self.page_id_2,
            "page_name": self.page_name,
            "page_url": self.page_url,
            "page_image": self.page_image,
        }
    
    def enable_tracker(self):
        self.tracker_enabled=True
        self.save()
        
    def disable_tracker(self):
        self.tracker_enabled=False
        self.save()

class Post(BaseModel):
    __tablename__="facebook_posts"
    
    post_id=db.Column(db.String, primary_key=True)
    publish_time=db.Column(db.DateTime, nullable=False)
    message=db.Column(db.String)
    comments=db.Column(db.Integer, default=0)
    views=db.Column(db.Integer, default=0)
    shares=db.Column(db.Integer, default=0)
    reactions=db.Column(db.JSON, default={})
    post_type=db.Column(db.String)
    page_id=db.Column(db.String, db.ForeignKey("facebook_pages.page_id"))
    page=relationship("Page", foreign_keys=[page_id])

    def get_json(self) -> dict:
        return {
            "created_at": self.created_at,
            "post_id": self.post_id,
            "publish_time": self.publish_time.isoformat(" "),
            "message": self.message,
            "comments": self.comments,
            "views": self.views,
            "shares": self.shares,
            "reactions": self.reactions,
        }

class Tracker(BaseModel):
    tracker_id=db.Column(db.String, primary_key=True)

class Target(BaseModel):
    target=db.Column(db.String, primary_key=True)
    target_type=db.Column(db.String,default="facebook")

with app.app_context():
    db.create_all()


@app.post("/post")
def save_post_endpoint():
    data=request.json
    page=Page.query.filter_by(page_id=str(data.get("page_id"))).first()
    if not page:
        page=Page.query.filter_by(page_id_2=str(data.get("page_id"))).first()
    post=Post.query.filter_by(post_id=str(data.get("post_id"))).first()
    if not post:
        post=Post(page=page, post_id=str(data.get("post_id")), publish_time=datetime.datetime.fromtimestamp(int(data.get("publish_time"))), message=data.get("message"), comments=data.get("comments"), views=data.get("views"), shares=data.get("shares"), reactions=data.get("reactions"))
    else:
        post.page=page
        post.post_id=str(data.get("post_id"))
        post.publish_time=datetime.datetime.fromtimestamp(int(data.get("publish_time")))
        post.message=data.get("message")
        post.comments=data.get("comments")
        post.views=data.get("views")
        post.shares=data.get("shares")
        post.reactions=data.get("reactions")
    post.save()
    return post.get_json()

@app.post("/page")
def save_page_endpoint():
    data=request.json
    page:Page=Page.query.filter_by(page_id=str(data.get("page_id"))).first()
    if not page:
        page:Page=Page(page_id=str(data.get("page_id")),page_id_2=str(data.get("page_id_2")),page_name=data.get("page_name"),page_url=data.get("page_url"),page_image=data.get("page_image"))
    else:
        page.page_id=str(data.get("page_id"))
        page.page_id_2=str(data.get("page_id_2"))
        page.page_name=data.get("page_name")
        page.page_url=data.get("page_url")
        page.page_image=data.get("page_image")
    page.save()
    return page.get_json()
@app.post("/tracker")
def create_new_tracker():
    request_data=request.json
    targets=request_data.get("target").split(",")
    target_type=request_data.get("type","facebook")
    for target in targets:
        try:
            Target(target=target, target_type=target_type).save()
        except: pass
    return targets

@app.delete("/tracker")
def stop_tracker():
    tracker_id=request.args.get("tracker_id")
    if tracker_id in active_trackers:
        active_trackers.pop(tracker_id)
    return tracker_id

active_trackers={}
def start_trackers(app:Flask):
    """Check the pages and create trackers accordingly"""
    with app.app_context():
        active_pages:List[Page]=Page.query.filter_by(tracker_enabled=True)
        for page in active_pages:
            if page.page_id not in active_trackers:
                # create new tracker for page posts
                active_trackers[page.page_id]=scrape_page_posts(page.page_id)
                print(active_trackers)
            if page.page_id_2 not in active_trackers:
                # create new tracker for page videos
                active_trackers[page.page_id_2]=scrape_page_videos(page.page_id_2)
                print(active_trackers)

def get_next_batch(start,step):
    """Get next values from the iterators"""
    print("Starting at {} with step {}".format(start,step))
    i=start
    while True:
        trackers=list(active_trackers.values())
        try:
            print(next(trackers[i],None))
            i+=step
        except StopIteration: pass
        except IndexError: pass
        except Exception as e:
            print("ERROR:",e, active_trackers)
            time.sleep(5)
            i=start

def facebook_target_starter(app:Flask):
    with app.app_context():
        target:Target=Target.query.filter_by(target_type="facebook").first()
        if target:
            try:
                print(target)
                page_identifier=target.target
                target.delete()
                get_page_info(page_identifier)
            except:
                Target(target=page_identifier, target_type="facebook")

scheduler=BackgroundScheduler()
scheduler.add_job(start_trackers, args=(app,), trigger="interval", seconds=5)
scheduler.add_job(facebook_target_starter, args=(app,), trigger="interval", seconds=5, max_instances=5)
scheduler.start()
number_of_threads=3
for i in range(number_of_threads):
    Thread(target=get_next_batch,args=(i,number_of_threads)).start()

if __name__=="__main__":
    app.run("localhost",45678)
