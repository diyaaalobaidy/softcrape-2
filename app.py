import datetime
import math
import sys
from threading import Thread
import time
from typing import List
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from sqlalchemy.orm import relationship
from apscheduler.schedulers.background import BackgroundScheduler
from scraper import get_page_info, scrape_page_posts, scrape_page_videos
from flask_restx import Api, Resource, fields, reqparse

app=Flask("SOFTCRAPE")
api=Api(app, "0.3", "Softcrape API", "An API for tracking social media pages.")
if sys.platform=="linux":
    app.config['SQLALCHEMY_DATABASE_URI']="postgresql://postgres:postgres@localhost:5432/softcrape_new"
else:
    app.config['SQLALCHEMY_DATABASE_URI']="sqlite:///test.db"
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
    tracker_period=db.Column(db.Integer, default=7)
    
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
            "page": self.page.get_json(),
            "page_id": self.page_id,
            "created_at": self.created_at.isoformat(" "),
            "post_id": self.post_id,
            "publish_time": self.publish_time.isoformat(" "),
            "message": self.message,
            "comments": self.comments,
            "views": self.views,
            "shares": self.shares,
            "reactions": self.reactions,
        }

class Target(BaseModel):
    target=db.Column(db.String, primary_key=True)
    target_type=db.Column(db.String,default="facebook")
    
    def get_json(self):
        return{
            "target": self.target,
            "type": self.target_type
        }

with app.app_context():
    db.create_all()

fb_ns=api.namespace("facebook", description="Facebook operations")

page_field=api.model('Page', dict(
    page_id=fields.String(description="Facebook page id"),
    page_id_2=fields.String(description="Second Facebook page id"),
    page_name=fields.String(description="Facebook page name"),
    page_url=fields.String(description="Facebook page url"),
))

page_list_field=api.model('Pages', dict(
    pages=fields.Nested(page_field, description="Facebook pages", as_list=True),
    total=fields.Integer(description="Total count"),
    fetched=fields.Integer(description="Fetched records count"),
    page=fields.Integer(description="Current page"),
    total_pages=fields.Integer(description="Total pages"),
))

post_field=api.model('Post', dict(
    page=fields.Nested(page_field,description="Facebook page id"),
    page_id=fields.String(description="Facebook page id"),
    created_at=fields.String(description="Timestamp of the post"),
    post_id=fields.String(description="Facebook post id"),
    publish_time=fields.String(description="Timestamp of the post"),
    message=fields.String(description="The body of the post"),
    comments=fields.Integer(description="Comments count"),
    views=fields.Integer(description="Views count"),
    shares=fields.Integer(description="Shares count"),
    reactions=fields.Nested(api.model('Reactions',dict(
            Like=fields.Integer(description="Like count"),
            Love=fields.Integer(description="Love count"),
            Care=fields.Integer(description="Care count"),
            Haha=fields.Integer(description="Haha count"),
            Wow=fields.Integer(description="Wow count"),
            Sad=fields.Integer(description="Sad count"),
            Angry=fields.Integer(description="Angry count")
        ))),
))


post_list_field=api.model('Pages', dict(
    posts=fields.Nested(post_field, description="Facebook pages", as_list=True),
    total=fields.Integer(description="Total count"),
    fetched=fields.Integer(description="Fetched records count"),
    page=fields.Integer(description="Current page"),
    total_pages=fields.Integer(description="Total pages"),
))

def ranged_int(min=None, max=None):
    def validate(value):
        value=int(value)
        if min is not None and value<min:
            raise ValueError("value must be more than or equal to {}".format(min))
        if max is not None and value<max:
            raise ValueError("value must be less than or equal to {}".format(max))
        return value
    return validate

pagination=reqparse.RequestParser()
pagination.add_argument("page",type=ranged_int(1),help="Page for pagination")
pagination.add_argument("limit",type=ranged_int(0),help="Items per page")
pagination.add_argument("sort_by",type=str,default="created_at", help="Sort by this field")
pagination.add_argument("descending",type=str, choices=["true","false"], default="true", help="Sorting descending")

@fb_ns.route("/post")
class PostRoutes(Resource):
    """
        Facebook post operations
    """
    @fb_ns.doc(False)
    @fb_ns.expect(post_field)
    @fb_ns.marshal_with(post_field, code=201)
    def post(self):
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
    
    @fb_ns.doc("Get posts")
    @fb_ns.expect(pagination)
    @fb_ns.marshal_list_with(post_list_field)
    def get(self):
        data=dict(pagination.parse_args(request))
        page=data.get("page",1) or 1
        limit=data.get("limit",10) or 10
        posts=Post.query.order_by(text("{} {}".format(data.get("sort_by"), "DESC" if data.get("descending")=="true" else "ASC")))
        total=posts.count()
        if limit>0:
            posts=posts.offset((page-1)*(limit)).limit(limit)
        response=dict(posts=[post.get_json() for post in posts], total=total, fetched=posts.count(), page=page, total_pages=math.ceil(total/limit))
        return response


@fb_ns.route("/post/<string:post_id>")
class PageRoutes(Resource):
    """
        Single facebook post operations
    """
    @fb_ns.doc("Get post by post_id")
    @fb_ns.marshal_with(post_field)
    @fb_ns.response(404, "Post not found")
    def get(self, post_id):
        post=Post.query.filter_by(post_id=post_id).first()
        if post:
            return post.get_json()
        fb_ns.abort(404, "Post with id {} not found".format(post_id))


@fb_ns.route("/post/page/<string:page_id>")
class PagePostsRoutes(Resource):
    """
        Single facebook post operations
    """
    @fb_ns.doc("Get posts by page_id")
    @fb_ns.marshal_list_with(post_list_field)
    @fb_ns.expect(pagination)
    def get(self, page_id):
        data=dict(pagination.parse_args(request))
        page=data.get("page",1) or 1
        limit=data.get("limit",10) or 10
        post=Post.query.filter_by(page_id=page_id).order_by(text("{} {}".format(data.get("sort_by"), "DESC" if data.get("descending")=="true" else "ASC")))
        total=posts.count()
        if limit>0:
            posts=posts.offset((page-1)*(limit)).limit(limit)
        total=posts.count()
        if limit>0:
            posts=posts.offset((page-1)*(limit)).limit(limit)
        response=dict(posts=[post.get_json() for post in posts], total=total, fetched=posts.count(), page=page, total_pages=math.ceil(total/limit))
        return response



@fb_ns.route("/page")
class PagesRoutes(Resource):
    """
        Facebook page operations
    """
    @fb_ns.doc(False)
    @fb_ns.expect(page_field)
    @fb_ns.marshal_with(page_field, code=201)
    def post(self):
        data=request.json
        page:Page=Page.query.filter_by(page_id=str(data.get("page_id"))).first()
        if not page:
            page:Page=Page(page_id=str(data.get("page_id")),page_id_2=str(data.get("page_id_2")),page_name=data.get("page_name"),page_url=data.get("page_url"),page_image=data.get("page_image"))
        else:
            page.page_id=str(data.get("page_id"))
            page.page_id_2=str(data.get("page_id_2"))
            page.page_name=data.get("page_name")
            page.page_url=data.get("page_url")
        page.save()
        return page.get_json()
    
    @fb_ns.doc("Get pages")
    @fb_ns.expect(pagination)
    @fb_ns.marshal_list_with(page_list_field)
    def get(self):
        data=dict(pagination.parse_args(request))
        page=data.get("page",1) or 1
        limit=data.get("limit",10) or 10
        pages=Page.query.order_by(text("{} {}".format(data.get("sort_by"), "DESC" if data.get("descending")=="true" else "ASC")))
        total=pages.count()
        if limit>0:
            pages=pages.offset((page-1)*(limit)).limit(limit)
        response=dict(pages=[page.get_json() for page in pages], total=total, fetched=pages.count(), page=page, total_pages=math.ceil(total/limit))
        return response

@fb_ns.route("/page/<string:page_id>")
class PageRoutes(Resource):
    """
        Single facebook page operations
    """
    @fb_ns.doc("Get page by page_id or page_id_2")
    @fb_ns.marshal_with(page_field)
    @fb_ns.response(404, "Page not found")
    def get(self, page_id):
        page=Page.query.filter_by(page_id=page_id).first()
        if not page:
            page=Page.query.filter_by(page_id_2=page_id).first()
        if page:
            return page.get_json()
        fb_ns.abort(404, "Page with id {} not found".format(page_id))

tracker_ns=api.namespace("tracker", description="Tracker operations")

tracker_field=api.model('Tracker', dict(
    target=fields.String(default="964Arabic",description="Target to track, for facebook it must be the page id or just the username, not URL."),
    type=fields.String(default="facebook",description="Target type (lowercase), \"facebook\" for facebook targets.")
))

@tracker_ns.route("/")
class TrackerRoutes(Resource):
    """
        Tracker operations
    """
    @tracker_ns.doc("Create a new tracker")
    @tracker_ns.expect(tracker_field)
    @tracker_ns.marshal_list_with(tracker_field)
    def post(self):
        request_data=request.json
        targets=request_data.get("target").split(",")
        target_type=request_data.get("type","facebook")
        response=[]
        for target in targets:
            try:
                new_target=Target(target=target, target_type=target_type)
                new_target.save()
                response.append(new_target.get_json())
            except: pass
        return response

    @fb_ns.doc("Get queued trackers")
    @fb_ns.expect(pagination)
    @fb_ns.marshal_list_with(tracker_field)
    def get(self):
        data=dict(pagination.parse_args(request))
        page=data.get("page",1) or 1
        limit=data.get("limit",10) or 10
        trackers=Target.query.order_by(text("{} {}".format(data.get("sort_by"), "DESC" if data.get("descending")=="true" else "ASC")))
        total=trackers.count()
        if limit>0:
            trackers=trackers.offset((page-1)*(limit)).limit(limit)
        response=dict(trackers=[tracker.get_json() for tracker in trackers], total=total, fetched=trackers.count(), page=page, total_pages=math.ceil(total/limit))
        return response


    @tracker_ns.doc("Stop a tracker")
    @tracker_ns.marshal_with(tracker_field)
    def delete(self):
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

def get_next_batch(start,step,app):
    """Get next values from the iterators"""
    print("Starting at {} with step {}".format(start,step))
    i=start
    start_trackers(app)
    while True:
        trackers=list(active_trackers.values())
        try:
            print(next(trackers[i],None))
            i+=step
        except StopIteration:
            trackers.pop(i)
        except IndexError:
            start_trackers(app)
            i=start
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
                Target(target=page_identifier, target_type="facebook").save()
if sys.platform=="linux":
    scheduler=BackgroundScheduler()
    # scheduler.add_job(start_trackers, args=(app,), trigger="interval", seconds=5)
    scheduler.add_job(facebook_target_starter, args=(app,), trigger="interval", seconds=5, max_instances=5)
    scheduler.start()
    number_of_threads=5
    for i in range(number_of_threads):
        Thread(target=get_next_batch,args=(i,number_of_threads,app)).start()

if __name__=="__main__":
    app.run("localhost",45679)