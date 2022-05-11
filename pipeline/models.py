from flask_sqlalchemy import SQLAlchemy
import pymysql
from sqlalchemy import create_engine
from sqlalchemy import Column,Integer,String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import db
import uuid

class items(db.Model):
    id = db.Column(db.Integer,primary_key=True)
    Title = db.Column(db.String(40960))
    Image = db.Column(db.String(40960))
    Rating = db.Column(db.String(40960))
    ProdLink = db.Column(db.String(40960)) 
    TotalReviews = db.Column(db.String(40960))
    Price = db.Column(db.String(40960))
    OrgPrice = db.Column(db.String(40960))
    CouponsDesc = db.Column(db.String(40960))
    CouponsSummery = db.Column(db.String(40960))
    CouponPrice  = db.Column(db.String(40960))

    def __init__(self,Title,Image,Rating,ProdLink,TotalReviews,Price,OrgPrice,CouponsDesc,CouponsSummery,CouponPrice):
        self.Title = Title
        self.Image = Image
        self.Rating = Rating
        self.ProdLink = ProdLink
        self.TotalReviews = TotalReviews
        self.Price = Price
        self.OrgPrice = OrgPrice
        self.CouponsDesc = CouponsDesc
        self.CouponsSummery = CouponsSummery
        self.CouponPrice = CouponPrice
        