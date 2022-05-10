from flask import Flask
from flask import jsonify
from models import *
from config  import *

app1 = Flask(__name__)
app1.config['SECRET_KEY'] = 'SuperSecretKey'
app1.config['SQLALCHEMY_DATABASE_URI'] = conn
app1.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False; 
db = SQLAlchemy(app1)
db.imit_app(app1)



@app.route('/')
def products_list():
    prod_list=[]
    products=items.query.all()

    for product in products:
        prod_dict=dict(product.__dict__) # converting each prod into dict
        prod_dict.append(prod_dict)
    print(prod_list)
    return jsonify(prod_list)    
