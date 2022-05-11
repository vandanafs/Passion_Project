from flask import Flask
from flask import jsonify
from models import *
from config  import *
from models import *
from flask import *
from flask_cors import CORS
from processData import *
#from config import db

# Flask constructor takes the name of
# current module (__name__) as argument. 
app = Flask(__name__)
app.config['SECRET_KEY'] = 'SuperSecretKey'
app.config['SQLALCHEMY_DATABASE_URI'] = conn
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False; 
CORS(app)

db.init_app(app)



@app.route('/')
def top_deals():
        productList = []
        print("Inside the app route")
        products = items.query.all()
        print("after query line")
        #print(products.values())
        #products = items.query.filter_by(Price=249.99).first()
        for product in products:
        #  print(product.__dict__)
        #json.dumps(list(products))
            dictret = dict(product.__dict__)
            dictret.pop('_sa_instance_state', None)
            productList.append(dictret)
        print(productList)
        sortedList = processdf(productList)
        return json.dumps(productList)
        #return "hello_world"


#main driver function
if __name__=='__main__':
    app.run(port=9000)
