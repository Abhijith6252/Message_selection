#import
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask import request
from casa_call import Return_msg 


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:password@localhost:5432/CASA"
db = SQLAlchemy(app)
migrate = Migrate(app, db)


class CmpgInfo(db.Model):
    __tablename__ = 'CmpgInfo'
    cmpg_id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.String())
    BU = db.Column(db.String())
    casa_id = db.Column(db.String())


class Messages(db.Model):
    __tablename__ = 'Messages'
    msg_id = db.Column(db.Integer, primary_key=True)
    casa_msg_id = db.Column(db.String())
    cmpg_id = db.Column(db.Integer, db.ForeignKey('CmpgInfo.cmpg_id'))


class LookUp(db.Model):
    __tablename__ = 'LookUp'
    id = db.Column(db.Integer, primary_key=True)
    cmpg_id = db.Column(db.Integer, db.ForeignKey('CmpgInfo.cmpg_id'))
    customer_id = db.Column(db.String())
    msg_id = db.Column(db.Integer, db.ForeignKey('Messages.msg_id'))
    click = db.Column(db.Boolean)
    buy = db.Column(db.Boolean)

#POST call when starting a new campaign
@app.route('/cmpg', methods=['POST'])
def create_cmpg():
    if request.method == 'POST':
        if request.is_json:
            data = request.get_json()
            new_cmpg = CmpgInfo(
                tenant_id=data['tenant_id'], BU=data['BU'], casa_id=data['casa_id'])
            db.session.add(new_cmpg)
            db.session.flush()
            msg_list = data['casa_msg_id']
            counter = new_cmpg.cmpg_id
            new_msg = []
            for msg_id in msg_list:
                new_msg.append(Messages(casa_msg_id=msg_id, cmpg_id=counter))
            db.session.add_all(new_msg)
            db.session.commit()
            return {"message": f"cmpg {new_cmpg.casa_id} has been created successfully."}
        else:
            return {"error": "The request payload is not in JSON format"}

#ulpoad the click buy info
@app.route('/upload', methods=['POST'])
def upload_lookup():
    if request.method == 'POST':
        if request.is_json:
            info = request.get_json()
            var1 = db.session.query(CmpgInfo).filter(
                CmpgInfo.tenant_id == info['tenant_id'], CmpgInfo.BU == info['BU'], CmpgInfo.casa_id == info['casa_id'])
            var2 = var1[0].cmpg_id
     
            var3 = db.session.query(Messages).filter(
                Messages.cmpg_id == var2, Messages.casa_msg_id == info['casa_msg_id'])
            var4 = var3[0].msg_id
    
            new_update = LookUp(
                cmpg_id=var2, msg_id=var4, customer_id=info['customer_id'], click=info['click'], buy=info['buy'])
            db.session.add(new_update)
            db.session.commit()
            return {"message": f"message uploaded"}
        else:
            return {"error": "The request payload is not in JSON format"}

#GET call to get the message ID for the customer
@app.route('/msg_return')
def get_msg():
    tenant_id = request.args.get('tenant_id')
    BU = request.args.get('BU')
    casa_id = request.args.get('casa_id')
    customer_id = request.args.get('customer_id')
    temp = db.session.query(CmpgInfo).filter(
        CmpgInfo.tenant_id == tenant_id, CmpgInfo.BU == BU, CmpgInfo.casa_id == casa_id)
    cmid = temp[0].cmpg_id
    message = Return_msg(customer_id, cmid)
    casa_message_id_var = db.session.query(
        Messages).filter(Messages.msg_id == message)
    casa_message_id = casa_message_id_var[0].casa_msg_id
    return 'the messsage id: {}'.format(casa_message_id)
