######################
#   Fei Xu z5239235  #
#   Comp9321 Assn2   #
######################

import sqlite3
from flask import *
from flask_restx import *
import urllib.request as u
import json
import time
import os
import matplotlib.pyplot as plt
import pandas as pd
import re
from datetime import datetime
from flask import send_file

'''
The first 4 functions are used to realize CURD in my DB: "my_tvshow.db".
The port can be modified.

Notes:
Q1: I Only store the matched tv show in DB.
Q2-Q3: Use normal sqlite3 method.
Q4: To get the displayed output, I created several api models.
Q5: The only question I used dataframe to get sorted results.
Q6: I used send_file() function

Summary：
The most difficult and annoying part is the return style.
I must always be careful to process every list and dict to get the desired output.
Q5 is the most difficult part I think.
This assignment teaches me a lot.
Thanks for your marking and have a good day.
'''


def DB_create(dbname):
    if os.path.exists(dbname):
        return 'Alreadly created'
    cnt = sqlite3.connect(dbname)
    cur = cnt.cursor()
    cur.execute('''CREATE TABLE "tvshow" (
    	"tvmaze_id"	NUMERIC NOT NULL UNIQUE,
    	"id"	INTEGER NOT NULL UNIQUE,
    	"last_update"	TEXT NOT NULL,
    	"name"	TEXT NOT NULL,
    	"type"	TEXT ,
    	"language"	TEXT,
    	"genres"	TEXT ,
    	"status"	TEXT ,
    	"runtime"	NUMERIC ,
    	"premiered"	TEXT ,
    	"officialSite"	TEXT ,
    	"schedule"	TEXT ,
    	"rating"	TEXT,
    	"weight"	NUMERIC ,
    	"network"	TEXT ,
    	"summary"	TEXT ,
    	PRIMARY KEY("id"  AUTOINCREMENT)
    );''')
    cur.close()
    cnt.close()


def DB_insert(insert_data):
    cnt = sqlite3.connect('my_tvshow.db')
    cur = cnt.cursor()
    #################
    querry2 = 'insert into tvshow (tvmaze_id,last_update,name,type,language,genres,status,' \
              'runtime,premiered,officialSite,schedule,rating,weight,network,summary) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    #################
    insert_data = tuple(insert_data)
    cur.execute(querry2, insert_data)
    cnt.commit()
    cur.close()
    cnt.close()
    return 'ok'


def DB_search(querry):
    if not os.path.exists('my_tvshow.db'):
        return -1
    con = sqlite3.connect('my_tvshow.db')
    cur = con.cursor()
    cur.execute(querry)
    rez = cur.fetchall()
    cur.close()
    con.close()
    return rez


def DB_delete(querry):
    con = sqlite3.connect('my_tvshow.db')
    cur = con.cursor()
    cur.execute(querry)
    con.commit()
    cur.close()
    con.close()


def DB_update(querry, data):
    con = sqlite3.connect('my_tvshow.db')
    cur = con.cursor()
    cur.execute(querry, data)
    con.commit()
    cur.close()
    con.close()


def links(ids, id, style):
    '''
    :param ids: id list
    :param id: current id
    :param style: href temple
    :return: _links dict
    '''
    index = ids.index(id)
    self_link = {"href": style.format(ID=id)}

    if len(ids) == 1:
        agg = {'self': self_link}
        return agg
    else:
        if index - 1 < 0:
            next_link = {"href": style.format(ID=ids[index + 1])}
            agg = {'self': self_link, 'next': next_link}
            return agg
        if index + 2 > len(ids):
            pre_link = {"href": style.format(ID=ids[index - 1])}
            agg = {'self': self_link, 'previous': pre_link}
            return agg

    pre_link = {"href": style.format(ID=ids[index - 1])}
    next_link = {"href": style.format(ID=ids[index + 1])}
    agg = {'self': self_link, 'previous': pre_link, 'next': next_link}
    return agg


# Create api
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
api = Api(app, title='Comp9321 Assn2', description='Z5239235 Fei Xu')

# Model used for Q4 update
country = api.model('tv_model_network_country',
                    {"name": fields.String, "code": fields.String, "timezone": fields.String})
schedule_model = api.model('tv_model_schedule', {"time": fields.String, "days": fields.List(fields.String)})
rating_model = api.model('tv_model_rating', {"average": fields.Float})
network_model = api.model('tv_model_network',
                          {"id": fields.Integer, "name": fields.String, "country": fields.Nested(country)})
tvshow_model = api.model('tvshow', {
    "tvmaze-id": fields.Integer,
    "name": fields.String,
    "type": fields.String,
    "language": fields.String,
    "genres": fields.List(fields.String),
    "status": fields.String,
    "runtime": fields.Integer,
    "premiered": fields.String,
    "officialSite": fields.String,
    "schedule": fields.Nested(schedule_model),
    "rating": fields.Nested(rating_model),
    "weight": fields.Integer,
    "network": fields.Nested(network_model),
    "summary": fields.String
})

# parser Question1
parser = reqparse.RequestParser()
parser.add_argument('name', type=str, required=True, help='Q1: input a show name', location='args')

# parser Question5
parser1 = reqparse.RequestParser()
parser1.add_argument('order_by', type=str, default='+id', help='Q5: input your order', location='args')
parser1.add_argument('page', type=int, default='1', help='Q5: input page number', location='args')
parser1.add_argument('page_size', type=int, default='100', help='Q5: input Page size', location='args')
parser1.add_argument('filter', type=str, default='id,name', help='Q5: input filter', location='args')

# parser Question6
parser2 = reqparse.RequestParser()
parser2.add_argument('format', type=str, required=True, help='Q6: image or json', location='args')
parser2.add_argument('by', type=str, required=True, help='Q6: pick one from (language, genres, status, and type)',
                     location='args')


#########################
#       Question1       #
#########################
@api.route('/tv-shows/import')
class Import(Resource):
    @api.doc(description="Question_1")
    @api.response(404, 'NOT FOUND')
    @api.response(200, 'OK')
    @api.response(400, 'BAD REQUEST')
    @api.expect(parser, validate=True)
    def post(self):
        try:
            # get the post param
            querry_name = parser.parse_args()['name'].replace(' ', '-').lower()
            querry_url = 'http://api.tvmaze.com/search/shows?q={string}'.format(string=querry_name)
            tvdata = json.loads(u.urlopen(querry_url).read())

            # The most matched name of tv show
            # If no data error return 404
            if not tvdata:
                return make_response(jsonify({
                    'message': 'Not found'
                }), 404)

            d2 = tvdata[0]['show']
            # Some of the elements are str type, use json.dumps
            genres, schedule, rating, network, tvname = json.dumps(d2['genres']), json.dumps(
                d2['schedule']), json.dumps(
                d2['rating']), json.dumps(d2['network']), d2['name']

            # Create DB
            DB_create('my_tvshow.db')

            # Not match 404, Else continue
            if querry_name != tvname.replace(' ', '-').lower():
                return make_response(jsonify({'message': 'No show named {}'.format(parser.parse_args()['name'])}), 404)
            else:
                # Matched pick the first and store in my DB
                querry1 = 'SELECT name FROM tvshow'
                names_in = DB_search(querry1)
                names_in = [i[0].replace(" ", "-").lower() for i in names_in]

                if querry_name in names_in:
                    msg = 'already exist, ready for pick data'
                    index = names_in.index(querry_name)
                else:
                    msg = 'New data able to insert'
                    insert_data = [d2['id'], time.strftime('%Y-%m-%d-%H:%M:%S', time.localtime()), d2['name'],
                                   d2['type'], d2['language'], genres, d2['status'], d2['runtime'], d2['premiered'],
                                   str(d2['officialSite']), schedule, rating, d2['weight'], network, d2['summary']]
                    index = -1
                    DB_insert(insert_data)

                querry2 = 'select id from tvshow'
                ids = DB_search(querry2)
                ids = [tuple[0] for tuple in ids] if len(ids) else []
                querry_final = 'SELECT * FROM tvshow WHERE name="{}"'.format(tvname)
                info_tuple = DB_search(querry_final)[-1]

                rez = {"id": info_tuple[1],
                       "last-update": info_tuple[2],
                       "tvmaze-id": info_tuple[0],
                       "_links": links(ids, ids[index], style)}
                return make_response(jsonify(rez), 201)

        except sqlite3.Error as e:
            return make_response(jsonify({
                'message': str(e)
            }), 400)


#########################
#      Question2,3,4    #
#########################
@api.route('/tv-shows/<int:id>')
class Tvshow(Resource):
    @api.doc(description="Question_2")
    @api.response(404, 'NOT FOUND')
    @api.response(200, 'OK')
    @api.response(400, 'BAD REQUEST')
    def get(self, id):
        try:
            querry1 = 'select id from tvshow'
            ids = DB_search(querry1)
            if ids == -1:
                return make_response(jsonify({'message': "No found a database"}), 400)
            ids = [tuple[0] for tuple in ids] if len(ids) else []
            if len(ids) == 0:
                return make_response(
                    jsonify({'message': "Not found with id {}, the tvshow table is empty now".format(id)}), 400)

            if id in ids:
                querry = 'select * from tvshow where id={}'.format(id)
                info_tuple = DB_search(querry)[-1]
                rez = {"tvmaze-id": info_tuple[0],
                       "id": info_tuple[1],
                       "last-update": info_tuple[2],
                       "name": info_tuple[3],
                       "type": info_tuple[4],
                       "language": info_tuple[5],
                       "genres": json.loads(info_tuple[6]),
                       "status": info_tuple[7],
                       "runtime": info_tuple[8],
                       "premiered": info_tuple[9],
                       "officialSite": info_tuple[-6],
                       "schedule": json.loads(info_tuple[-5]),
                       "rating": json.loads(info_tuple[-4]),
                       "weight": info_tuple[-3],
                       "network": json.loads(info_tuple[-2]),
                       "summary": info_tuple[-1],
                       "_links": links(ids, id, style)
                       }
                return make_response(jsonify(rez), 200)

            else:
                if id < ids[-1]:
                    return make_response(
                        jsonify({"message": "The tv show with id {} was removed from the database!".format(id),
                                 "id": id}), 200)
                else:
                    return make_response(jsonify({"message": "Not found with id {}".format(id)}), 404)
        except sqlite3.Error as e:
            return make_response(jsonify({
                'message': str(e)
            }), 400)

    @api.doc(description="Question_3")
    @api.response(404, 'NOT FOUND')
    @api.response(200, 'OK')
    @api.response(400, 'BAD REQUEST')
    def delete(self, id):
        try:
            querry1 = 'select id from tvshow'
            ids = DB_search(querry1)
            if ids == -1:
                return make_response(jsonify({'message': "No found a database"}), 400)
            ids = [tuple[0] for tuple in ids] if len(ids) else []
            if id in ids:
                querry = 'delete from tvshow where id ={}'.format(id)
                DB_delete(querry)

                rez = {"message": "The tv show with id {} was removed from the database!".format(id),
                       "id": '{}'.format(id)}
                return make_response(rez, 200)
            else:
                return make_response({'message': 'Not found id with {}'.format(id)}, 404)
        except sqlite3.Error as e:
            return make_response(jsonify({
                'message': str(e)
            }), 400)

    @api.doc(description="Question_4")
    @api.response(404, 'NOT FOUND')
    @api.response(200, 'OK')
    @api.response(400, 'BAD REQUEST')
    @api.expect(tvshow_model)
    def put(self, id):
        try:
            ids = DB_search('select id from tvshow')
            if ids == -1:
                return make_response(jsonify({'message': "No found a database"}), 400)
            ids = [tuple[0] for tuple in ids] if len(ids) else []
            # No such id 404
            if id not in ids:
                return make_response(jsonify({'message': "Id with {} doesn't exist".format(id)}), 400)

            # payload
            tv = request.json

            # test all input key
            for i in tv.keys():
                if i not in tvshow_model.keys():
                    return make_response(jsonify({"message": "Property {} is invalid".format(i)}), 400)

            # update
            update_key, update_value = [], []
            q = ''
            for i in tv.keys():
                q += i.replace('-', '_') + '=?, '
                if i in ('genres', 'schedule', 'rating', 'network', 'tvname'):
                    update_value.append(json.dumps(tv[i]))
                else:
                    update_value.append(tv[i])

            q = q + 'last_update=?'
            querry = 'update tvshow set {KEY} where id = {ID}'.format(KEY=q, ID=id)
            update_value.append(time.strftime('%Y-%m-%d-%H:%M:%S', time.localtime()))
            updata_tuple = tuple(update_value)
            DB_update(querry, updata_tuple)

            querry2 = 'select id from tvshow'
            ids = DB_search(querry2)
            ids = [tuple[0] for tuple in ids] if len(ids) else []
            querry_final = 'SELECT * FROM tvshow WHERE id="{}"'.format(id)
            info_tuple = DB_search(querry_final)[-1]
            rez = {"id": info_tuple[1],
                   "last-update": info_tuple[2],
                   "_links": links(ids, id, style)}

            return make_response(jsonify(rez), 200)
        except sqlite3.Error as e:
            return make_response(jsonify({
                'message': str(e)
            }), 400)


#########################
#       Question5       #
#########################
@api.route('/tv-shows')
class Tvshow(Resource):
    @api.doc(description="Question_5, This function can only used when database is not empty")
    @api.response(404, 'NOT FOUND')
    @api.response(200, 'OK')
    @api.response(400, 'BAD REQUEST')
    @api.expect(parser1)
    def get(self):
        order, psize = parser1.parse_args()['order_by'], parser1.parse_args()['page_size']
        page, filter = parser1.parse_args()['page'], parser1.parse_args()['filter']
        allowed_filter = ['tvmaze_id', 'id', 'last_update', 'name', 'type', 'language', 'genres', 'status', 'runtime',
                          'premiered', 'officialSite', 'schedule', 'rating', 'weight', 'network', 'summary']
        allowed_order = ['id', 'name', 'runtime', 'premiered', 'rating']
        # convert db to df
        cnt = sqlite3.connect('my_tvshow.db')
        df = pd.read_sql_query("SELECT * FROM tvshow", cnt)

        order_element = [i[1:].replace('-', '_').replace('rating_average', 'rating') for i in order.split(',')]
        filter_element = [i.replace('-', '_') for i in filter.split(',')]
        if len(set(order_element + allowed_order)) > len(set(allowed_order)) or len(
                set(filter_element + allowed_filter)) > len(set(allowed_filter)):
            return make_response(jsonify({
                'message': 'Invaild orderby or filter input'
            }), 404)

        asd = []
        for i in order.split(','):
            tf = 1 if i[:1] == '+' else 0
            asd.append(tf)
        df1 = df.sort_values(by=order_element, ascending=asd).copy()
        rez = list(df1[filter_element].T.to_dict().values())

        a = []
        for i in rez:
            for k, v in i.items():
                if k in ["genres", "schedule", "rating", "network"]:
                    a.append(json.loads(v))
        for i in rez:
            for k, v in i.items():
                if k in ["genres", "schedule", "rating", "network"]:
                    i[k] = a[rez.index(i)]

        # Test page and pages size
        if len(rez) / psize % 1 != 0:
            max_page = int(len(rez) / psize) + 1
        else:
            max_page = int(len(rez) / psize)
        if page > max_page:
            return make_response(jsonify({
                'message': 'Invalid page number'
            }), 404)
        if page <= 0 or psize <= 0:
            return make_response(jsonify({
                'message': 'Invalid page number'
            }), 404)

        if page * psize > len(rez):
            begin = (page - 1) * psize
            end = len(rez)

        else:
            begin = (page - 1) * psize
            end = (page) * psize

        s1 = "http://{host}:{port}/tv-shows?order_by={order}&page=".format(host=host, port=port, order=order)
        s2 = "&page_size={ps}&filter={f}".format(ps=psize, f=filter)
        s = s1 + '{ID}' + s2
        ids = [i for i in range(1, max_page + 1)]
        output = {"page": page, "page-size": psize,
                  "tv-shows": rez[begin:end],
                  "_links": links(ids, page, s)}
        return make_response(jsonify(output), 200)


#########################
#       Question6       #
#########################
@api.route('/tv-shows/statistics')
class Statistics(Resource):
    @api.doc(description="Question_6")
    @api.response(404, 'NOT FOUND')
    @api.response(200, 'OK')
    @api.response(400, 'BAD REQUEST')
    @api.expect(parser2, validate=True)
    def get(self):
        try:
            format = parser2.parse_args()['format'].lower()
            by = parser2.parse_args()['by'].lower()

            if format in ['json', 'image'] and by in ['language', 'genres', 'status', 'type']:
                temp = DB_search('select {},last_update from tvshow'.format(by))
                rez = []
                for i in temp:
                    diff = datetime.now() - datetime.strptime(i[-1], "%Y-%m-%d-%H:%M:%S")
                    r = (diff).seconds / 3600.0
                    if r < 24:
                        rez.append(i[0])
                # Count
                total = len(temp)
                total_update = len(rez)

                # Process data
                rez = re.findall(r'"(\w+)"', str(rez)) if by == 'genres' else rez
                counts = pd.value_counts(rez).to_dict()

                labels, data = [], []
                for key, value in counts.items():
                    labels.append(key)
                    data.append(value)

                # Draw pie chart and save
                if format == 'image':
                    #
                    plt.figure(figsize=(10, 9))
                    plt.pie(data, labels=labels, autopct='%1.2f%%', textprops={'fontsize': 14, 'color': 'black'},
                            labeldistance=1)
                    plt.axis('equal')
                    plt.title('{} distribution of TV show'.format(by), color='Black', fontsize='xx-large',
                              fontweight='heavy',
                              bbox=dict(edgecolor='blue', alpha=0.15))
                    plt.savefig('{}.jpg'.format(by))

                    return make_response(send_file('{}.jpg'.format(by), mimetype='image/jpg'),
                                         200)
                # Json output
                else:
                    sum = 0
                    for i in data: sum += i
                    percentage = [round(i / sum * 100, 1) for i in data]
                    values = dict(zip(labels, percentage))

                    return make_response(jsonify({
                        "total": total,
                        "total-updated": total_update,
                        "values": values
                    }), 200)

            else:
                return make_response(jsonify({
                    'message': 'Invalid input'
                }), 404)
        except sqlite3.Error as e:
            return make_response(jsonify({
                'message': str(e)
            }), 400)


if __name__ == '__main__':
    host = '127.0.0.1'
    port = 5031
    style = "http://{}/tv-shows/".format(host + ':' + str(port)) + "{ID}"
    app.run(port=port)
