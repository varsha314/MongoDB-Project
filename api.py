from datetime import datetime
from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
import pymongo

# Creating Flask instance
app = Flask(__name__)

# Configurations
app.config['MONGO_DBNAME'] = 'new'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/new'

# Establish a connection btw the Python application and MongoDB
client = pymongo.MongoClient()
db = client["new"]
mycol = db["add"]

mongo = PyMongo(app)


@app.route("/")
def home():
    return "<h1>QUERY</h1>"


# Implementation of Query 1
@app.route('/search_analytics/v1/zero_results', methods=['GET'])
def get_q1():
    # Accessing the date passed through URL
    date1 = request.args.get('date1', None)
    date2 = request.args.get('date2', None)
    try:
        # Convert string to datetime object
        d1 = datetime.strptime(date1, "%Y/%m/%d")
        d2 = datetime.strptime(date2, "%Y/%m/%d")
    except:
        # Error handling for improper date format
        return date_err("IMPROPER DATE FORMAT!! REQUIRED FORMAT %Y/%m/%d")
    else:
        # Convert datetime object to timestamp
        t1 = int(datetime.timestamp(d1))
        t2 = int(datetime.timestamp(d2))
        # Check for invalid date range
        if t2 <= t1:
            return date_err("INVALID DATE RANGE")
        # Aggregation pipeline for Query 1 implementation
        pipeline = [
            {"$match": {"$and": [{"num_found": {'$eq': 0}}, {"timestamp": {'$gte': t1, '$lte': t2}}]}},
            {'$group': {'_id': {"query": "$query", 'vendor': '$vendor'},
                        "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 100}
        ]
        q1 = db.add.aggregate(pipeline)
        result = list(q1)
        # Check whether the query return a matching document
        if len(result):
            return jsonify(result)
        else:
            message = {"message": "NO RESULTS FOUND"}
            return jsonify(message)


# Implementation of Query 2
@app.route('/search_analytics/v1/frequent_attributes', methods=['GET'])
def get_q2():
    # Accessing the date passed through URL
    date1 = request.args.get('date1', None)
    date2 = request.args.get('date2', None)
    try:
        # Convert string to datetime object
        d1 = datetime.strptime(date1, "%Y/%m/%d")
        d2 = datetime.strptime(date2, "%Y/%m/%d")
    except:
        # Error handling for improper date format
        return date_err("IMPROPER DATE FORMAT!! REQUIRED FORMAT %Y/%m/%d")
    else:
        # Convert datetime object to timestamp
        t1 = int(datetime.timestamp(d1))
        t2 = int(datetime.timestamp(d2))
        # Check for invalid date range
        if t2 <= t1:
            return date_err("INVALID DATE RANGE")
        # Aggregation pipeline for Query 2 implementation
        pipeline = [
            {"$match": {"timestamp": {'$gte': t1, '$lte': t2}}},
            {"$unwind": "$attributes"},
            {"$project": {"_id": 1, "category": 1, "attributes.color": "$attributes.color",
                          "attributes.category": "$attributes.category",
                          "attributes.discount": "$attributes.discount"}},
            {'$group': {'_id': {"category": "$category"},
                        "count": {"$sum": 1},
                        "attribute": {"$push": {"color": '$attributes.color', "category": "$attributes.category",
                                                "discount": "$attributes.discount"}}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
            {"$project": {"_id": "$_id._id", "attribute": 1}},
            {"$unwind": "$attribute"},
            {"$group": {
                "_id": {"category": "$attribute.category", "color": "$attribute.color"},
                "count": {"$sum": 1},
                "attribute": {"$push": {"color": '$attribute.color', "category": "$attribute.category",
                                        "discount": "$attribute.discount"}}}},
            {"$sort": {"count": -1}},
            {"$unwind": "$attribute"},
            {"$project": {"_id": 1, "count": 1, "attribute": 1}},

            {"$group": {"_id": {"category": "$_id.category"},
                        "color": {"$first": "$_id.color"},
                        "attribute": {"$push": {"color": '$attribute.color', "category": "$attribute.category",
                                                "discount": "$attribute.discount"}}}},

            {"$unwind": "$attribute"},
            {"$group": {"_id": {"category": "$_id.category", "color": "$color", "discount": "$attribute.discount"},
                        "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$group": {"_id": {"category": "$_id.category"},
                        "color": {"$first": "$_id.color"},
                        "discount": {"$first": "$_id.discount"}}}
        ]
        q2 = db.add.aggregate(pipeline)
        result = list(q2)
        # Check whether the query return a matching document
        if len(result):
            return jsonify(result)
        else:
            message = {"message": "NO RESULTS FOUND"}
            return jsonify(message)


# Error handling for incorrect URL
@app.errorhandler(404)
def not_found(error=None):
    message = {
        'message': 'Resource Not Found ' + request.url,
        'status': 404
    }
    response = jsonify(message)
    response.status_code = 404
    return response


# Error handling for dates
def date_err(value):
    message = {
        'message': value,
    }
    response = jsonify(message)
    return response


if __name__ == '__main__':
    app.run(debug=True)
