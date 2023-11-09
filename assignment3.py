import os
import requests
from flask import Flask, request, Response, jsonify

# Part1
# Create a Key-value Store that will support adding a new key-value pair to the store,
# retrieving and updating the value of an existing key, and deleting an existing key from the store.

# The store does not need to persist the key value dta, i.e., it can store data in memory only.
# If the store goes down and then gets started again, it does not need to contain the data
# it had before the crash

app = Flask(__name__) 
#forwarding_address = os.environ.get("FORWARDING_ADDRESS")
SOCKET_ADDRESS = os.environ.get("SOCKET_ADDRESS")
VIEW = os.environ.get("VIEW")


print("No forwarding address specified, running in main mode")
kv_store = {} # in-memory key-value store using dictionary
sa_store = {} # in-memory store for storing the set of replicas among which the store is replicated

# print("Broadcasting self to other replicas")
views = VIEW.split(",")
for view in views:
    if view != SOCKET_ADDRESS:
        # print(f"Sending PUT request to {view}")
        try:
            requests.put(f"http://{view}/view", json={"socket-address": SOCKET_ADDRESS})
        except requests.exceptions.ConnectionError:
            # print("Connection error")
            pass 
    
# View operations - “view” refers to the current set of replicas among which the store is replicated.
# A replica supports view operations to describe the current view, add a new replica to the view, or 
# delete an existing replica from the view.
    
@app.route('/view', methods=['GET', 'PUT', 'DELETE'])
def handle_view():
    if request.method == 'GET':
        # Return the current view of the store.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"view": ["<IP:PORT>", "<IP:PORT>", ...]}.
        return jsonify({"view": list(sa_store.keys())}), 200
        
    if request.method == 'PUT':
        # Add a new replica <socket-address> to the view.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "added"}.
        data = request.get_json()
        replica = data['socket-address']
        if replica in sa_store:
            return jsonify({"result": "already present"}), 200
        else:
            sa_store[replica] = True
            return jsonify({"result": "added"}), 201
            
    if request.method == 'DELETE':
        # Delete an existing replica <socket-address> from the view.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "deleted"}.
        data = request.get_json()
        replica = data['socket-address']
        if replica in sa_store:
            del sa_store[replica]
            return jsonify({"result": "deleted"}), 200
        else:
            return jsonify({"result": "View has no such replica"}), 404

# check the request type and process with HTTP status code and JSON body
@app.route('/kvs/<key>', methods=['PUT', 'GET', 'DELETE'])
def handle_key(key):
    # testing 
    # print(f"Method received: {request.method}, Key: {key}")
        
    # If the length of the key <key> is more than 50 characters, then return an error.
    # – Response code is 400 (Bad Request).
    # – Response body is JSON {"error": "Key is too long"}.
    if len(key) > 50:
        return jsonify({"error": "Key is too long"}), 400

    # PUT HTTP method
    # This endpoint is used to create or update key-value mappings in the store.
    # It is dictionary operations which add a new key.
    if request.method == 'PUT':
        try:
            data = request.get_json()
            value = data['value']
        # If the request body is not a JSON object with key "value", then return an error.
        # – Response code is 400 (Bad Request).
        # – Response body is JSON {"error": "PUT request does not specify a value"}
        except (TypeError, KeyError):
            # print("Exception caught: Either TypeError or KeyError")
            return jsonify({"error": "PUT request does not specify a value"}), 400

        # if the <key> already exists in the store, then update the mapping to point to the new <value>.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "replaced"}.
        if key in kv_store:
            kv_store[key] = value
            return jsonify({"result": "replaced"}), 200
        # Otherwise, If the key <key> does not exist in the store, add a new mapping from <key> to <value> into the store.
        # – Response code is 201 (Created).
        # – Response body is JSON {"result": "created"}
        else:
            kv_store[key] = value
            return jsonify({"result": "created"}), 201

    # GET HTTP method
    # This endpoint is used to read values from existing key-value mappings in the store. 
    # It is dictionary operations which look up keys and return values.
    elif request.method == 'GET':
        # If the key <key> exists in the store, then return the mapped value in the response.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "found", "value": "<value>"}
        if key in kv_store:
            return jsonify({"result": "found", "value": kv_store[key]}), 200
        # Otherwise, If the key does not exist in the store, then return an error.
        # – Response code is 404 (Not Found).
        # – Response body is JSON {"error": "Key does not exist"}.
        else:
            return jsonify({"error": "Key does not exist"}), 404

    # DELETE HTTP method
    # This endpoint is used to remove key-value mappings from the store. 
    # It is dictionary operations which delete keys.
    elif request.method == 'DELETE':
        # If the key <key> exists in the store, then remove it.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "deleted"}.
        if key in kv_store:
            del kv_store[key]
            return jsonify({"result": "deleted"}), 200
        # If the key <key> does not exist in the store, then return an error.
        # – Response code is 404 (Not Found).
        # – Response body is JSON {"error": "Key does not exist"}.
        else:
            return jsonify({"error": "Key does not exist"}), 404
    
if __name__=='__main__':
    app.run(host='0.0.0.0', port=8090)
