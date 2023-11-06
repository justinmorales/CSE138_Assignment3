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
forwarding_address = os.environ.get("FORWARDING_ADDRESS")

if forwarding_address is None:
    print("No forwarding address specified, running in main mode")
    kv_store = {} # in-memory key-value store using dictionary

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

        
else:
    print("Forwarding address specified, running in forwarding mode")
    print("Forwarding address: " + forwarding_address)
    
    @app.route('/kvs/<key>', methods=['GET','PUT','DELETE'])
    def forward_key_request(key):
        url = f"http://{forwarding_address}/kvs/{key}"
        try:
            if request.method == 'GET':
                response =  requests.get(url)
            elif request.method == 'PUT':
                response = requests.put(url, json=request.get_json())
            elif request.method == 'DELETE':
                response = requests.delete(url)
            #elif request.method == 'POST':
            #    response = requests.post(request_to_main, json=request.get_json())
            else:
                return Response("Method Not Allowed",status=405)
            return jsonify(response.json()), response.status_code
        except requests.exceptions.RequestException as e:
            #print(e)
            return jsonify({"error": "Cannot forward request"}), 503
        
if __name__=='__main__':
    app.run(host='0.0.0.0', port=8090)
