import os
import requests
import time
from flask import Flask, request, Response, jsonify

# Part1
# Create a Key-value Store that will support 
# 1) adding a new key-value pair to the store,
# 2) retrieving and updating the value of an existing key
# 3) deleting an existing key from the store.

# The store does not need to persist the key value dta, i.e., it can store data in memory only.
# If the store goes down and then gets started again, it does not need to contain the data it had before the crash

app = Flask(__name__)
SOCKET_ADDRESS = os.environ.get("SOCKET_ADDRESS")
VIEW = os.environ.get("VIEW")

kv_store = {} # in-memory key-value store using dictionary
sa_store = {} # in-memory store for storing the set of replicas among which the store is replicated
vector_clock = [0,0,0]

# Create a function to increment vector clock based on each replica to ensure the causal consistency
def inc_vector_clock():
    if SOCKET_ADDRESS == "10.10.0.2:8090":
        vector_clock[0] += 1
    elif SOCKET_ADDRESS == "10.10.0.3:8090":
        vector_clock[1] += 1
    elif SOCKET_ADDRESS == "10.10.0.4:8090":
        vector_clock[2] += 1

# Create a function to update vector clock based on each replica to ensure eventual consistency
def update_vector_clock(v):
        vector_clock[0] = max(vector_clock[0], v[0])
        vector_clock[1] = max(vector_clock[1], v[1])
        vector_clock[2] = max(vector_clock[2], v[2])

# Create a function to send http request based on method
def send_http_request(url, method, data):
    try:
        if method == 'PUT':
            return requests.put(url, json=data)
        elif method == 'DELETE':
            return requests.delete(url, json=data)
        else:
            return requests.get(url)
    except requests.exceptions.ConnectionError:
        return None

# Create a function to check if the length of the key <key> is more than 50 characters
def is_key_valid(key):
    return len(key) < 50

# Create a function to replicate data based on method
def broadcast(key, value, method):
    for replica in sa_store:
        if replica != SOCKET_ADDRESS:
            try:
                url = f"http://{replica}/kvs/{key}"
                if method == 'PUT':
                    send_http_request(url, method, {"value": value})
                else: # method = 'DELETE'
                    send_http_request(url, method)
                inc_vector_clock()
            except requests.exceptions.ConnectionError: # if not reachable
                # if a replica is not reachable
                # throws error **503 Service unavailable {"error": "Causal dependencies not satisfied; try again later"}**.
                # Make sure that the write reaches that replica eventually.
                # One method would be to sleep 1 second and retry the write at that replica until it is successful. This is buffering messages at the sender.
                while True:
                    try:
                        url = f"http://{replica}/kvs/{key}"
                        if method == 'PUT':
                            response = send_http_request(url, method, {"value": value})
                        else:  # method = 'DELETE'
                            response = send_http_request(url, method)
                        
                        if response and response.status_code == 200:
                            inc_vector_clock()
                            break
                        else:
                            time.sleep(1)
                    except requests.exceptions.ConnectionError:
                        time.sleep(1)
                # deletes the replica from the store by requesting to view to delete
                requests.delete(f"http://{view}/view", json={"socket-address": replica})
                return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
    
    return None # make sure it successfully broadcasts to all replicas

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
        else:
            # print("PUT request successful")
            sa_store[view] = True
    if view == SOCKET_ADDRESS:
        sa_store[view] = True
        pass
    
# View operations - “view” refers to the current set of replicas among which the store is replicated.
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
            # send entire kv_store to the new replica
            for key, value in kv_store.items():
                send_http_request(f"http://{replica}/kvs/{key}", 'put', {"value": value})
            return jsonify({"result": "added"}), 201
            
    if request.method == 'DELETE':
        # Delete an existing replica <socket-address> from the view.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "deleted"}.
        data = request.get_json()
        replica = data['socket-address']
        if replica in sa_store:
            del sa_store[replica]
            #broadcasts DELETE-view requests to other replicas
            for view in views:
                if view != SOCKET_ADDRESS:
                    send_http_request(f"http://{view}/view", 'delete', {"socket-address": replica})
            return jsonify({"result": "deleted"}), 200
        else:
            return jsonify({"result": "View has no such replica"}), 404

# check the request type and process with HTTP status code and JSON body
@app.route('/kvs/<key>', methods=['PUT', 'GET', 'DELETE'])
def handle_key(key):
    # Request body is JSON {"value": <value>, "causal-metadata": <V>}.
    # <V> is null when the PUT does not depend on prior writes.

    # If the length of the key <key> is more than 50 characters, then return an error.
    # – Response code is 400 (Bad Request).
    # – Response body is JSON {"error": "Key is too long"}.
    if not is_key_valid(key):
        return jsonify({"error": "Key is too long"}), 400

    # PUT HTTP method
    # This endpoint is used to create or update key-value mappings in the store.
    # It is dictionary operations which add a new key.
    if request.method == 'PUT':
        try:
            data = request.get_json()
            value = data['value']
        except (TypeError, KeyError):
            # print("Exception caught: Either TypeError or KeyError")
            return jsonify({"error": "PUT request does not specify a value"}), 400

        update_vector_clock(data['causal-metadata'])
        inc_vector_clock()
        result = "created" if key not in kv_store else "replaced"
        kv_store[key] = value
        broadcast(key, value, 'PUT')
        return jsonify({"result": result}), 200 if result == "replaced" else 201
        # If the request body is not a JSON object with key "value", then return an error.
        # – Response code is 400 (Bad Request).
        # – Response body is JSON {"error": "PUT request does not specify a value"}

        # if the <key> already exists in the store, then update the mapping to point to the new <value>.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "replaced", "causal-metadata": <V'>}.
        #    *The <V'> here and in the 201 response indicates a causal dependency on <V> and this PUT.
        # Otherwise, If the key <key> does not exist in the store, add a new mapping from <key> to <value> into the store.
        # – Response code is 201 (Created).
        # – Response body is JSON {"result": "created", "causal-metadata": <V'>}

    # GET HTTP method
    # This endpoint is used to read values from existing key-value mappings in the store. 
    # It is dictionary operations which look up keys and return values.
    elif request.method == 'GET':
        # Request body is JSON {"causal-metadata": <V>}.
        # – The <V> is null when the client does not know of prior writes.
        # – 503 (Service Unavailable) {"error": "Causal dependencies not satisfied; try again later"}
        update_vector_clock(data['causal-metadata'])

        inc_vector_clock()
        # If the key <key> exists in the store, then return the mapped value in the response.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "found", "value": "<value>", "causal-metadata": <V'>}
        #    ∗ The <V'> indicates a causal dependency on the PUT of <key>,<value>.
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
        
        # Request body is JSON {"causal-metadata": <V>}.
        # – The <V> is null when the DELETE does not depend on prior writes. Note: This should never
        # happen. Think about why.
        # – 503 (Service Unavailable) {"error": "Causal dependencies not satisfied; try again later"}
        update_vector_clock(data['causal-metadata'])
        inc_vector_clock()
        # If the key <key> exists in the store, then remove it.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "deleted", "causal-metadata": <V'>}.
        # ∗ The <V'> indicates a causal dependency on <V> and this DELETE
        if key in kv_store:
            del kv_store[key]
            for replica in sa_store:
                broadcast(key, None, 'delete')
                return jsonify({"result": "deleted"}), 200
        # If the key <key> does not exist in the store, then return an error.
        # – Response code is 404 (Not Found).
        # – Response body is JSON {"error": "Key does not exist"}.
        else:
            return jsonify({"error": "Key does not exist"}), 404
    
if __name__=='__main__':
    app.run(host='0.0.0.0', port=8090)
