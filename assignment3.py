import os
import requests
import time
import copy
from flask import Flask, request, Response, jsonify

app = Flask(__name__)
SOCKET_ADDRESS = os.environ.get("SOCKET_ADDRESS")
VIEW = os.environ.get("VIEW")

kv_store = {} # in-memory key-value store using dictionary
sa_store = {} # in-memory store for storing the set of replicas among which the store is replicated
vector_clock = [0,0,0]

recovery_store = {"10.10.0.2:8090" : {},
                  "10.10.0.3:8090" : {},
                  "10.10.0.4:8090" : {}}

# Create a function to increment vector clock based on each replica to ensure the causal consistency
def inc_vector_clock():
    if SOCKET_ADDRESS == "10.10.0.2:8090":
        vector_clock[0] += 1
    elif SOCKET_ADDRESS == "10.10.0.3:8090":
        vector_clock[1] += 1
    elif SOCKET_ADDRESS == "10.10.0.4:8090":
        vector_clock[2] += 1
        
def compare_vector_clock(v):
    if SOCKET_ADDRESS == "10.10.0.2:8090":
        if vector_clock[0] < v[1] or vector_clock[0] < v[2]:
            return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
    elif SOCKET_ADDRESS == "10.10.0.3:8090":
        if vector_clock[1] < v[0] or vector_clock[1] < v[2]:
            return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
    elif SOCKET_ADDRESS == "10.10.0.4:8090":
        if vector_clock[2] < v[0] or vector_clock[2] < v[1]:
            return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503

# Create a function to update vector clock based on each replica to ensure eventual consistency
def update_vector_clock(v):
        vector_clock[0] = max(vector_clock[0], v[0])
        vector_clock[1] = max(vector_clock[1], v[1])
        vector_clock[2] = max(vector_clock[2], v[2])

# Create a function to check if the length of the key <key> is more than 50 characters
def is_key_valid(key):
    return len(key) < 50

# print("Broadcasting self to other replicas")
views = VIEW.split(",")
for view in views:
    if view != SOCKET_ADDRESS:
        # print(f"Sending PUT request to {view}")
        try:
            requests.put(f"http://{view}/view", json={"socket-address": SOCKET_ADDRESS}, timeout=0.2)
        except requests.exceptions.ConnectionError:
            # print("Connection error")
            pass
        else:
            # print("PUT request successful")
            sa_store[view] = True
            r = requests.get(f"http://{view}/kvs", json={"socket-address": SOCKET_ADDRESS})
            kv_store.update(r.json()["recovery_data"])
            update_vector_clock(r.json()["causal-metadata"])
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
            return jsonify({"result": "added"}), 201
            
    if request.method == 'DELETE':
        # Delete an existing replica <socket-address> from the view.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "deleted"}.
        data = request.get_json()
        replica = data['socket-address']
        if replica in sa_store:
            recovery_store[replica] = kv_store
            del sa_store[replica]
            if "broadcasted" in data:
                return jsonify({"result": "deleted"}), 200
            #broadcasts DELETE-view requests to other replicas
            for view in sa_store:
                if view != SOCKET_ADDRESS:
                    url = f"http://{view}/view"
                    try:
                        requests.delete(url, json={"socket-address": replica, "broadcasted": "true"}, timeout=0.2)
                    except requests.exceptions.ConnectionError:
                        pass
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
            # print(data["value"])
            value = data["value"]

        except (TypeError, KeyError):
            # print("Exception caught: Either TypeError or KeyError")
            return jsonify({"error": "PUT request does not specify a value"}), 400

        causal_metadata = data.get('causal-metadata')
        inc_vector_clock()
        
        if causal_metadata:
            compare_vector_clock(causal_metadata)
            update_vector_clock(causal_metadata)
        result = "created" if key not in kv_store else "replaced"
        kv_store[key] = value

        if "broadcasted" in data:
            return jsonify({"result": result, "causal-metadata": vector_clock}), 200 if result == "replaced" else 201
        
        # This variable makes it so we can modify the size of the dictionary while iterating over it
        temp_sa_store = sa_store.copy()

        for replica in temp_sa_store:
            if replica != SOCKET_ADDRESS:
                try:
                    #inc_vector_clock()
                    url = f"http://{replica}/kvs/{key}"
                    method = request.method
                    r = requests.put(url, json={"value": value, "causal-metadata": vector_clock, "broadcasted": "true"}, timeout=0.5)
                    # update vector clock after request is returned
                    if r and (r.status_code == 200 or r.status_code == 201):
                        update_vector_clock(r.json()["causal-metadata"])
                
                except requests.exceptions.ConnectionError:
                    url = f"http://{SOCKET_ADDRESS}/view"
                    requests.delete(url, json={"socket-address": replica})
                    # try again
            
        return jsonify({"result": result, "causal-metadata": vector_clock}), 200 if result == "replaced" else 201
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
        data = request.get_json()
        causal_metadata = data.get('causal-metadata')

        if causal_metadata:
            compare_vector_clock(causal_metadata)
            #compare = [x > y for x, y in zip(causal_metadata, vector_clock)]
            #if any(compare):
            #    return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503

        # If the key <key> exists in the store, then return the mapped value in the response.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "found", "value": "<value>", "causal-metadata": <V'>}
        #    ∗ The <V'> indicates a causal dependency on the PUT of <key>,<value>.
        if key in kv_store:
            return jsonify({"result": "found", "value": kv_store[key], "causal-metadata": vector_clock}), 200
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
        data = request.get_json()
        causal_metadata = data.get('causal-metadata')
        inc_vector_clock()
        if causal_metadata:
            compare_vector_clock(causal_metadata)
            update_vector_clock(causal_metadata)
        # If the key <key> exists in the store, then remove it.
        # – Response code is 200 (Ok).
        # – Response body is JSON {"result": "deleted", "causal-metadata": <V'>}.
        # ∗ The <V'> indicates a causal dependency on <V> and this DELETE
        if key in kv_store:
            del kv_store[key]
            if "broadcasted" in data:
                return jsonify({"result": "deleted", "causal-metadata": vector_clock, "broadcasted": "true"}), 200
            
            # This variable makes it so we can modify the size of the dictionary while iterating over it
            temp_sa_store = sa_store.copy()
            
            for replica in temp_sa_store:
                # broadcast(key, None, 'DELETE')
                try: 
                    url = f"http://{replica}/kvs/{key}"
                    requests.delete(url, json={"value": None, "causal-metadata": vector_clock, "broadcasted": "true"}, timeout=0.5)
                except requests.exceptions.ConnectionError:
                    url = f"http://{SOCKET_ADDRESS}/view"
                    requests.delete(url, json={"socket-address": replica})
                    pass
                    # return jsonify({"error": "Causal dependenies not satisfied; try again later"}), 503
                return jsonify({"result": "deleted", "causal-metadata": vector_clock, "broadcasted": "true"}), 200
        # If the key <key> does not exist in the store, then return an error.
        # – Response code is 404 (Not Found).
        # – Response body is JSON {"error": "Key does not exist"}.
        else:
            return jsonify({"error": "Key does not exist"}), 404
        
        
@app.route('/kvs', methods=['GET'])
def get_key_list():
    # This method returns a list of all keys and values in the store.
    # – Response code is 200 (Ok).
    # – Response body is JSON {"key1": "value1", "key2": "value2", ...}.
    data = request.get_json('socket-address')
    socket_address = str(data)  # Convert data to a string
    
    recovery_kvs = {}
    recovery_kvs = copy.deepcopy(socket_address)
    #recovery_kvs = recovery_store[socket_address]
    return jsonify({"recovery_data": recovery_kvs, "causal-metadata": vector_clock}), 200
    
    
if __name__=='__main__':
    app.run(host='0.0.0.0', port=8090)