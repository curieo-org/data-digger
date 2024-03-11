from sentence_transformers import SentenceTransformer
import os
from flask import Flask, request, jsonify
import logging

# Script to expose embeddings computer in a minimalist way
# Run with:
#  python3 -m flask --app scripts/embeddings_server.py run

modelname='all-mpnet-base-v2'
modelpath=os.path.expanduser("~/Documents/code/") + modelname
model = SentenceTransformer(modelpath, device='mps')
logging.info('Model %s loaded'%(modelpath))

app = Flask(__name__)
app.config.from_mapping(
    SECRET_KEY="dev",
)
app.config.from_prefixed_env()

@app.route("/embed", methods = ['POST', 'GET'])
def embed():
    if request.method == 'POST':
        text = request.json.get('text')
        # print("text received %s"%(str(text)))
        encoding = model.encode(text, normalize_embeddings=False)
        return jsonify(isError= False,
                        message= "Success",
                        statusCode= 200,
                        data= encoding.tolist()), 200
    else:
        text = request.args.get('text')
        encoding = model.encode(text, normalize_embeddings=False)
        return jsonify(isError= False,
                        message= "Success",
                        statusCode= 200,
                        data= encoding.tolist()), 200
