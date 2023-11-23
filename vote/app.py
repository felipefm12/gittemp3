import csv
import numpy as np
from flask import Flask, render_template, request, make_response, g
from redis import Redis
import os
import socket
import random
import json
import logging

app = Flask(__name__)

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)

def get_redis():
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5)
    return g.redis

# Lectura de datos y preprocesamiento
ratings = {}
with open('ratings.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        user_id = int(row['userId'])
        rating = row['rating']
        if rating:
            ratings.setdefault(user_id, []).append(float(rating))

# Obtener la longitud máxima de calificaciones
max_length = max(len(r) for r in ratings.values())

# Construir la lista de listas para las calificaciones de cada usuario
ratings_matrix = []
for user_ratings in ratings.values():
    padded_ratings = user_ratings + [0.0] * (max_length - len(user_ratings))
    ratings_matrix.append(padded_ratings)

# Convertir la lista de listas a una matriz NumPy
ratings_matrix = np.array(ratings_matrix)

# Definir la función cosine_similarity_numpy
def cosine_similarity_numpy(vec1, vec2):
    common_indices = np.logical_and(vec1 != 0, vec2 != 0)
    vec1_common = vec1[common_indices]
    vec2_common = vec2[common_indices]

    dot_product = np.dot(vec1_common, vec2_common)
    magnitude_vec1 = np.linalg.norm(vec1_common)
    magnitude_vec2 = np.linalg.norm(vec2_common)

    if magnitude_vec1 == 0 or magnitude_vec2 == 0:
        return 0  # Evitar la división por cero

    return dot_product / (magnitude_vec1 * magnitude_vec2)

# Calcular y almacenar las similitudes entre usuarios en un diccionario
similarities = {}
user_ids = list(ratings.keys())
for i, user_id_1 in enumerate(user_ids):
    for j, user_id_2 in enumerate(user_ids):
        if i < j:  # Evitar cálculos duplicados y simetría (similitud entre i y j es igual a j y i)
            similarity = cosine_similarity_numpy(ratings_matrix[i], ratings_matrix[j])
            similarities[(user_id_1, user_id_2)] = similarity
            similarities[(user_id_2, user_id_1)] = similarity

# Función para obtener vecinos más cercanos utilizando las similitudes precalculadas
def find_nearest_neighbors_precalculated(user_id, ratings_dict, num_neighbors=10):
    user_similarities = {
        other_user_id: similarities.get((user_id, other_user_id), 0.0)
        for other_user_id in ratings_dict.keys() if other_user_id != user_id
    }

    nearest_neighbors_ids = sorted(user_similarities, key=user_similarities.get, reverse=True)[:num_neighbors]
    return nearest_neighbors_ids

@app.route("/", methods=['POST', 'GET'])
def hello():
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    if request.method == 'POST':
        redis = get_redis()

        if 'calculate' in request.form:
            user_id = int(request.form.get('user_id'))
            cosine_neighbors = find_nearest_neighbors_precalculated(user_id, ratings)
            
            neighbors_data = json.dumps({'user_id': user_id, 'neighbors': cosine_neighbors})
            redis.rpush('cosine_neighbors', neighbors_data)
            app.logger.info(neighbors_data)
            if redis.exists('cosine_neighbors'):
                app.logger.info('Data uploaded to Redis successfully')
            else:
                app.logger.error('Failed to upload data to Redis')

    resp = make_response(render_template(
        'index.html',
        option_a=os.getenv('OPTION_A', "Cats"),
        option_b=os.getenv('OPTION_B', "Dogs"),
        hostname=socket.gethostname(),
        similarity=None,
        ratings_data=None,
    ))
    resp.set_cookie('voter_id', voter_id)
    return resp

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)
