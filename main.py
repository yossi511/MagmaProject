import os
import pika
import json
from elasticsearch import Elasticsearch

# Connection to the RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Connection to the Elasticsearch index
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Relative path to directory for storing the images
images_directory = "images"


# Creating a document in the Elasticsearch index with a link to the image
# TODO: Add image handling
def save_to_elasticsearch(pokemon, image_path):
    #pokemon['image_path'] = image_path
    es.index(index='pokemon_index', body=pokemon)


# Function to handle messages from RabbitMQ
def callback(ch, method, properties, body):
    pokemon_data = body.decode('utf-8')  # Converting the message to a Pokémon
    pokemon = parse_pokemon_data(pokemon_data)  # Parsing the Pokémon from the original data
    image_path = save_image(pokemon['name'], pokemon['image_url'])  # Saving the image and getting the path
    save_to_elasticsearch(pokemon, image_path)  # Saving the Pokémon with the image in Elasticsearch
    print("Received and saved:", pokemon)


# Function for parsing the Pokémon data from the message
# For example pokemon_data could be: "Pikachu, 25, Electric, <image-data>".
def parse_pokemon_data(pokemon_data):
    pokemon_object = json.loads(pokemon_data)

    return pokemon_object


# Function for saving the image and returning the path to the image file
# TODO: Add handling to download image from image_url
def save_image(pokemon_name, image_data):
    image_path = os.path.join(images_directory,
                              pokemon_name + ".jpg")  # Assuming the image arrives in a certain format and can use the Pokémon's name
    # with open(image_path, "wb") as image_file:
    #     image_file.write(image_data)
    return image_path


# Setting up conditions for receiving messages from RabbitMQ
channel.queue_declare(queue='pokemon_queue', durable=True)
channel.basic_consume(queue='pokemon_queue', on_message_callback=callback, auto_ack=True)

print('Waiting for messages...')
channel.start_consuming()
