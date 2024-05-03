import os
import pika
from elasticsearch import Elasticsearch

# Connection to the RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Connection to the Elasticsearch index
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Relative path to directory for storing the images
images_directory = "images"


# Creating a document in the Elasticsearch index with a link to the image
def save_to_elasticsearch(pokemon, image_path):
    pokemon['image_path'] = image_path
    es.index(index='pokemon_index', body=pokemon)


# Function to handle messages from RabbitMQ
def callback(ch, method, properties, body):
    pokemon_data = body.decode('utf-8')  # Converting the message to a Pokémon
    pokemon = parse_pokemon_data(pokemon_data)  # Parsing the Pokémon from the original data
    image_path = save_image(pokemon['name'], pokemon['image'])  # Saving the image and getting the path
    save_to_elasticsearch(pokemon, image_path)  # Saving the Pokémon with the image in Elasticsearch
    print("Received and saved:", pokemon)


# Function for parsing the Pokémon data from the message
# For example pokemon_data could be: "Pikachu, 25, Electric, <image-data>".
def parse_pokemon_data(pokemon_data):
    # Splitting the data assuming it's delimited by some character (e.g., comma)
    parsed_data = pokemon_data.split(',')

    # Assuming the order of properties is: name, level, type, image
    name = parsed_data[0].strip()
    level = int(parsed_data[1].strip())
    pokemon_type = parsed_data[2].strip()
    image = parsed_data[3].strip()

    # Creating the Pokémon structure
    pokemon = {
        'name': name,
        'level': level,
        'type': pokemon_type,
        'image': image
    }

    return pokemon


# Function for saving the image and returning the path to the image file
def save_image(pokemon_name, image_data):
    image_path = os.path.join(images_directory,
                              pokemon_name + ".jpg")  # Assuming the image arrives in a certain format and can use the Pokémon's name
    with open(image_path, "wb") as image_file:
        image_file.write(image_data)
    return image_path


# Setting up conditions for receiving messages from RabbitMQ
channel.queue_declare(queue='pokemon_queue')
channel.basic_consume(queue='pokemon_queue', on_message_callback=callback, auto_ack=True)

print('Waiting for messages...')
channel.start_consuming()
