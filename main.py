import os
import pika
import json
from elasticsearch import Elasticsearch

# Connection to the Elasticsearch index
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Relative path to directory for storing the images
images_directory = "images"


# Creating a document in the Elasticsearch index with a link to the image
# TODO: Add image handling
def save_to_elasticsearch(pokemon, image_path):
    # pokemon['image_path'] = image_path
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


# Perform a search query to retrieve Pokémon
def search_pokemon(query):
    result = es.search(index='pokemon_index',
                       body={"query": {"match": {"name": {"query": query, "fuzziness": "AUTO"}}}})
    return result['hits']['hits']


# Function to create an Elasticsearch index
def create_pokemon_index():
    # Define the index settings and mappings
    index_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "name": {"type": "text"},
                "level": {"type": "integer"},
                "type": {"type": "keyword"},
                "image_url": {"type": "text"}
            }
        }
    }

    # Name of the index
    index_name = "pokemon_index"

    # Check if the index already exists
    if not es.indices.exists(index=index_name):
        # Create the index
        es.indices.create(index=index_name, body=index_settings)
        print(f"Index '{index_name}' created successfully.")
    else:
        print(f"Index '{index_name}' already exists.")


# Main function to execute the search
def main():
    create_pokemon_index()

    while True:
        query = input("Enter a Pokémon name or part of a name: (Enter exit in order to exit)")
        if query.lower() == 'exit':
            break
        pokemon_hits = search_pokemon(query)
        print("Search results:")
        for hit in pokemon_hits:
            print(hit['_source'])

    # Connection to the RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Setting up conditions for receiving messages from RabbitMQ
    channel.queue_declare(queue='pokemon_queue', durable=True)
    channel.basic_consume(queue='pokemon_queue', on_message_callback=callback, auto_ack=True)

    print('Waiting for messages...')
    channel.start_consuming()


if __name__ == "__main__":
    main()
