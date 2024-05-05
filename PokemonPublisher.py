import requests
import json
import random
import time
import pika


# Function to fetch a random Pokémon from the PokeAPI
def get_random_pokemon():
    response = requests.get('https://pokeapi.co/api/v2/pokemon?limit=1000')
    if response.status_code == 200:
        pokemon_list = response.json()['results']
        random_pokemon = random.choice(pokemon_list)
        pokemon_data = requests.get(random_pokemon['url']).json()
        name = pokemon_data['name']
        level = random.randint(1, 100)
        types = [t['type']['name'] for t in pokemon_data['types']]
        image_url = pokemon_data['sprites']['front_default']
        return {
            'name': name,
            'level': level,
            'types': types,
            'image_url': image_url
        }
    else:
        print("Failed to fetch Pokemon:", response.status_code)


# Function to send the Pokémon data to RabbitMQ
def send_to_rabbitmq(pokemon):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='pokemon_queue', durable=True)
    channel.basic_publish(exchange='', routing_key='pokemon_queue', body=json.dumps(pokemon))
    print("Sent:", pokemon)
    channel.close()
    connection.close()


# Main loop
while True:
    pokemon = get_random_pokemon()
    send_to_rabbitmq(pokemon)
    time.sleep(5)
    break
