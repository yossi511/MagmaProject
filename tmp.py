
from PIL import Image
import io
import os


def image_to_data(image_path):
    # Open the image file in binary mode
    with open(image_path, 'rb') as file:
        # Read the binary data
        image_data = file.read()
    return image_data

# Example usage:
image_path = 'images/pika.jpg'  # Replace 'example_image.jpg' with the path to your JFIF image file
image_data = image_to_data(image_path)
print("Image data:", image_data)

base_directory = ("")
images = "images"
images_directory = os.path.join(base_directory, images)
pokemon_name = 'Pikachu'

image_path = os.path.join(images_directory,
                          pokemon_name + ".jpg")  # Assuming the image arrives in a certain format and can use the Pokémon's name
with open(image_path, "wb") as image_file:
    image_file.write(image_data)



