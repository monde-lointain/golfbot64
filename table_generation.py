from PIL import Image, ImageDraw, ImageFont
from prettytable import PrettyTable

import io

def create_ascii_table(title, field_names, data):

    table = PrettyTable()
    table.title = title
    table.field_names = field_names
    for row in data:
        table.add_row(row)

    table.align = "l"
    table._hrules = None
    table.left_padding_width = 0
    table.right_padding_width = 1
    table.vertical_char = " "
    table.horizontal_char = "-"
    table.junction_char = " "

    return table


def create_image_from_table(table_text):
    # Define font and font size
    font = ImageFont.truetype("Inconsolata/static/Inconsolata-Medium.ttf", 20)
    
    # Getting the size of the table to create the size of the blank image
    box = font.getsize_multiline(table_text)

    # Set the background color. Add 10 to the table width and 32 to the height
    # for margins
    image = Image.new("RGB", (box[0] + 10, box[1] + 15), "#2f3137")

    # Draw the background
    draw = ImageDraw.Draw(image)

    # Insert the text
    draw.text((5, 5), table_text, font=font, fill="#bbbcbf")

    # Save the image to a binary stream and reset the pointer
    image_stream = io.BytesIO()
    image.save(image_stream, format="PNG")
    image_stream.seek(0)
    
    return image_stream


def create_image_table(title, field_names, data):
    table = create_ascii_table(title, field_names, data)

    file = create_image_from_table(table)
    return file
