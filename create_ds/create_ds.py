import os
import base64
from PIL import Image
import libsql_experimental as libsql
from dotenv import load_dotenv

def image_to_base64(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode('utf-8')

def process_images(images_folder, cmaps_folder, db_name, table_name, AUTH_TOKEN, DB_URL, max_images=None):

    conn = libsql.connect(db_name, sync_url=DB_URL, auth_token=AUTH_TOKEN)

    conn.sync()
    
    cursor = conn.cursor()

    
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY,
        image_base64 TEXT,
        mask_base64 TEXT,
        cmap_base64 TEXT
    )
    ''')

    # Gather image file pairs
    image_files = sorted([f for f in os.listdir(images_folder) if f.endswith('.jpg')])
    mask_files = sorted([f for f in os.listdir(images_folder) if f.endswith('_mask.png')])
    cmap_files = sorted([f for f in os.listdir(cmaps_folder) if f.endswith('.png') and f.startswith('cmapped_mask_')])

    if not (len(image_files) == len(mask_files) == len(cmap_files)):
        print("Mismatch in the number of image, mask, and colormap files.")
        return

    total_images = len(image_files)
    print(f"Total images found: {total_images}")


    if max_images is not None:
        total_images = min(total_images, max_images)

    print(f"Processing up to {total_images} images")

    for idx, (img_file, mask_file, cmap_file) in enumerate(zip(image_files, mask_files, cmap_files)):
        if idx >= total_images:
            break

        img_id = img_file.split('.')[0][6:]


        if not (mask_file.startswith(f'sample{img_id}_mask') and cmap_file.startswith(f'cmapped_mask_{img_id}')):
            print(f"ID mismatch found: {img_file}, {mask_file}, {cmap_file}")
            continue


        img_path = os.path.join(images_folder, img_file)
        mask_path = os.path.join(images_folder, mask_file)
        cmap_path = os.path.join(cmaps_folder, cmap_file)


        img_base64 = image_to_base64(img_path)
        mask_base64 = image_to_base64(mask_path)
        cmap_base64 = image_to_base64(cmap_path)

        cursor.execute(f'''
        INSERT INTO {table_name} (image_base64, mask_base64, cmap_base64)
        VALUES (?, ?, ?)
        ''', (img_base64, mask_base64, cmap_base64))

        print(f"Processed image set {idx + 1}/{total_images}: {img_file}, {mask_file}, {cmap_file}")

    conn.commit()
    conn.close()
    
    print("Processing complete.")


load_dotenv()

AUTH_TOKEN = os.getenv('AUTH_TOKEN')
DB_URL = os.getenv('DB_URL')


images_folder = "create_ds/dataset"
cmaps_folder = "create_ds/whole_cmaps"
db_name = "app.db"
table_name = "images_dataset"
max_images = 15

process_images(images_folder, cmaps_folder, db_name, table_name, AUTH_TOKEN, DB_URL, max_images )
