import os
import base64
from PIL import Image
from io import BytesIO
import libsql_experimental as libsql
from dotenv import load_dotenv
import glob


def image_to_base64(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode('utf-8')

def convert_16bit_to_8bit_jpeg(img_16bit):
    if img_16bit.mode != 'I;16':
        raise ValueError("The provided image is not a 16-bit grayscale image.")
    img_8bit = img_16bit.point(lambda i: i * (1./256)).convert('L')
    buffer = BytesIO()
    img_8bit.save(buffer, format="JPEG")
    return buffer

def convert_to_jpeg(im):
    with BytesIO() as f:
        im.save(f, format='JPEG')
        return f.getvalue()


def process_compression(img_path, mask_path, cmap_path):
    
    img_buffered = BytesIO()
    mask_buffered = BytesIO()
    cmap_buffered = BytesIO()

    
    img = Image.open(img_path)
    img = img.resize((2048,2048))
    img.save(img_buffered, format="JPEG")
    
    img = Image.open(cmap_path)
    img.save(cmap_buffered, format="JPEG")
    
    img = Image.open(mask_path)
    mask_buffered = convert_16bit_to_8bit_jpeg(img)
    # img.save(mask_buffered, format="JPEG")



    return img_buffered, mask_buffered, cmap_buffered


def process_images(images_folder, cmaps_folder, db_name, table_name, AUTH_TOKEN, DB_URL, use_presplit_ds, compress, max_images=None):
    conn = libsql.connect(db_name, sync_url=DB_URL, auth_token=AUTH_TOKEN)    
    conn.sync()

    
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY,
        image_base64 TEXT,
        mask_base64 TEXT,
        cmap_base64 TEXT
    )
    ''')
    
    cur = conn.cursor()

    if not use_presplit_ds:
        try:
            image_files = sorted([f for f in os.listdir(images_folder) if f.endswith('.jpg')])
            mask_files = sorted([f for f in os.listdir(images_folder) if f.endswith('_mask.png')])
            cmap_files = sorted([f for f in os.listdir(cmaps_folder) if f.endswith('.png') and f.startswith('cmapped_mask_')])
        except FileNotFoundError:
            images_folder = os.path.join('create_ds', images_folder)
            cmaps_folder = os.path.join('create_ds', cmaps_folder)
            image_files = sorted([f for f in os.listdir(images_folder) if f.endswith('.jpg')])
            mask_files = sorted([f for f in os.listdir(images_folder) if f.endswith('_mask.png')])
            cmap_files = sorted([f for f in os.listdir(cmaps_folder) if f.endswith('.png') and f.startswith('cmapped_mask_')])
    else:
        images_folder = os.path.join('create_ds', images_folder)
        image_files = sorted(glob.glob(os.path.join(images_folder, '**/sub_image_*.png')))
        mask_files = sorted(glob.glob(os.path.join(images_folder, '**/sub_mask_*.png')))
        cmap_files = sorted(glob.glob(os.path.join(images_folder, '**/sub_cmapped_mask_*.png'))) 
        
    assert len(image_files) > 0, "No images found in the specified folder."
    assert len(mask_files) > 0, "No masks found in the specified folder."
    assert len(cmap_files) > 0, "No colormaps found in the specified folder."
    
    if not (len(image_files) == len(mask_files) == len(cmap_files)):
        print("Mismatch in the number of image, mask, and colormap files.")
        return

    total_images = len(image_files)
    print(f"Total images found: {total_images}")


    if max_images is not None:
        total_images = min(total_images, max_images)

    print(f"Processing up to {total_images} images")

    insert_sql = f"INSERT INTO {table_name} (image_base64, mask_base64, cmap_base64) VALUES (?, ?, ?)"

    for idx, (img_file, mask_file, cmap_file) in enumerate(zip(image_files, mask_files, cmap_files)):
        if idx >= total_images:
            break
        
        if use_presplit_ds:
            img_id = img_file.split('/')[-1].split('_')[-1].split('.')[0]
            if not img_id in mask_file and not img_id in cmap_file:
                print(f"ID mismatch found: {img_file}, {mask_file}, {cmap_file}")
                continue
        else:
            img_id = img_file.split('.')[0][6:]
            if not (mask_file.startswith(f'sample{img_id}_mask') and cmap_file.startswith(f'cmapped_mask_{img_id}')):
                print(f"ID mismatch found: {img_file}, {mask_file}, {cmap_file}")
                continue
            img_path = os.path.join(images_folder, img_file)
            mask_path = os.path.join(images_folder, mask_file)
            cmap_path = os.path.join(cmaps_folder, cmap_file)
        
        if compress and not use_presplit_ds: # can be adjusted for presplit ds
            img_b, mask_b, cmap_b = process_compression(img_path, mask_path, cmap_path)
            img_base64 = base64.b64encode(img_b.getvalue()).decode('ascii')
            mask_base64 = base64.b64encode(mask_b.getvalue()).decode('ascii')
            cmap_base64 = base64.b64encode(cmap_b.getvalue()).decode('ascii')
        else:
            if use_presplit_ds:
                img_path = img_file
                mask_path = mask_file
                cmap_path = cmap_file
            img_base64 = image_to_base64(img_path)
            mask_base64 = image_to_base64(mask_path)
            cmap_base64 = image_to_base64(cmap_path)
        
        conn.sync()
        cur.execute(insert_sql, (img_base64, mask_base64, cmap_base64))

        print(f"Processed image set {idx + 1}/{total_images}: {img_file}, {mask_file}, {cmap_file}")
    conn.commit()
    
    print("Processing complete.")


load_dotenv()

AUTH_TOKEN = os.getenv('AUTH_TOKEN')
DB_URL = os.getenv('DB_URL')

compress=True
images_folder = "dataset"
cmaps_folder = "whole_cmaps"
db_name = "app.db"
table_name = "images_dataset"
max_images = 200

use_presplit_ds = True

if use_presplit_ds:
    images_folder += "_presplit"
    table_name = 'low_' + table_name
    compress = False

process_images(images_folder, cmaps_folder, db_name, table_name, AUTH_TOKEN, DB_URL, use_presplit_ds, compress, max_images )
