#!/usr/bin/env python3
"""
Migration script to convert image BLOBs to filesystem files.
Run this script once to migrate existing data.
"""

import os
import pathlib
import MySQLdb.cursors
from app import config, get_image_extension, generate_image_filename, get_image_path

def migrate_images():
    """Migrate existing image BLOBs to filesystem files"""
    
    # Connect to database
    conf = config()["db"].copy()
    conf["charset"] = "utf8mb4"
    conf["cursorclass"] = MySQLdb.cursors.DictCursor
    conf["autocommit"] = True
    db = MySQLdb.connect(**conf)
    cursor = db.cursor()
    
    # Get all posts with image data
    cursor.execute("SELECT id, mime, imgdata FROM posts WHERE imgdata IS NOT NULL AND LENGTH(imgdata) > 100")
    posts = cursor.fetchall()
    
    print(f"Found {len(posts)} posts with image data to migrate")
    
    migrated = 0
    errors = 0
    
    for post in posts:
        post_id = post["id"]
        mime = post["mime"]
        imgdata = post["imgdata"]
        
        # Skip if imgdata looks like a filename (already migrated)
        if isinstance(imgdata, str) and len(imgdata) < 100:
            print(f"Post {post_id}: Already migrated (filename: {imgdata})")
            continue
            
        try:
            # Generate filename
            filename = generate_image_filename(post_id, mime)
            image_path = get_image_path(filename)
            
            # Write image data to file
            with open(image_path, 'wb') as f:
                if isinstance(imgdata, str):
                    f.write(imgdata.encode('latin1'))  # Handle encoding issues
                else:
                    f.write(imgdata)
            
            # Update database record
            cursor.execute("UPDATE posts SET imgdata = %s WHERE id = %s", (filename, post_id))
            
            print(f"Post {post_id}: Migrated to {filename}")
            migrated += 1
            
        except Exception as e:
            print(f"Post {post_id}: Error - {e}")
            errors += 1
    
    print(f"\nMigration complete:")
    print(f"  Migrated: {migrated}")
    print(f"  Errors: {errors}")
    print(f"  Total processed: {len(posts)}")

if __name__ == "__main__":
    migrate_images() 