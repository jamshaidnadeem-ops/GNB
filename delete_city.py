import os
import sys
import pymysql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host':     os.environ.get('DB_HOST', '142.93.87.55'),
    'port':     int(os.environ.get('DB_PORT', 18897)),
    'user':     os.environ.get('DB_USER', 'avnadmin'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'database': os.environ.get('DB_NAME', 'defaultdb'),
    'charset':  'utf8mb4',
}

def delete_city_data(city_name):
    """Deletes all leads and progress for a specific city."""
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        print(f"\n--- Deleting data for: {city_name} ---")

        # 1. Delete leads from the main table
        sql_leads = "DELETE FROM car_detailers WHERE City = %s"
        cursor.execute(sql_leads, (city_name,))
        leads_deleted = cursor.rowcount

        # 2. Delete progress records from the progress table
        sql_progress = "DELETE FROM scraper_progress WHERE city = %s"
        cursor.execute(sql_progress, (city_name,))
        progress_deleted = cursor.rowcount

        connection.commit()
        
        print(f"✅ Successfully deleted {leads_deleted} leads.")
        print(f"✅ Reset progress records ({progress_deleted} rows).")
        print(f"{'='*40}\n")

    except Exception as e:
        print(f"❌ Error: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Use city from command line argument: python delete_city.py "New York"
        target_city = " ".join(sys.argv[1:])
        delete_city_data(target_city)
    else:
        # Interactive mode
        city_to_delete = input("Enter the City name to delete all data for: ").strip()
        if city_to_delete:
            confirm = input(f"Are you sure you want to delete ALL data for '{city_to_delete}'? (y/n): ").lower()
            if confirm == 'y':
                delete_city_data(city_to_delete)
            else:
                print("Operation cancelled.")
        else:
            print("No city name entered.")
