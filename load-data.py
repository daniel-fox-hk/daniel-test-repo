import json
import psycopg2
from psycopg2 import sql
from datetime import datetime
import os

DB_CONFIG = {
    'host': 'localhost',
    'database': 'goaled',
    'user': 'postgres',
    'password': 'password',
    'port': 5432
}

def load_data_from_file():
    """Load school data from data.json file"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_file_path = os.path.join(script_dir, 'data.json')
    
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"✓ Loaded {len(data)} records from data.json")
            return data
    except FileNotFoundError:
        print(f"Error: data.json not found in {script_dir}")
        print("Please ensure data.json is in the same directory as this script.")
        return []
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in data.json - {e}")
        return []
    except Exception as e:
        print(f"Error reading data.json: {e}")
        return []


def truncate_school_table(conn):
    """Truncate the school table"""
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE TABLE school CASCADE")
        conn.commit()
        print("✓ School table truncated successfully")
        return True
    except Exception as e:
        conn.rollback()
        print(f"✗ Error truncating school table: {e}")
        return False
    finally:
        cursor.close()


def map_json_to_db(school_json):
    """Map JSON fields to database columns"""
    # Clean phone number by removing leading quote if present
    phone = school_json.get('phone_number', '')
    if phone and phone.startswith("'"):
        phone = phone[1:]
    
    return {
        'school_id': school_json.get('gek', ''),  # Map gek to school_id
        'phone_number': phone or None,
        'banding': school_json.get('banding') or None,
        'email': school_json.get('email') or None,
        'principal_name': school_json.get('principal_name_eng') or None,
        'principal_name_cht': school_json.get('principal_name_cht') or None,
        'principal_name_eng': school_json.get('principal_name_eng') or None,
        'superintendent': school_json.get('school_board_eng') or None,
        'superintendent_name_cht': school_json.get('school_board_cht') or None,
        'superintendent_name_eng': school_json.get('school_board_eng') or None,
        'name_eng': school_json.get('name_eng') or None,
        'addr1': school_json.get('addr_eng') or None,
        'addr2': None,
        'city': school_json.get('city_eng') or None,
        'city_cht': school_json.get('city_cht') or None,
        'country': school_json.get('country_eng') or None,
        'country_cht': school_json.get('country_cht') or None,
        'level': school_json.get('level_eng') or None,
        'level_cht': school_json.get('level_cht') or None,
        'level_eng': school_json.get('level_eng') or None,
        'name': school_json.get('name_eng', ''),  # Required field
        'name_chinese': school_json.get('name_cht') or None,
        'ranking': None,
        'region': None,
        'region_cht': None,
        'region_eng': None,
        'type': school_json.get('type_eng') or None,
        'type_cht': school_json.get('type_cht') or None,
        'type_eng': school_json.get('type_eng') or None,
        'addr1_cht': school_json.get('addr_cht') or None,
        'addr1_eng': school_json.get('addr_eng') or None,
        'addr2_cht': None,
        'addr2_eng': None,
        'city_eng': school_json.get('city_eng') or None,
        'country_eng': school_json.get('country_eng') or None,
        'district': school_json.get('district_eng') or None,
        'district_cht': school_json.get('district_cht') or None,
        'district_eng': school_json.get('district_eng') or None,
        'gender': school_json.get('gender_eng') or None,
        'gender_cht': school_json.get('gender_cht') or None,
        'gender_eng': school_json.get('gender_eng') or None,
        'icon_url': school_json.get('school_logo_url') or None,
        'image_url': school_json.get('school_pic_url') or None,
        'language': school_json.get('language_eng') or None,
        'language_cht': school_json.get('language_cht') or None,
        'language_eng': school_json.get('language_eng') or None,
        'religion': school_json.get('religion_eng') or None,
        'religion_cht': school_json.get('religion_cht') or None,
        'religion_eng': school_json.get('religion_eng') or None,
        'admission_info': school_json.get('admission_info_eng') or None,
        'school_info': school_json.get('school_info_eng') or None,
        'school_info_cht': school_json.get('school_info_cht') or None,
        'school_info_eng': school_json.get('school_info_eng') or None,
        'website': school_json.get('website') or None,
        'specific_info': school_json.get('school_fee') or None,
        'created': datetime.now(),
        'updated': datetime.now()
    }


def check_duplicates(schools_data):
    """Check for duplicate gek values in the JSON data"""
    gek_values = [school.get('gek', '') for school in schools_data]
    unique_geks = set(gek_values)
    
    if len(gek_values) != len(unique_geks):
        print(f"\n⚠ WARNING: Found duplicate gek values in JSON!")
        print(f"Total records: {len(gek_values)}")
        print(f"Unique gek values: {len(unique_geks)}")
        print(f"Duplicates: {len(gek_values) - len(unique_geks)}")
        
        # Find and display duplicates
        from collections import Counter
        gek_counts = Counter(gek_values)
        duplicates = {gek: count for gek, count in gek_counts.items() if count > 1}
        
        print(f"\nDuplicate gek values found:")
        for gek, count in sorted(duplicates.items()):
            print(f"  gek: {gek} appears {count} times")
            # Show names of duplicate schools
            duplicate_schools = [s for s in schools_data if s.get('gek', '') == gek]
            for idx, school in enumerate(duplicate_schools, 1):
                print(f"    [{idx}] {school.get('name_eng', 'N/A')}")
        
        return duplicates
    else:
        print(f"✓ No duplicate gek values found")
        return {}


def insert_school(conn, school_data):
    """Insert a single school record"""
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO school (
            created, updated, school_id, phone_number, banding, email,
            principal_name, principal_name_cht, principal_name_eng,
            superintendent, superintendent_name_cht, superintendent_name_eng,
            name_eng, addr1, addr2, city, city_cht, country, country_cht,
            level, level_cht, level_eng, name, name_chinese, ranking,
            region, region_cht, region_eng, type, type_cht, type_eng,
            addr1_cht, addr1_eng, addr2_cht, addr2_eng, city_eng,
            country_eng, district, district_cht, district_eng, gender,
            gender_cht, gender_eng, icon_url, image_url, language,
            language_cht, language_eng, religion, religion_cht,
            religion_eng, admission_info, school_info, school_info_cht,
            school_info_eng, website, specific_info
        ) VALUES (
            %(created)s, %(updated)s, %(school_id)s, %(phone_number)s,
            %(banding)s, %(email)s, %(principal_name)s, %(principal_name_cht)s,
            %(principal_name_eng)s, %(superintendent)s, %(superintendent_name_cht)s,
            %(superintendent_name_eng)s, %(name_eng)s, %(addr1)s, %(addr2)s,
            %(city)s, %(city_cht)s, %(country)s, %(country_cht)s, %(level)s,
            %(level_cht)s, %(level_eng)s, %(name)s, %(name_chinese)s,
            %(ranking)s, %(region)s, %(region_cht)s, %(region_eng)s,
            %(type)s, %(type_cht)s, %(type_eng)s, %(addr1_cht)s,
            %(addr1_eng)s, %(addr2_cht)s, %(addr2_eng)s, %(city_eng)s,
            %(country_eng)s, %(district)s, %(district_cht)s, %(district_eng)s,
            %(gender)s, %(gender_cht)s, %(gender_eng)s, %(icon_url)s,
            %(image_url)s, %(language)s, %(language_cht)s, %(language_eng)s,
            %(religion)s, %(religion_cht)s, %(religion_eng)s,
            %(admission_info)s, %(school_info)s, %(school_info_cht)s,
            %(school_info_eng)s, %(website)s, %(specific_info)s
        )
    """
    
    try:
        cursor.execute(insert_query, school_data)
        conn.commit()
        return True, None
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        cursor.close()


def main():
    """Main execution function"""
    # Load data from JSON file
    schools_data = load_data_from_file()
    
    if not schools_data:
        print("No data to process. Exiting.")
        return
    
    # Check for duplicates before processing
    print("\nChecking for duplicate gek values...")
    duplicates = check_duplicates(schools_data)
    
    if duplicates:
        response = input("\nDuplicates found. Continue anyway? (y/n): ")
        if response.lower() != 'y':
            print("Aborted by user.")
            return
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        print("\nConnected to database successfully")
        
        # Truncate the table
        if not truncate_school_table(conn):
            print("Failed to truncate table. Exiting.")
            conn.close()
            return
        
        success_count = 0
        error_count = 0
        failed_records = []
        
        # Process each school record
        for idx, school_json in enumerate(schools_data, 1):
            school_data = map_json_to_db(school_json)
            
            if not school_data['school_id']:
                error_count += 1
                failed_records.append({
                    'gek': school_data['school_id'],
                    'name': school_data.get('name', 'N/A'),
                    'error': 'Missing gek value'
                })
                print(f"✗ [{idx}/{len(schools_data)}] Skipped: {school_data.get('name', 'N/A')[:50]} - Missing gek value")
                continue
            
            success, error_msg = insert_school(conn, school_data)
            
            if success:
                success_count += 1
                print(f"✓ [{idx}/{len(schools_data)}] Inserted: {school_data.get('name', 'N/A')[:50]} (gek: {school_data['school_id']})")
            else:
                error_count += 1
                failed_records.append({
                    'gek': school_data['school_id'],
                    'name': school_data.get('name', 'N/A'),
                    'error': error_msg
                })
                print(f"✗ [{idx}/{len(schools_data)}] Failed: {school_data.get('name', 'N/A')[:50]} - {error_msg}")
        
        print(f"\n{'='*70}")
        print(f"LOAD COMPLETE")
        print(f"{'='*70}")
        print(f"Total records processed: {len(schools_data)}")
        print(f"Successfully inserted:   {success_count}")
        print(f"Failed insertions:       {error_count}")
        print(f"{'='*70}")
        
        # Verify database count
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM school")
        db_count = cursor.fetchone()[0]
        cursor.close()
        
        print(f"\n✓ Database now contains {db_count} records")
        
        if failed_records:
            print(f"\n{'='*70}")
            print(f"FAILED INSERTIONS SUMMARY:")
            print(f"{'='*70}")
            for record in failed_records:
                print(f"gek: {record['gek']}")
                print(f"  name: {record['name']}")
                print(f"  error: {record['error']}\n")
        
        conn.close()
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()