import json
import psycopg2
from psycopg2 import sql
from datetime import datetime
import os

# Database connection parameters (hidden from AI)
DB_CONFIG = {
    'host': 'goaled-test.cby80ecou4gk.ap-east-1.rds.amazonaws.com',
    'database': 'goaled',
    'user': 'postgres',
    'password': 'JmSvheVQHRDjvSbWj2sO',
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


def map_json_to_db(school_json):
    """Map JSON fields to database columns"""
    return {
        'school_id': str(school_json.get('school_id', '')),
        'addr1': school_json.get('addr_eng', '') or '',
        'addr2': None,
        'admission_info': school_json.get('admission_info_eng', '') or '',
        'banding': school_json.get('banding', '') or '',
        'city': school_json.get('city_eng', '') or '',
        'committee': school_json.get('school_board_eng'),
        'country': school_json.get('country_eng', '') or '',
        'created': datetime.now(),
        'district': school_json.get('district_eng', '') or '',
        'email': school_json.get('email'),
        'gender': school_json.get('gender_eng', '') or '',
        'icon_url': school_json.get('school_logo_url'),
        'image_url': school_json.get('school_pic_url'),
        'language': school_json.get('language_eng', '') or '',
        'level': school_json.get('level_eng', '') or '',
        'name': school_json.get('name_eng', '') or '',
        'name_chinese': school_json.get('name_cht'),
        'phone_number': school_json.get('phone_number'),
        'principal_name': school_json.get('principal_name_eng'),
        'ranking': '',
        'region': '',
        'religion': school_json.get('religion_eng', '') or '',
        'school_info': school_json.get('school_info_eng', '') or '',
        'specific_info': school_json.get('school_fee', '') or '',
        'type': school_json.get('type_eng', '') or '',
        'updated': datetime.now(),
        'website': school_json.get('website', '') or '',
        'addr1_cht': school_json.get('addr_cht'),
        'addr1_eng': school_json.get('addr_eng'),
        'addr2_cht': None,
        'addr2_eng': None,
        'city_cht': school_json.get('city_cht'),
        'city_eng': school_json.get('city_eng'),
        'country_cht': school_json.get('country_cht'),
        'country_eng': school_json.get('country_eng'),
        'district_cht': school_json.get('district_cht'),
        'district_eng': school_json.get('district_eng'),
        'gender_cht': school_json.get('gender_cht'),
        'gender_eng': school_json.get('gender_eng'),
        'language_cht': school_json.get('language_cht'),
        'language_eng': school_json.get('language_eng'),
        'level_cht': school_json.get('level_cht'),
        'level_eng': school_json.get('level_eng'),
        'name_eng': school_json.get('name_eng'),
        'principal_name_cht': school_json.get('principal_name_cht'),
        'principal_name_eng': school_json.get('principal_name_eng'),
        'region_cht': None,
        'region_eng': None,
        'religion_cht': school_json.get('religion_cht'),
        'religion_eng': school_json.get('religion_eng'),
        'school_info_cht': school_json.get('school_info_cht'),
        'school_info_eng': school_json.get('school_info_eng'),
        'superintendent': school_json.get('school_board_eng'),
        'superintendent_name_cht': school_json.get('school_board_cht'),
        'superintendent_name_eng': school_json.get('school_board_eng'),
        'type_cht': school_json.get('type_cht'),
        'type_eng': school_json.get('type_eng')
    }

def insert_school(conn, school_data):
    """Insert a single school record"""
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO school (
            school_id, addr1, addr2, admission_info, banding, city, committee,
            country, created, district, email, gender, icon_url, image_url,
            language, level, name, name_chinese, phone_number, principal_name,
            ranking, region, religion, school_info, specific_info, type, updated,
            website, addr1_cht, addr1_eng, addr2_cht, addr2_eng, city_cht,
            city_eng, country_cht, country_eng, district_cht, district_eng,
            gender_cht, gender_eng, language_cht, language_eng, level_cht,
            level_eng, name_eng, principal_name_cht, principal_name_eng,
            region_cht, region_eng, religion_cht, religion_eng, school_info_cht,
            school_info_eng, superintendent, superintendent_name_cht,
            superintendent_name_eng, type_cht, type_eng
        ) VALUES (
            %(school_id)s, %(addr1)s, %(addr2)s, %(admission_info)s, %(banding)s,
            %(city)s, %(committee)s, %(country)s, %(created)s, %(district)s,
            %(email)s, %(gender)s, %(icon_url)s, %(image_url)s, %(language)s,
            %(level)s, %(name)s, %(name_chinese)s, %(phone_number)s,
            %(principal_name)s, %(ranking)s, %(region)s, %(religion)s,
            %(school_info)s, %(specific_info)s, %(type)s, %(updated)s,
            %(website)s, %(addr1_cht)s, %(addr1_eng)s, %(addr2_cht)s,
            %(addr2_eng)s, %(city_cht)s, %(city_eng)s, %(country_cht)s,
            %(country_eng)s, %(district_cht)s, %(district_eng)s, %(gender_cht)s,
            %(gender_eng)s, %(language_cht)s, %(language_eng)s, %(level_cht)s,
            %(level_eng)s, %(name_eng)s, %(principal_name_cht)s,
            %(principal_name_eng)s, %(region_cht)s, %(region_eng)s,
            %(religion_cht)s, %(religion_eng)s, %(school_info_cht)s,
            %(school_info_eng)s, %(superintendent)s, %(superintendent_name_cht)s,
            %(superintendent_name_eng)s, %(type_cht)s, %(type_eng)s
        )
        ON CONFLICT (school_id) DO UPDATE SET
            addr1 = EXCLUDED.addr1,
            name = EXCLUDED.name,
            updated = EXCLUDED.updated
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

def verify_missing_records(conn, schools_data):
    """Check which school_ids from JSON are missing in the database"""
    cursor = conn.cursor()
    
    # Get all school_ids from JSON
    json_school_ids = set(str(school.get('school_id', '')) for school in schools_data)
    
    # Get all school_ids from database
    cursor.execute("SELECT school_id FROM school")
    db_school_ids = set(row[0] for row in cursor.fetchall())
    
    # Find missing school_ids
    missing_ids = json_school_ids - db_school_ids
    
    cursor.close()
    
    return missing_ids, len(json_school_ids), len(db_school_ids)

def main():
    """Main execution function"""
    # Load data from JSON file
    schools_data = load_data_from_file()
    
    if not schools_data:
        print("No data to process. Exiting.")
        return
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to database successfully")
        
        success_count = 0
        error_count = 0
        failed_records = []
        
        # Process each school record
        for school_json in schools_data:
            school_data = map_json_to_db(school_json)
            success, error_msg = insert_school(conn, school_data)
            
            if success:
                success_count += 1
                print(f"✓ Inserted school_id: {school_data['school_id']}")
            else:
                error_count += 1
                failed_records.append({
                    'school_id': school_data['school_id'],
                    'name': school_data.get('name', 'N/A'),
                    'error': error_msg
                })
                print(f"✗ Failed school_id: {school_data['school_id']} - {error_msg}")
        
        print(f"\n{'='*70}")
        print(f"Insert complete: {success_count} successful, {error_count} errors")
        print(f"{'='*70}")
        
        # Verify which records are missing
        print("\nVerifying database records...")
        missing_ids, json_count, db_count = verify_missing_records(conn, schools_data)
        
        print(f"\n{'='*70}")
        print(f"VERIFICATION RESULTS:")
        print(f"{'='*70}")
        print(f"Records in JSON file: {json_count}")
        print(f"Records in database:  {db_count}")
        print(f"Missing records:      {len(missing_ids)}")
        print(f"{'='*70}")
        
        if missing_ids:
            print(f"\n⚠ WARNING: {len(missing_ids)} records are missing from the database!")
            print("\nMissing school_ids:")
            
            # Create detailed report of missing records
            for school_json in schools_data:
                school_id = str(school_json.get('school_id', ''))
                if school_id in missing_ids:
                    name = school_json.get('name_eng', 'N/A')
                    print(f"  - school_id: {school_id}, name: {name}")
            
            # Save missing records to a file
            script_dir = os.path.dirname(os.path.abspath(__file__))
            missing_file = os.path.join(script_dir, 'missing_records.json')
            
            missing_records_data = [
                school for school in schools_data 
                if str(school.get('school_id', '')) in missing_ids
            ]
            
            with open(missing_file, 'w', encoding='utf-8') as f:
                json.dump(missing_records_data, f, indent=2, ensure_ascii=False)
            
            print(f"\n✓ Missing records saved to: {missing_file}")
        else:
            print("\n✓ All records successfully imported!")
        
        if failed_records:
            print(f"\n{'='*70}")
            print(f"FAILED INSERTIONS SUMMARY:")
            print(f"{'='*70}")
            for record in failed_records:
                print(f"school_id: {record['school_id']}")
                print(f"  name: {record['name']}")
                print(f"  error: {record['error']}\n")
        
        conn.close()
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()