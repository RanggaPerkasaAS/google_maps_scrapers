import requests
import json
import time
import csv
import datetime
import pytz
import concurrent.futures
import sys

# Set stdout encoding to utf-8
sys.stdout.reconfigure(encoding='utf-8')

api_key = "api_key"
search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json?"
details_url = "https://maps.googleapis.com/maps/api/place/details/json?"

store_types = ["baby and kids"]
areas = [
    "KABUPATEN FAKFAK, PAPUA BARAT",
    "KABUPATEN KAIMANA, PAPUA BARAT",
    "KABUPATEN MANOKWARI, PAPUA BARAT",
    "KABUPATEN MANOKWARI SELATAN, PAPUA BARAT",
    "KABUPATEN MAYBRAT, PAPUA BARAT",
    "KABUPATEN PEGUNUNGAN ARFAK, PAPUA BARAT",
    "KABUPATEN RAJA AMPAT, PAPUA BARAT",
    "KABUPATEN SORONG, PAPUA BARAT",
    "KABUPATEN SORONG SELATAN, PAPUA BARAT",
    "KABUPATEN TAMBRAUW, PAPUA BARAT",
    "KABUPATEN TELUK BINTUNI, PAPUA BARAT",
    "KABUPATEN TELUK WONDAMA, PAPUA BARAT"
]

queries = []
for area in areas:
    for store_type in store_types:
        queries.append(f"{store_type} {area}")

page_limit = 10

def get_district(address):
    parts = address.split(', ')
    district = parts[-3] if len(parts) > 1 else None
    return district

def make_request(query):
    r = requests.get(search_url + 'query=' + query + '&key=' + api_key)
    print("Search Response Status Code:", r.status_code)
    print("Search Response Text:", r.text)
    return r.json()

def process_result(x):
    page_count = 1
    results = []

    while 'next_page_token' in x:
        if page_count >= page_limit:
            break

        y = x['results']
        for place in y:
            place_id = place['place_id']
            r = requests.get(details_url + 'place_id=' + place_id +
                            '&fields=name,type,formatted_phone_number,formatted_address,user_ratings_total,rating,url,geometry,opening_hours,website' +
                            '&key=' + api_key)
            print("Details Response Status Code:", r.status_code)
            print("Details Response Text:", r.text)

            place_details = r.json()
            if 'result' in place_details:
                result = place_details['result']

                location = result.get('geometry', {}).get('location', {})
                latitude = location.get('lat')
                longitude = location.get('lng')
                

                types = [type for type in result.get('types', [])]
                # if types:
                district = get_district(result.get('formatted_address'))
                results.append({
                    'name': result.get('name'),
                    'types': ','.join(types),
                    'phone_number': result.get('formatted_phone_number'),
                    'address': result.get('formatted_address'),
                    'user_ratings_total': result.get('user_ratings_total'),
                    'rating': result.get('rating'),
                    'url': result.get('url'),
                    'latitude': latitude,
                    'longitude': longitude,
                    'website' : result.get('website'),
                    'district': district
                })
        time.sleep(2)
        r = requests.get(search_url + 'pagetoken=' + x['next_page_token'] + '&key=' + api_key)
        print("Search Response Status Code (paginated):", r.status_code)
        print("Search Response Text (paginated):", r.text)

        x = r.json()
        page_count += 1
    return results

jst = pytz.timezone('Asia/Tokyo')
datetime_jst = datetime.datetime.now(jst)
datetime_str = datetime_jst.strftime('%Y%m%d_%H%M%S')

with open(f'Papua-Barat-google-maps-baby-and-kids{datetime_str}.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['name', 'types', 'phone_number', 'address', 'district', 'user_ratings_total', 'rating', 'url', 'latitude', 'longitude', 'website']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(make_request, query) for query in queries]

        for future in concurrent.futures.as_completed(futures):
            x = future.result()
            results = process_result(x)

            for row in results:
                writer.writerow(row)