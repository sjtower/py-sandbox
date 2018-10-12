import csv
import json
import threading
from Queue import Queue

import requests
import zipcodes

MAX_THREADS = 20

dirty_restaurants = []
threads = []

restaurant_request_queue = Queue()
restaurant_check_queue = Queue()


def restaurant_requester():
    while True:
        request_url = restaurant_request_queue.get()
        if request_url is None:
            break
        get_restaurants(request_url)
        restaurant_request_queue.task_done()


def restaurant_checker():
    while True:
        restaurant = restaurant_check_queue.get()
        if restaurant is None:
            break
        check_restaurant(restaurant)
        restaurant_check_queue.task_done()


class Restaurant:

    def __init__(self, address, rid, latitude, longitude):
        self.address = address
        self.rid = rid
        self.latitude = latitude
        self.longitude = longitude

    address = []
    rid = 0
    latitude = None
    longitude = None
    problem = ""


def get_restaurants(request_url):
    print('requesting restaurant data: {}'.format(request_url))
    response = requests.get(request_url)
    json_data = json.loads(response.text)

    if 'data' in json_data:
        for restaurant in json_data['data']:
            restaurant_check_queue.put(restaurant)
    else:
        'ERROR: No data from request: {}'.format(request_url)
        return


def check_restaurant(restaurant):
    print('checking restaurant data: {}'.format(restaurant['id']))
    restaurant = Restaurant(restaurant['Address'], restaurant['id'], restaurant['GeoLocation']['Latitude'],
                            restaurant['GeoLocation']['Longitude'])

    try:
        zipcode = restaurant.address['PostCode'].encode('UTF-8').strip()
        returned_json = zipcodes.matching(zipcode)
    except:
        zipcode = restaurant.address['PostCode']
        if zipcode is None:
            restaurant.problem = 'no zipcode'
        else:
            restaurant.problem = 'zipcode couldn\'t be parsed: {}'.format(zipcode.encode('UTF-8'))
        dirty_restaurants.append(restaurant)
        return

    if len(returned_json) == 0:
        restaurant.problem = 'could not match zipcode: {}'.format(zipcode.encode('UTF-8'))
        dirty_restaurants.append(restaurant)
        return

    returned_address = returned_json[0]

    check_city(restaurant, returned_address)
    check_state(restaurant, returned_address)
    check_zipcode(restaurant, returned_address)
    # check_latitude(restaurant, returned_address)
    # check_longitude(restaurant, returned_address)


def check_latitude(restaurant, returned_address):
    if restaurant.latitude is None:
        restaurant.problem = 'no latitude'
        dirty_restaurants.append(restaurant)
        return

    returned_latitude = round(returned_address['lat'], 2)
    given_latitude = round(float(restaurant.latitude), 2)
    if given_latitude != returned_latitude:
        restaurant.problem = u'latitude doesn\'t match: {} | {}'.format(given_latitude, returned_latitude)
        dirty_restaurants.append(restaurant)
        return


def check_longitude(restaurant, returned_address):
    if restaurant.longitude is None:
        restaurant.problem = 'no longitude'
        dirty_restaurants.append(restaurant)
        return

    returned_longitude = round(returned_address['long'], 2)
    given_longitude = round(float(restaurant.longitude), 2)
    if given_longitude != returned_longitude:
        restaurant.problem = u'longitude doesn\'t match: {} | {}'.format(given_longitude, returned_longitude)
        dirty_restaurants.append(restaurant)
        return


def check_zipcode(restaurant, returned_address):
    returned_zipcode = returned_address['zip_code']
    given_zipcode = restaurant.address['PostCode'].encode('UTF-8').strip()
    if given_zipcode != returned_zipcode:
        restaurant.problem = u'zipcode doesn\'t match: {} | {}'.format(given_zipcode, returned_zipcode)
        dirty_restaurants.append(restaurant)
        return


def check_city(restaurant, returned_address):
    if 'City' not in restaurant.address or restaurant.address['City'] is None:
        restaurant.problem = 'no city'
        dirty_restaurants.append(restaurant)
        return

    returned_city = returned_address['city'].lower()
    given_city = restaurant.address['City'].lower()
    if returned_city != given_city:
        restaurant.problem = u'city doesn\'t match: {} | {}'.format(given_city, returned_city)
        dirty_restaurants.append(restaurant)


def check_state(restaurant, returned_address):
    if 'State' not in restaurant.address or restaurant.address['State'] is None:
        restaurant.problem = 'no state'
        dirty_restaurants.append(restaurant)
        return

    returned_state = returned_address['state'].lower()
    given_state = restaurant.address['State'].lower()
    if returned_state != given_state:
        restaurant.problem = u'state doesn\'t match: {} | {}'.format(given_state, returned_state)
        dirty_restaurants.append(restaurant)


def main():
    print("Hello. Checking all restaurant for location issues.")

    for i in range(1, 80):
        url = "http://restaurant-sc.otenv.com/v9/restaurants?stateIds[]=1,6,7,12,13,16&pageSize=500&page={}".format(i)
        restaurant_request_queue.put(url)

    for i in range(MAX_THREADS):
        t = threading.Thread(target=restaurant_requester)
        t.start()
        threads.append(t)

    # block until all tasks are done
    restaurant_request_queue.join()

    # stop workers
    for i in range(MAX_THREADS):
        restaurant_request_queue.put(None)
    for t in threads:
        t.join()

    for i in range(MAX_THREADS):
        t = threading.Thread(target=restaurant_checker)
        t.start()
        threads.append(t)

    # block until all tasks are done
    restaurant_check_queue.join()

    # stop workers
    for i in range(MAX_THREADS):
        restaurant_check_queue.put(None)
    for t in threads:
        t.join()

    print('dirty restaurants output to CSV \n')

    dirty_restaurants.sort(key=lambda r: r.rid)

    with open('dirty_restaurants.csv', 'wb') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(['RID'] + ['PROBLEM (given value | received value)'])
        for restaurant in dirty_restaurants:
            try:
                csv_writer.writerow([restaurant.rid] + [restaurant.problem.encode('UTF-8')])
            except:
                csv_writer.writerow([restaurant.rid] + ['restaurant data has unsupported encoding'])


if __name__ == '__main__':
    main()
