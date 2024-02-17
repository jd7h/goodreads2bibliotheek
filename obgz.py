from goodreads2bibliotheek import load_goodreads_data, SIM_THRESHOLD, fuzz, urllib_quote

import datetime
import pprint
import json

import requests
import pandas as pd

def get_book_data(title, author):
    #escaped_title = urllib_quote(title)

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'nl-NL',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://obgz.hostedwise.nl',
        'Pragma': 'no-cache',
        #'Referer': f"https://obgz.hostedwise.nl/wise-apps/catalog/9990/search/iets/{escaped_title}?wf_medium_srt=BOE",
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'WISE_KEY': 'c66e838f-6fa9-4838-99c7-211a7ff42c6e:b39b254c70dec512ad89ef5f0edbc0ced0fe8bddca3cb840bf13465e92f59b03',
        'WISE_SESSION': '95202ecf-918c-4831-8df8-d2d047c90a3d',
        'sec-ch-ua': '"Chromium";v="121", "Not A(Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    request_data = {
        'prt': 'INTERNET',
        'var': 'portal',
        'vestnr': '9990',
        'fmt': 'json',
        'search_in': 'iets',
        'amount': '10',
        'catalog': 'default',
        'event': 'osearch',
        'preset': 'all',
        'offset': '0',
        #'wf_medium_srt': 'BOE', # let op, boeken zijn geen sprinters
        'qs': title,
        'vcgrpf': '0',
        'perspectiveId': '129',
        'backend': 'wise',
        'vcgrpt': '0',
    }

    response = requests.post('https://obgz.hostedwise.nl/cgi-bin/bx.pl', headers=headers, data=request_data)
    return response.json()

def parse_book_data(data, original_title, original_author, branch_name):
    matches = []
    for book_object in data.get('objects'):
        match = {}
        match['search_title'] = original_title
        match['search_author'] = original_author
        match['title'] = ''
        match['author'] = ''

        try:
            author_list = []
            for field in ['auteur','ovrg_aut']:
                if book_object.get('fields').get(field):
                    for person in book_object.get('fields').get(field).get('content'):
                        name_reversed = " ".join(person.get('value').split(",")[::-1]).strip()
                        author_list.append(name_reversed)
            match['author'] = ", ".join(author_list)
            
            title_list = []
            for field in ['titel','subtitle']:
                if book_object.get('fields').get(field):
                    title_list.append(book_object.get('fields').get(field).get('content').get('value'))
            match['title'] = ": ".join(title_list)

            match['book_id'] = book_object.get('fields').get('id').get('content').get('value')
            match['link'] = f"https://obgz.hostedwise.nl/wise-apps/catalog/9990/detail/wise/{match['book_id']}"
        except Exception as e:
            print("Parsing error:", e)
            pprint.pprint(book_object)
            continue
            
        # Fuzzy match author
        match['author_similarity'] = fuzz.partial_ratio(original_author.lower(), match['author'].lower())
        match['title_similarity'] = fuzz.partial_ratio(original_title.lower(), match['title'].lower())
        if match['author_similarity'] < SIM_THRESHOLD:
            continue
        
        # get availability info
        availability_data = get_book_availability(match['book_id'])
        statuses, locations, return_dates = parse_availability(availability_data, branch_name)
        match['locations'] = locations
        match['on_loan'] = statuses.get('ON_LOAN') or 0
        match['available'] = statuses.get('AVAILABLE') or 0
        match['return_dates'] = return_dates
        matches.append(match)

    return matches



def get_book_availability(book_id):
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'nl-NL',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Referer': 'https://obgz.hostedwise.nl/wise-apps/catalog/9990/detail/wise/218065?offset=0&qs=the%20curious%20incident%20of%20the%20dog%20in%20the%20nighttime&search_in=iets&state=search&wf_medium_srt=BOE',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'WISE_KEY': 'c66e838f-6fa9-4838-99c7-211a7ff42c6e:b39b254c70dec512ad89ef5f0edbc0ced0fe8bddca3cb840bf13465e92f59b03',
        'WISE_SESSION': '95202ecf-918c-4831-8df8-d2d047c90a3d',
        'sec-ch-ua': '"Chromium";v="121", "Not A(Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    params = {
        'branchCatGroups': '0',
        'branchId': '9990',
        'clientType': 'I',
    }

    response = requests.get(
        f"https://obgz.hostedwise.nl/restapi/title/{book_id}/iteminformation",
        params=params,
        headers=headers,
    )
    
    return response.json()

def parse_availability(availability_data, branch_name="Mariënburg"):
    relevant_items = [book for book in availability_data if book.get('branchName') == branch_name]
    on_loan = 0
    available = 0
    
    statuses = {}
    locations = []
    return_dates = []
    
    for item in relevant_items:
        if item.get('effectiveStatus') not in statuses:
            statuses[item.get('effectiveStatus')] = 0
        statuses[item.get('effectiveStatus')] += 1
    
        if item.get('effectiveStatus') == 'AVAILABLE':
            location_str = f"{item.get('subLocation')} {item.get('callNumber')}"
            locations.append(location_str)
            
        if item.get('effectiveStatus') == 'ON_LOAN':
            formatted_date = datetime.datetime.fromisoformat(item.get('returnDate')).date().isoformat()
            return_dates.append(formatted_date)

    return_dates.sort()

    return statuses, list(set(locations)), return_dates

def check_catalogue(title, author, branch_name):
    print(f"Checking availability of {title} by {author}")

    book_data = get_book_data(title=title, author=author)
    matches = parse_book_data(book_data, original_title=title, original_author=author, branch_name=branch_name)
    return matches

def format_results(run_results):
    s = ''
    for row in run_results.to_dict(orient='records'):
        s += f"{row['title']} - {row['author']}\n"
        s += f"{row['link']}\n"
        s += f"Aantal aanwezig: {row['available']}\n"
        if row['available'] > 0:
            s += f"Locatie: {', '.join(row['locations'])}\n"
        else:
            s += f"Inleverdata: {', '.join(row['return_dates'])}\n"
        s += "\n"
    return s

def print_results(run_results):
    print(format_results(run_results))

def run(goodreads_library_export='goodreads_library_export.csv', branch_name="Mariënburg", max_books=None):
    # load, clean and filter data
    df, df_filtered = load_goodreads_data(goodreads_library_export)

    # max books to check
    if max_books:
        df_filtered = df_filtered.iloc[:max_books]
        
    results = df_filtered.apply(lambda row: check_catalogue(title=row['title_clean'], author=row['Author'], branch_name=branch_name), axis=1)
    
    # turn results into neat DataFrame
    df_results = pd.DataFrame([r for bookresult in results.to_list() for r in bookresult])
    
    print_results(df_results)

    return df_results