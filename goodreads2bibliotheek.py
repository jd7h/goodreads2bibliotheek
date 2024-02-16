from urllib.parse import quote as urllib_quote # for escaping weird characters in titles and author names
import pprint

from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz
import pandas as pd
import requests

SIM_THRESHOLD = 75

def cleanup_whitespace(s):
    return " ".join(s.strip().split())

def parse_results(response_text, original_title, original_author):
    soup = BeautifulSoup(response_text, "html.parser")

    matches = []
    books = soup.find_all("div", class_="content list-big")
    for book in books:
        match = {}
        match['search_author'] = original_author
        match['search_title'] = original_title

        # parse minimum required information
        try:
                match['author'] = cleanup_whitespace(book.find("span", class_="creator").text)
                match['title'] = cleanup_whitespace(book.find("span", class_="title").text)
                match['link'] = book.find("a", class_="distinctparts")["href"]
        except Exception as e:
            print(f"Error parsing basic book info.")
            continue

        # parse additional metadata in book widget
        try:
            additional_info_str = "|".join([additional.text.strip() for additional in book.find_all("p", class_="additional")])
            match['additional_info'] = [info.strip() for info in additional_info_str.split("|")]
            match['is_audiobook'] = 'Luisterboek' in match['additional_info']
            match['is_ebook'] = 'E-book' in match['additional_info']
        except Exception as e:
            print(f"Error parsing additional info for {match['author']} - {match['title']}")
            print(e)

        # Fuzzy match author
        match['author_similarity'] = fuzz.partial_ratio(original_author.lower(), match['author'].lower())
        match['title_similarity'] = fuzz.partial_ratio(original_title.lower(), match['title'].lower())
        if match['author_similarity'] < SIM_THRESHOLD:
            continue
        matches.append(match)

    return matches

def check_availability(title, author, work_type='ebook'):
    if work_type not in ['ebook','audiobook']:
        raise ValueError("work_type must be 'ebook' or 'audiobook'")

    print(f"Checking availability of {title} by {author}")

    formatted_title = urllib_quote(title) # escape characters such as ' '

    url = {
        'ebook': f"https://www.onlinebibliotheek.nl/zoekresultaten.catalogus.html?q={formatted_title}&leesvorm=ereader",
        'audiobook': f"https://www.onlinebibliotheek.nl/zoekresultaten.catalogus.html?q={formatted_title}&type=Digitaal_luisterboek"
    }

    try:
        response = requests.get(url[work_type])
        results = parse_results(response.text, title, author)
        return results
    except Exception as e:
        print(f"Error checking {author} - {title}: {e}")
        print(e)
        print(type(e))
        return []

def load_goodreads_data(goodreads_library_export='goodreads_library_export.csv', filter_shelf='to-read', ignore_shelves=None, filter_all=False):
    df = pd.read_csv(goodreads_library_export)

    # clean first
    df = df.assign(title_clean=lambda df: df['Title'].str.split(":").str[0].apply(
        lambda x: pd.Series(x).replace(r"\[.*?\]|\(.*?\)", "", regex=True)[0]
    ).str.strip())

    # then filter
    df_filtered = df.loc[lambda df: df['Exclusive Shelf'] == filter_shelf]
    if filter_all:
        df_filtered = df.loc[lambda df: df['Bookshelves'] == filter_shelf]

    if ignore_shelves:
        for shelfname in ignore_shelves:
            df_filtered = df_filtered.loc[lambda df: ~df['Bookshelves'].str.contains(shelfname, case=False, na=False)]

    return df, df_filtered

def format_results(run_results):
    s = ''
    for row in run_results.to_dict(orient='records'):
        s += f"{row['title']} - {row['author']}\n"
        s += f"{row['link']}\n"
        s += "\n"
    return s

def print_results(run_results):
    print(format_results(run_results))

    # alternative:
    # pprint.pprint(row, sort_dicts=False)

def run(goodreads_library_export='goodreads_library_export.csv', work_type='ebook', max_books=None):
    # load, clean and filter data
    df, df_filtered = load_goodreads_data(goodreads_library_export)

    # max books to check
    if max_books:
        df_filtered = df_filtered.iloc[:max_books]

    # scrape results
    results = df_filtered.apply(lambda row: check_availability(title=row['title_clean'], author=row['Author'], work_type=work_type), axis=1)

    # turn results into neat DataFrame
    df_results = pd.DataFrame([r for bookresult in results.to_list() for r in bookresult])

    print_results(df_results)

    # write formatted results to file
    with open("wishlist_online_bibliotheek.txt", 'w') as outfile:
        outfile.write(format_results(df_results))

    return df_results




# TODO: script errors on search with many hits ("Heen" by Laurens Verhagen)
