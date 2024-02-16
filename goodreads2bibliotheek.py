from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz
import pandas as pd
import requests

SIM_THRESHOLD = 75

def parse_results(response_text, original_title, original_author):
    soup = BeautifulSoup(response_text, "html.parser")

    matches = []
    books = soup.find_all("div", class_="content list-big")
    for book in books:
        match = {}
        match['search_author'] = original_author
        match['search_title'] = original_title

        match['author'] = book.find("span", class_="creator").text.strip()
        match['title'] = book.find("span", class_="title").text.strip()
        match['link'] = book.find("a", class_="distinctparts")["href"]
        additional_info_str = "|".join([additional.text.strip() for additional in book.find_all("p", class_="additional")])
        match['additional_info'] = [info.strip() for info in additional_info_str.split("|")]
        match['is_audiobook'] = 'Luisterboek' in match['additional_info']
        match['is_ebook'] = 'E-book' in match['additional_info']

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

    formatted_title = title.replace(" ", "%20")
    #formatted_title = f"{title} {author}".replace(" ", "%20")

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

def run(goodreads_library_export, work_type='ebook', max_books=None):
    # load, clean and filter data
    df, df_filtered = load_goodreads_data(goodreads_library_export)

    # max books to check
    if max_books:
        df_filtered = df_filtered.iloc[:max_books]

    # scrape results
    results = df_filtered.apply(lambda row: check_availability(title=row['title_clean'], author=row['Author'], work_type=work_type), axis=1)
    return results


# TODO: script errors on search with many hits ("Heen" by Laurens Verhagen)
