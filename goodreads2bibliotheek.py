from fuzzywuzzy import fuzz
from bs4 import BeautifulSoup
import pandas as pd
import requests

# Load and prep Goodreads export CSV
df = pd.read_csv("goodreads_library_export.csv")
df_filtered = df[
    df["Bookshelves"].str.contains("to-read", case=False, na=False)
    & ~df["Bookshelves"].str.contains("acquired", case=False, na=False)
    & ~df["Bookshelves"].str.contains("on-ereader", case=False, na=False)
]
df_filtered["Title"] = df_filtered["Title"].str.split(":").str[0]
df_filtered["Title"] = df_filtered["Title"].apply(
    lambda x: pd.Series(x).replace(r"\[.*?\]|\(.*?\)", "", regex=True)[0]
)
df_filtered["Title"] = df_filtered["Title"].str.strip()


def parse_results(response_text, original_title, original_author):
    soup = BeautifulSoup(response_text, "html.parser")

    matches = {"English": [], "Dutch": []}  # Store matches here

    books = soup.find_all("div", class_="content list-big")
    for book in books:
        author = book.find("span", class_="creator").text.strip()
        title = book.find("span", class_="title").text.strip()
        link = book.find("a", class_="distinctparts")["href"]

        # Fuzzy match author
        author_similarity = fuzz.partial_ratio(original_author.lower(), author.lower())

        if author_similarity > 75:  # Adjust threshold as necessary
            title_similarity = fuzz.partial_ratio(original_title.lower(), title.lower())

            # Determine if it's English or Dutch based on title similarity
            language = "Dutch" if title_similarity <= 75 else "English"
            matches[language].append(link)

    # Prepare the return statement to include all relevant matches
    result_lines = []
    if matches["English"] or matches["Dutch"]:
        if matches["English"]:
            for link in matches["English"]:
                result_lines.append(f"English: {link}")
        if matches["Dutch"]:
            for link in matches["Dutch"]:
                result_lines.append(f"Dutch: {link}")
    else:
        result_lines.append("No results found")

    return result_lines


def check_availability(title, author):
    formatted_title = title.replace(" ", "%20")
    url = f"https://www.onlinebibliotheek.nl/zoekresultaten.catalogus.html?q={formatted_title}"

    try:
        response = requests.get(url)
        results = parse_results(response.text, title, author)
        formatted_results = "\n".join(results)
        return f"{title} - {author}:\n{formatted_results}"
    except Exception as e:
        return f"Error checking {author} - {title}: {e}"


# Example loop to check availability (make sure to adjust for your actual data and environment)
for index, row in df_filtered.iterrows():
    print(check_availability(row["Title"], row["Author"]))
print("Done.")

# TODO: script now assumes that Goodreads titles are in English and returns
# "English" for fully matching title, but some Goodreads titles are in Dutch.
# TODO: script errors on search with many hits ("Heen" by Laurens Verhagen)
# TODO: script does not yet distinguish e-books and audiobooks
