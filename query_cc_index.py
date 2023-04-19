import requests
import json

from tqdm.contrib.concurrent import process_map
from time import sleep


URL_INDICES = "https://index.commoncrawl.org/collinfo.json"

params_default = {
    # API here: https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference
    # "from": "20050101"
    # "to" : "30010101"
    # "limit": 20
    # "sort": 'reverse' / 'closest'
    "filter": ["=mime:text/html", "!=status:200"],
    "fl": ["length", "offset", "filename", "languages", "encoding", "timestamp", "url"],
}

params = params_default

params_separator = {
    "filter": "&filter=",
    "fl": ","
}


def retry(max_attempts, wait):
    def decorator(f):
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return f(*args, **kwargs)
                except BaseException as err:
                    errmsg = err
                    sleep(wait*attempts)
                attempts += 1
            print(f.__name__, f"{args} {kwargs}:", errmsg)
        return wrapper
    return decorator


@retry(max_attempts=3, wait=3)
def get_pages(domain, index_url):
    pages_query = f"{index_url}?url={domain}&showNumPages=true&output=json"
    r = requests.get(pages_query)
    if r.text:
        try:
            pages = r.json()
        except:
            print(f"(get_pages) ERROR PARSING:'{r.text}. Query: {pages_query}'")
        # pages = r.json()
        # return pages

    else:
        return []


@retry(max_attempts=3, wait=3)
def get_index_entries(domain, index_url, page_num):
    param_string = ""
    for key, value in params.items():
        param_string += "&"
        if isinstance(value, (list, tuple)):
            value = params_separator[key].join(value)
        param_string += f"{key}={value}"
    # extra params
    index_query = f"{index_url}?url={domain}{param_string}&page={page_num}&output=json"
    r = requests.get(index_query)  # response: jsonlines
    index_entries = []
    for row in r.text.split('\n'):
        if row:
            try:
                j = json.loads(row)
                index_entries.append(j)
            except:
                print(f"(get_index_entries) ERROR PARSING:'{j}'. Query: {index_query}")
    #[json.loads(row) for row in r.text.split('\n') if row]
    return index_entries

def iterate_over_indices(domain_index_info):
    # 1. Get number of index pages for domain
    domain, index_info = domain_index_info
    index_url = index_info['cdx-api']
    # try:
    pages = get_pages(domain, index_url)
    print(f"{index_info['id']} ({pages['pages']} pages)")
    # except BaseException as e:
    #     print("Couldn't get pages!", domain, index_url)
    #     print(e)
    #     return False
    # 2. Query each page of the index individually
    for page_num in range(pages['pages']):
        # try:
        domain_urls = get_index_entries(domain, index_url, page_num)
        # except BaseException as e:
        #     print("Couldn't get URLs!", domain, index_url, page_num)
        #     print(e)
        #     continue
        with open(f"indices/{domain.replace('*', '')}-{index_info['id']}.json", "a") as ofile:
            for row in domain_urls:
                json.dump(row, ofile)
    return True


if __name__ == '__main__':
    # 1. Get monitored domains
    with open("domains.txt", "r") as ifile:
        domains = [row.replace('\n', '') for row in ifile]

    # 2. Get index URLs
    r = requests.get(URL_INDICES)
    assert r.status_code == 200
    indices = r.json()

    # 3. Query each index for each domain
    to_process = [(domain, index_info) for domain in domains for index_info in indices]
    result = process_map(iterate_over_indices, to_process, max_workers=8, chunksize=1)

    for (domain, index_info), res in list(zip(to_process, result)):
        print(domain, index_info['id'], res)

