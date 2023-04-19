import subprocess
import shlex


# 1. Get monitored domains
with open("domains.txt", "r") as ifile:
    domains = [row.replace('\n', '') for row in ifile]

for domain in domains:
    r = {
        "processes": 8,
        # "filter": ["=mime:text/html", "!=status:200"],
        "fl": ",".join(["length", "offset", "filename", "languages", "encoding", "timestamp", "url"]),
        "json": "",
        "directory": "indices",
        "coll": "all",
        "timeout": 60,
        "max-retries": 5,
        "retry-wait": 5,
        "verbose": "",
    }

    cmd = " ".join([f"--{k} {v}" for k,v in r.items()])
    print(["venv\Scripts\python.exe"] + ["cdx_index_client.py", domain] + shlex.split(cmd))
    command = subprocess.call(["venv\Scripts\python.exe"] + ["cdx_index_client.py", domain] + shlex.split(cmd))
