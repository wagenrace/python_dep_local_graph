#%%
import json
import os
import re

import pandas as pd
import requests
from py2neo import Graph
from tqdm import tqdm

from scrap_helpers import get_license

with open("config.json") as f:
    config = json.load(f)

neo4j_url = config.get("neo4jUrl", "bolt://localhost:7687")
user = config.get("user", "neo4j")
pswd = config.get("pswd", "password")
neo4j_import_loc = config["neo4j_import_loc"]
graph = Graph(neo4j_url, auth=(user, pswd))

all_packages = {}
all_start_packages = ["tomni", "neo4j"]
deps_on = []

response = requests.get(
    r"https://hugovk.github.io/top-pypi-packages/top-pypi-packages-30-days.min.json"
)
top5000 = response.json()["rows"]

for top_package in top5000:
    all_start_packages.append(top_package["project"])


def add_package(package_name: str):
    try:
        if not package_name:
            return

        # If the package is already added stop
        if all_packages.get(package_name):
            return
        url = "https://pypi.python.org/pypi/" + str(package_name) + "/json"
        data = requests.get(url).json()

        raw_deps = data["info"].get("requires_dist", [])
        raw_deps = raw_deps if raw_deps else []
        package_size = max([i["size"] for i in data["urls"]])

        # Get license
        license = get_license(data)
        all_packages[package_name] = {
            "license": license,
            "package_size": package_size,
        }

        deps = []
        for raw_dep in raw_deps:
            if "extra == " in raw_dep:
                continue
            dep = re.search("[a-zA-Z0-9\-\_\.]*", raw_dep).group().lower()
            deps.append(dep)

        for dep in set(deps):
            deps_on.append({"package": package_name, "dependsOn": dep})
            add_package(dep)
    except:
        print(f"Error with {package_name}")


for start_package in tqdm(all_start_packages):
    add_package(start_package)

all_packages_pd = pd.DataFrame(all_packages).transpose()
all_packages_pd.index.name = "name"
deps_on_pd = pd.DataFrame(deps_on, index=None)

all_packages_pd.to_csv(os.path.join(neo4j_import_loc, "all_packages.csv"))
deps_on_pd.to_csv(os.path.join(neo4j_import_loc, "all_dependencies.csv"), index=False)

#%% Create packages
response = graph.run(
    """
    LOAD CSV  WITH HEADERS FROM 'file:///all_packages.csv' AS row 
    CREATE (n:package {name: row.name, license: row.license, packageSize: row.package_size})
    """
).data()

#%% Create constrain
response = graph.run(
    """
    CREATE constraint packageName if not exists for (n:package) require n.name is unique;
    """
).data()

#%% Create packages relations
response = graph.run(
    """
    LOAD CSV  WITH HEADERS FROM 'file:///all_dependencies.csv' AS row 
    MATCH (p1:package {name: row.package})
    MATCH (p2:package {name: row.dependsOn})

    CREATE (p1)-[:DEPENDS_ON]->(p2)
    """
).data()
