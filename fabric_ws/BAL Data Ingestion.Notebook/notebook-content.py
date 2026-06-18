# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "149b9454-87ac-45e9-bbc2-25b5bf3a7680",
# META       "default_lakehouse_name": "BAL_Lakehouse",
# META       "default_lakehouse_workspace_id": "d3924b68-b19f-47cd-a229-e0de011cfbb5",
# META       "known_lakehouses": [
# META         {
# META           "id": "149b9454-87ac-45e9-bbc2-25b5bf3a7680"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import pandas as pd
import os
from datetime import datetime
from bs4 import BeautifulSoup
import re


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BASE_URL = "https://hosted.dcd.shared.geniussports.com/embednf/BAL/en/standings"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/html,*/*",
    "Referer": "https://bal.nba.com/",
    "Origin": "https://bal.nba.com",
}

def fetch_standings(phase_name: str) -> pd.DataFrame:
    params = {
        "phaseName": phase_name,
        "_ht": "1",
        "_mf": "1",
    }
    r = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()

    payload = r.json()
    inner_html = payload["html"]

    df = pd.read_html(inner_html)[0]
    df["conference"] = phase_name
    df["snapshot_utc"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sahara_df = fetch_standings("Sahara Conference")
kalahari_df = fetch_standings("Kalahari Conference")

standings_all = pd.concat([sahara_df, kalahari_df], ignore_index=True)
standings_all

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(standings_all.columns.tolist())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

standings_all = standings_all.drop(columns = ['Unnamed: 1'])
standings_all

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Pandas -> Spark 
sdf = spark.createDataFrame(standings_all)

#write to a lakehouse table(delta)
table_name = 'silver_bal_standings_snapshot'
(
    sdf.write
    .format("delta")
    .mode("overwrite") #overwrite snapshots after each refresh
    .option("mergeSchema", "true")
    .saveAsTable(table_name) #writes into Lakehouse Table
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **PLAYER STATISTICS**

# CELL ********************

import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/html,*/*",
    "Referer": "https://bal.nba.com/",
    "Origin": "https://bal.nba.com",
}

def normalize_category(cat: str) -> str:
    cat = str(cat).strip().lower()

    mapping = {
        "Average minutes": "MPG",
        "mpg": "MPG",
        "Average points": "PPG",
        "ppg": "PPG",
        "Average assists": "APG",
        "apg": "APG",
        "Average total rebounds": "RPG",
        "rpg": "RPG",
        "Average steals": "STPG",
        "stpg": "STPG",
        "spg": "STPG",
        "Average blocks": "BLKPG",
        "blkpg": "BLKPG",
        "Field goals attempted": "FGA",
        "fga": "FGA",
        "Field goals made": "FGM",
        "fgm": "FGM",
        "Field goal percentage": "FG%",
        "fg%": "FG%",
        "Free throws attempted": "FTA",
        "fta": "FTA",
        "Free throws made": "FTM",
        "ftm": "FTM",
        "Free throw percentage": "FT%",
        "ft%": "FT%",
        "3 points attempted": "3PA",
        "3pa": "3PA",
        "3 points made": "3PM",
        "3pm": "3PM",
        "3 point percentage": "3P%",
        "3p%": "3P%",
    }

    return mapping.get(cat, cat.upper())


def fetch_leaders_one_table(url: str, season_year: int) -> pd.DataFrame:
    if not url:
        return pd.DataFrame(columns=["season_year", "category", "snapshot_utc"])

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    payload = r.json()
    inner_html = payload.get("html", "")
    snapshot_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    if not inner_html:
        return pd.DataFrame(columns=["season_year", "category", "snapshot_utc"])

    soup = BeautifulSoup(inner_html, "html.parser")

    rows = []
    title_candidates = soup.find_all(["h2", "h3", "h4", "div"], string=True)

    for t in title_candidates:
        txt = t.get_text(" ", strip=True)
        if not txt:
            continue

        # only keep likely stat titles
        txt_norm = normalize_category(txt)

        valid_categories = {
            "MPG", "PPG", "APG", "RPG", "STPG", "BLKPG",
            "FGA", "FGM", "FG%", "FTA", "FTM", "FT%",
            "3PA", "3PM", "3P%"
        }

        if txt_norm not in valid_categories:
            continue

        nxt = t.find_next("table")
        if not nxt:
            continue

        df = pd.read_html(str(nxt))[0]
        df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed")]

        df["category"] = txt_norm
        df["season_year"] = season_year
        df["snapshot_utc"] = snapshot_utc
        rows.append(df)

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

    if out.empty:
        return out

    # dedupe repeated same table caused by full title + initials
    non_meta_cols = [c for c in out.columns if c not in ["category", "season_year", "snapshot_utc"]]

    out = (
        out
        .drop_duplicates(subset=non_meta_cols + ["category", "season_year"])
        .reset_index(drop=True)
    )

    return out

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

leaders_long_2026 = fetch_leaders_one_table(
    "https://hosted.dcd.shared.geniussports.com/embednf/BAL/en/leaders?&iurl=https%3A%2F%2Fbal.nba.com%2Fstatistics&_ht=1&_cc=1&_mf=1",
    2026
)

leaders_long_2026.head(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(leaders_long_2026.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

leaders_long_2026["category"].value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

leaders_top9 = leaders_long_2026.copy()

# list of stat columns you care about
stat_cols = [
    "MPG", "PPG", "APG", "RPG", "STPG", "BLKPG",
    "FGA", "FGM", "FG%", "FTA", "FTM", "FT%",
    "3PA", "3PM", "3P%"
]

#ensure stat columns are numeric 
for col in stat_cols:
    if col in leaders_top9.columns:
        leaders_top9[col] = pd.to_numeric(leaders_top9[col], errors = "coerce")

#group players by grain and take max
leaders_top9_wide = (
    leaders_top9
    .groupby(["Player", "Team", "season_year"], as_index=False)
    .agg({
        "snapshot_utc": "max",
        "MPG": "max",
        "PPG": "max",
        "APG": "max",
        "RPG": "max",
        "STPG": "max",
        "BLKPG": "max",
        "FGA": "max",
        "FGM": "max",
        "FG%": "max",
        "FTA": "max",
        "FTM": "max",
        "FT%": "max",
        "3PA": "max",
        "3PM": "max",
        "3P%": "max"
    })
)
leaders_top9_wide.head(43)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#unpivot categories

stat_cols = [
    "MPG", "PPG", "APG", "RPG", "STPG", "BLKPG",
    "FGA", "FGM", "FG%", "FTA", "FTM", "FT%",
    "3PA", "3PM", "3P%"
]


top_performers = leaders_top9_wide.melt(
    id_vars = ["Player", "Team", "season_year", "snapshot_utc"],
    value_vars = stat_cols,
    var_name = "category",
    value_name = "score"


)

top_performers = top_performers.dropna(subset=["score"]).reset_index(drop=True)
top_performers.head(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(top_performers.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Pandas -> Spark 
sdf = spark.createDataFrame(top_performers)

#write to a lakehouse table(delta)
table_name = 'Category_performers_snapshot'
(
    sdf.write
    .format("delta")
    .mode("overwrite") #overwrite snapshots after each refresh
    .option("overwriteSchema", "true")
    .saveAsTable(table_name) #writes into Lakehouse Table
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **LEADERS IN EACH CATEGORY**

# CELL ********************

def fetch_leader_cards(url: str, season_year: int) -> pd.DataFrame:
    if not url:
        return pd.DataFrame(columns=[
            "season_year", "category", "player", "team", "value", "snapshot_utc"
        ])

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    payload = r.json()
    inner_html = payload.get("html", "")

    snapshot_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    if not inner_html:
        return pd.DataFrame(columns=[
            "season_year", "category", "player", "team", "value", "snapshot_utc"
        ])

    soup = BeautifulSoup(inner_html, "html.parser")
    rows = []

    title_candidates = soup.find_all(["h2", "h3", "h4", "div"], string=True)

    for t in title_candidates:
        category = t.get_text(" ", strip=True)
        if not category:
            continue

        # if "Average" not in category and "%" not in category:
        #     continue

        nxt_table = t.find_next("table")
        if not nxt_table:
            continue

        # Try to get the nearest panel/container holding both the title and table
        panel = nxt_table.find_parent("div")
        while panel and t not in panel.descendants:
            panel = panel.find_parent("div")

        if not panel:
            continue

        # Look for top player name/team/value inside the panel BEFORE the table
        pre_table_html = ""
        for child in panel.children:
            if child == nxt_table:
                break
            pre_table_html += str(child)

        card_soup = BeautifulSoup(pre_table_html, "html.parser")

        # first meaningful links often contain the featured player
        link_texts = [a.get_text(" ", strip=True) for a in card_soup.find_all("a")]
        link_texts = [x for x in link_texts if x]

        # visible texts
        texts = list(card_soup.stripped_strings)

        # numeric candidates
        number_candidates = []
        for txt in texts:
            if re.fullmatch(r"\d+(\.\d+)?", txt):
                number_candidates.append(float(txt))

        player = None
        team = None
        value = None

        if link_texts:
            player = link_texts[0]

        if number_candidates:
            value = max(number_candidates)

        # crude team guess: first non-category/non-player/non-number text after player
        for txt in texts:
            if txt == category:
                continue
            if player and txt == player:
                continue
            if re.fullmatch(r"\d+(\.\d+)?", txt):
                continue
            if len(txt) <= 4:   # skip MPG, PPG, etc.
                continue
            team = txt
            break

        if player and value is not None:
            rows.append({
                "season_year": season_year,
                "category": category,
                "player": player,
                "team": team,
                "value": value,
                "snapshot_utc": snapshot_utc
            })

    return pd.DataFrame(rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

leaders_cards = fetch_leader_cards(
    "https://hosted.dcd.shared.geniussports.com/embednf/BAL/en/leaders?&iurl=https%3A%2F%2Fbal.nba.com%2Fstatistics&_ht=1&_cc=1&_mf=1",
    2026  
)

#print(leaders_2025.shape)
leaders_cards.head(40)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

leaders_cards_clean = (
    leaders_cards
   #.sort_values(["season_year", "category", "value"], ascending=[True, True, False])
    .drop_duplicates(subset=["season_year", "player", "value"], keep="first")
    .reset_index(drop=True)
)
leaders_cards_clean.head(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mapping = {
    "Average minutes": "MPG",
    "Average points": "PPG",
    "Average assists": "APG",
    "Average total rebounds": "RPG",
    "Average steals": "STPG",
    "Average blocks": "BLKPG",
    "Field goals attempted": "FGA",
    "Field goals made": "FGM",
    "Field goal percentage": "FG%",
    "Free throws attempted": "FTA",
    "Free throws made": "FTM",
    "Free throw percentage": "FT%",
    "3 Points attempted": "3PA",
    "3 Points made": "3PM",
    "3 Point percentage": "3P%"
}

leaders_cards_clean["stat"] = leaders_cards_clean["category"].map(mapping)
leaders_cards_clean.head(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Pandas -> Spark 
sdf = spark.createDataFrame(leaders_cards_clean)

#write to a lakehouse table(delta)
table_name = 'Category_leaders_snapshot'
(
    sdf.write
    .format("delta")
    .mode("overwrite") #overwrite snapshots after each refresh
    .option("overwriteSchema", "true")
    .saveAsTable(table_name) #writes into Lakehouse Table
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Getting a DIMplayerstable 
#leaders_cards_clean.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
