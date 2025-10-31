import json
from tempfile import tempdir
from typing import Any

from DB_Connect import DBConnection
from File_Save import save_match, save_timeline

from creds import API_Key
import requests
from datetime import datetime, timezone, timedelta
import time

import sys #for debugging
import os #temp

match_dir = r"C:/Api_Data/match_data/EUW/"
timeline_dir = r"C:/Api_data/timeline_data/EUW/"

current_patch = 15.17
prev_patch = 15.16
acceptable_patch = 15.15

rank_map = {
    "IRON IV": 0, "IRON III": 1, "IRON II": 2, "IRON I": 3,
    "BRONZE IV": 4, "BRONZE III": 5, "BRONZE II": 6, "BRONZE I": 7,
    "SILVER IV": 8, "SILVER III": 9, "SILVER II": 10, "SILVER I": 11,
    "GOLD IV": 12, "GOLD III": 13, "GOLD II": 14, "GOLD I": 15,
    "PLATINUM IV": 16, "PLATINUM III": 17, "PLATINUM II": 18, "PLATINUM I": 19,
    "EMERALD IV": 20, "EMERALD III": 21, "EMERALD II": 22, "EMERALD I": 23,
    "DIAMOND IV": 24, "DIAMOND III": 25, "DIAMOND II": 26, "DIAMOND I": 27,
    "MASTER I": 28, "GRANDMASTER I": 29, "CHALLENGER I": 30
}

int_to_rank = {v: k for k, v in rank_map.items()}

desired_distribution = {"IRON" : 0.1, "BRONZE" : 0.1,
                      "SILVER" : 0.15, "GOLD" : 0.15,
                      "PLATINUM" : 0.1, "EMERALD" : 0.1, "DIAMOND" : 0.1,
                      "MASTER" : 0.066, "GRANDMASTER" : 0.067, "CHALLENGER" : 0.067}

class ApiAccess:
    def __init__(self, db : DBConnection):
        self.db = db
        self.HEADERS = {"X-Riot-Token": API_Key}

    def get_player_matches(self,seed : str):
        print("scraping " + seed)
        match_list = self.api_call("https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/" + seed + "/ids?queue=420&type=ranked&start=0&count=100")
        self.db.set_player_scraped(seed, datetime.now(timezone.utc))
        for match_id in match_list:
            if not self.db.match_saved(match_id):
                if not self.process_match(match_id):
                    break
        self.db.set_scrape_complete(seed)
        rank_needed = self.calculate_needed_rank()
        next_seed = self.db.get_seed(rank_needed)["puuid"]
        self.get_player_matches(next_seed)


    def process_match(self, match_id : str):
        print("Processing Match " + match_id)
        start = time.time()
        self.db.insert_match_id(match_id)
        match_data = self.api_call("https://europe.api.riotgames.com/lol/match/v5/matches/" + match_id)
        if match_data["info"]["queueId"] != 420:
            print("not ranked")
            return False
        # calculates game age and breaks loop if match scraped is > 7 days old (for rank accuracy)
        print("match date " + str(datetime.fromtimestamp(match_data["info"]["gameStartTimestamp"] / 1000)))
        game_start = datetime.fromtimestamp(match_data["info"]["gameStartTimestamp"] / 1000)
        if (datetime.now() - game_start) > timedelta(days=7):
            self.db.remove_match_id(match_id)
            print("Scraped last 7 days")
            return False
        average_rank = int_to_rank[self.get_match_participants(match_data)].split(" ")
        # save to disk
        save_match(match_id, match_data, (match_dir + match_data["info"]["gameVersion"]))
        # insert to database
        self.db.insert_match(match_id, game_start, match_data["info"]["gameDuration"],match_data["info"]["gameVersion"], average_rank[0], average_rank[1])
        #timeline_data = self.api_call("https://europe.api.riotgames.com/lol/match/v5/matches/" + match_id + "/timeline")
        #save_timeline(match_id, timeline_data, timeline_dir, match_data["info"]["gameVersion"])
        #self.db.insert_match_data(match_id, json.dumps(match_data))
        print("saved match " + match_id)
        print("Finished in " + str(round(time.time() - start)) + " seconds")
        return True

    @staticmethod
    def get_average_rank(rank_list : list[str]) -> int:
        total = 0
        for rank in rank_list:
            total += rank_map[rank]
        total = round(total / 10)
        return total

    def get_match_participants(self,match_data : json) -> int:
        rank_list = []
        for participant in match_data["info"]["participants"]:
            player = participant["puuid"]
            self.db.insert_player(player, "EUW")
            self.db.insert_participant(match_data["metadata"]["matchId"],participant)
            # checks player presence in database
            db_player = self.db.check_player_rank(player)
            if db_player:
                time_diff = datetime.now(timezone.utc) - db_player["rank_date"]
                # no recent scrape, rescrape
                if time_diff > timedelta(days=7) or time_diff < timedelta(days=-7):
                    ranks = self.get_player_rank(player)
                else:
                    ranks = {"rank" : db_player["current_rank"], "division" : db_player["current_division"], "lp" : db_player["current_lp"]}
            else:
                #player not in database, insert
                ranks = self.get_player_rank(player)
            if not ranks:
                continue
            rank_list.append(ranks["rank"] + " " + ranks["division"])
        return self.get_average_rank(rank_list)

    def get_player_rank(self, puuid : str) -> dict[str, Any]:
        response = self.api_call(f"https://euw1.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}")
        if not response:
            self.db.remove_player(puuid)
            return None
        if len(response) > 1:
            if response[0]["queueType"] == "RANKED_SOLO_5x5":
                player_details = response[0]
            else:
                player_details = response[1]
        else:
            player_details = response[0]
        rank = player_details["tier"]
        division = player_details["rank"]
        lp = player_details["leaguePoints"]
        self.db.update_rank(puuid, rank, division, lp, datetime.now(timezone.utc))
        return {"rank" : rank, "division" : division, "lp" : lp}

    def calculate_needed_rank(self) -> str:
        ranks = self.db.get_matches_ranks()
        match_count = self.db.get_matches_count()[0]["count"]
        rank_distribution = {}
        # Higher number = More need
        distribution_ratios = {}
        for rank in ranks:
            if not rank["rank"]:
                self.complete_incomplete_matches()
                continue
            # Actual percentage of rank in database
            rank_distribution[rank["rank"]] = (rank["match_count"] / match_count)
            #print(rank["rank"],rank["match_count"],desired_distribution[rank["rank"]], rank_distribution[rank["rank"]])
            # Ratio needed to reach desired distribution
            distribution_ratios[rank["rank"]] = round(desired_distribution[rank["rank"]] / rank_distribution[rank["rank"]],2)
        reverse_dist_ratios = {v: k for k, v in distribution_ratios.items()}
        print("Needed Rank: " + reverse_dist_ratios[max(distribution_ratios.values())])
        return reverse_dist_ratios[max(distribution_ratios.values())]

    def get_rank_composition(self):
        ranks = self.db.get_matches_ranks()
        match_count = self.db.get_matches_count()[0]["count"]
        rank_distribution = {}
        # Higher number = More need
        distribution_ratios = {}
        for rank in ranks:
            if not rank["rank"]:
                self.complete_incomplete_matches()
                continue
            # Actual percentage of rank in database
            rank_distribution[rank["rank"]] = (rank["match_count"] / match_count)
            # print(rank["rank"],rank["match_count"],desired_distribution[rank["rank"]], rank_distribution[rank["rank"]])
            # Ratio needed to reach desired distribution
            distribution_ratios[rank["rank"]] = round(desired_distribution[rank["rank"]] / rank_distribution[rank["rank"]], 2)
        print(rank_distribution)
        print(distribution_ratios)

    def complete_incomplete_matches(self):
        match_list = self.db.get_incomplete_matches()
        for match in match_list:
            self.process_match(match["match_id"])

    def api_call(self, url : str, max_retries = 3) -> json:
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.HEADERS, timeout=10)

                if response.status_code == 200:
                    time.sleep(1.6)
                    return response.json()
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After'), 60)
                    print(f"Rate limited, waiting {retry_after} seconds")
                    time.sleep(retry_after + 120)
                elif response.status_code == 404:
                    return None
                else:
                    print(f"API error {response.status_code}")
                    time.sleep(2 ** attempt)

            except requests.RequestException as e:
                print(f"Request failed: {e}")
                time.sleep(2 ** attempt)
        return None

    # Made specifically for rank_snapshot merge to players
    def update_player_ranks(self):
        players = self.db.query_players()
        for player in players:
            player_rank = self.db.query_rank(player["puuid"])
            if not player_rank:
                continue
            self.db.update_rank(player["puuid"],player_rank["rank"], player_rank["division"], player_rank["lp"], player_rank["snapshot_date"])

    # Made specifically for rank-division split in matches
    def update_match_ranks(self):
        matches = self.db.query_matches()
        for match in matches:
            rank_split = match["rank"].split(" ")
            self.db.update_rank_division(match["match_id"], rank_split[0], rank_split[1])

    # Made to re scrape the players after forgetting to filter out ranked flex
    def rescrape_players(self):
        players = self.db.query_players()
        player_count = len(players)
        count = 1
        for player in players:
            print(f"Player {count} / " + str(player_count))
            self.get_player_rank(player["puuid"])
            count += 1

    # Made to re rank the matches after player ranks were rescraped
    def rerank_matches(self):
        matches = self.db.query_matches()
        for match in matches:
            #print(match["raw_data"]["info"]["queueId"])
            # if match["raw_data"]["info"]["queueId"] == 420:
            #     print("ok")
            # else:
            #     self.db.remove_participants(match["match_id"])
            #     self.db.remove_match_id(match["match_id"])
            #     print("removed match")
            average_rank = int_to_rank[self.get_match_participants(match["raw_data"])].split(" ")
            rank = [match["rank"] , match["division"]]
            if average_rank != rank:
                print("updated rank")
                print(average_rank, rank)
            self.db.update_rank_division(match["match_id"], average_rank[0], average_rank[1])

    def insert_match_data(self):
        matches = self.db.query_matches()
        for match in matches:
            self.db.insert_match_data(match["match_id"], match["raw_data"])
            print("inserted into " + match["match_id"])
            pass

    tempdir = r"C:/Api_Data/match_data/EUW/15.16.706.7476"

    def insert_old_matches(self):
        for file in os.listdir(r"C:/Api_Data/match_data/EUW/15.18.710.2811"):
            filename = os.fsdecode(file).split("_")
            self.db.ins_mat(filename[0] + "_" +  filename[1])
            print(filename[0] + "_" +  filename[1])

    def ins(self):
        matches = self.db.get_mat()
        for match in matches:
            match_id = match["match_id"]
            if not self.db.match_saved(match_id):
                if not self.db.check_match(match_id):
                    continue
                if not self.process_match(match_id):
                    continue

    def insert_timeline(self, match_id : str, patch : str):
        if not self.db.timeline_saved(match_id):
            timeline_data = self.api_call("https://europe.api.riotgames.com/lol/match/v5/matches/" + match_id + "/timeline")
            save_timeline(match_id, timeline_data, timeline_dir, patch)
            self.db.insert_timeline_data(match_id, json.dumps(timeline_data))

    def populate_timeline(self):
        matches = self.db.query_matches()
        for match in matches:
            match_id = match["match_id"]
            patch = match["patch_version"]
            print("inserting " + match_id)
            self.insert_timeline(match_id, patch)