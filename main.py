import json
import time
import calendar
import requests
import pymysql
import collections

# Configuration values for database
db_host = "host"
db_port = 3306
db_user = "user"
db_pass = "password"
db_database = "db_name"

# Constants used in the program
regions = {"NA": 104, "EU": 102}
# Variables: Region ID, Start time for Matches (seconds since epoch), End time for matches (seconds since epoch)
stats_api = "http://euw.lolesports.com/api/gameStatsFantasy.json?tournamentId=%s&dateBegin=%s&dateEnd=%s"
match_api = "http://euw.lolesports.com/api/match/%s.json"  # Variables: Match ID


def score_team_points(victory, barons, dragons, first_blood, towers):
    return (2 * victory +
            2 * barons +
            dragons +
            2 * first_blood +
            towers)


def score_player_points(kills, deaths, assists, creep_score, triple_kills, quadra_kills, penta_kills):
    return (2 * kills -
            0.5 * deaths +
            1.5 * assists +
            0.01 * creep_score +
            2 * triple_kills +
            5 * quadra_kills +
            10 * penta_kills +
            (2 if kills >= 10 or assists >= 10 else 0))

# SQL structure definition
sql_structure = (
    """SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for matches
-- ----------------------------
DROP TABLE IF EXISTS `matches`;
CREATE TABLE `matches` (
  `id` int(11) NOT NULL,
  `team1_id` int(11) NOT NULL,
  `team2_id` int(11) NOT NULL,
  `datetime` datetime NOT NULL,
  `week` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_matches_team1_id` (`team1_id`),
  KEY `fk_matches_team2_id` (`team2_id`),
  CONSTRAINT `fk_matches_team2_id` FOREIGN KEY (`team2_id`) REFERENCES `teams` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_matches_team1_id` FOREIGN KEY (`team1_id`) REFERENCES `teams` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Table structure for player_scores
-- ----------------------------
DROP TABLE IF EXISTS `player_scores`;
CREATE TABLE `player_scores` (
  `player_id` int(11) NOT NULL,
  `match_id` int(11) NOT NULL,
  `kills` int(11) NOT NULL,
  `deaths` int(11) NOT NULL,
  `assists` int(11) NOT NULL,
  `creep_score` int(11) NOT NULL,
  `double_kills` int(11) NOT NULL,
  `triple_kills` int(11) NOT NULL,
  `quadra_kills` int(11) NOT NULL,
  `penta_kills` int(11) NOT NULL,
  `points` float(11,2) NOT NULL,
  PRIMARY KEY (`player_id`,`match_id`),
  KEY `fk_player_scores_match_id` (`match_id`),
  CONSTRAINT `fk_player_scores_match_id` FOREIGN KEY (`match_id`) REFERENCES `matches` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_player_scores_player_id` FOREIGN KEY (`player_id`) REFERENCES `players` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Table structure for players
-- ----------------------------
DROP TABLE IF EXISTS `players`;
CREATE TABLE `players` (
  `id` int(11) NOT NULL,
  `team_id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `role` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_players_team_id` (`team_id`),
  CONSTRAINT `fk_players_team_id` FOREIGN KEY (`team_id`) REFERENCES `teams` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Table structure for team_scores
-- ----------------------------
DROP TABLE IF EXISTS `team_scores`;
CREATE TABLE `team_scores` (
  `team_id` int(11) NOT NULL,
  `match_id` int(11) NOT NULL,
  `side` varchar(255) NOT NULL,
  `victory` int(11) NOT NULL,
  `defeat` int(11) NOT NULL,
  `barons` int(11) NOT NULL,
  `dragons` int(11) NOT NULL,
  `first_blood` int(11) NOT NULL,
  `first_tower` int(11) NOT NULL,
  `first_inhibitor` int(11) NOT NULL,
  `towers_killed` int(11) NOT NULL,
  `points` float(11,2) NOT NULL,
  PRIMARY KEY (`team_id`,`match_id`),
  KEY `fk_team_scores_match_id` (`match_id`),
  CONSTRAINT `fk_team_scores_match_id` FOREIGN KEY (`match_id`) REFERENCES `matches` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_team_scores_team_id` FOREIGN KEY (`team_id`) REFERENCES `teams` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Table structure for teams
-- ----------------------------
DROP TABLE IF EXISTS `teams`;
CREATE TABLE `teams` (
  `id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `code` varchar(255) NOT NULL,
  `region` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;""")

insert_match = (
    """INSERT INTO matches
       (id,team1_id,team2_id,datetime,week)
       VALUES(%s,%s,%s,STR_TO_DATE('%s','%%Y-%%m-%%dT%%H:%%iZ'),%s)""")

insert_team = (
    """INSERT INTO teams
       (id,name,code,region)
       VALUES(%s,'%s','%s','%s')""")

insert_team_score = (
    """INSERT INTO team_scores
       (team_id,match_id,side,victory,defeat,barons,dragons,first_blood,first_tower,first_inhibitor,towers_killed,points)
       VALUES(%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

insert_player = (
    """INSERT INTO players
       (id,team_id,name,role)
       VALUES(%s,%s,'%s','%s')""")

insert_player_score = (
    """INSERT INTO player_scores
       (player_id,match_id,kills,deaths,assists,creep_score,double_kills,triple_kills,quadra_kills,penta_kills,points)
       VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

# Epoch times for splits' start and end dates
season4_summer_starts = {"NA": calendar.timegm(time.strptime("2014-05-23T00:00", "%Y-%m-%dT%H:%M")),
                         "EU": calendar.timegm(time.strptime("2014-05-20T00:00", "%Y-%m-%dT%H:%M"))}
season4_summer_ends = {"NA": calendar.timegm(time.strptime("2014-08-05T00:00", "%Y-%m-%dT%H:%M")),
                       "EU": calendar.timegm(time.strptime("2014-08-02T00:00", "%Y-%m-%dT%H:%M"))}
# DB connection
db_conn = pymysql.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_database, autocommit=True)

# SQL structure creation
cur = db_conn.cursor()
cur.execute(sql_structure)
cur.close()

# Get the main stat JSONs from the API
conns = {"NA": requests.get(stats_api % (regions["NA"], season4_summer_starts["NA"], season4_summer_ends["NA"])),
         "EU": requests.get(stats_api % (regions["EU"], season4_summer_starts["EU"], season4_summer_ends["EU"]))}

# OrderedDicts are necessary for the player to team mappings (they are connected by order, no IDs, bad Rito)
stats = {"NA": json.loads(conns["NA"].text, object_pairs_hook=collections.OrderedDict),
         "EU": json.loads(conns["EU"].text, object_pairs_hook=collections.OrderedDict)}

known_teams = []
known_players = []
cur = db_conn.cursor()
for regionkey, region_stats in stats.items():

    # Adding matches, teams and team stats
    for game in region_stats["teamStats"].values():
        match_data = json.loads(requests.get(match_api % game["matchId"]).text)
        team_blue = match_data["contestants"]["blue"]
        team_red = match_data["contestants"]["red"]

        # Add the teams that are not already known to the DB and the known teams list
        for team in match_data["contestants"].values():
            if team["id"] not in known_teams:
                cur.execute(insert_team % (team["id"],
                                           team["name"].strip(),
                                           team["acronym"],
                                           regionkey))
                known_teams.append(team["id"])

        # Add the match data to the DB
        cur.execute(insert_match % (match_data["matchId"],
                                    team_blue["id"],
                                    team_red["id"],
                                    match_data["dateTime"],
                                    match_data["tournament"]["round"]))

        # Add the team scores to the DB
        for teamkey, team in game.items():
            if "team" in teamkey:  # JSON element is a team point summary
                cur.execute(insert_team_score % (team["teamId"],
                                                 game["matchId"],
                                                 "blue" if team["teamId"] == int(team_blue["id"]) else "red",
                                                 team["matchVictory"],
                                                 team["matchDefeat"],
                                                 team["baronsKilled"],
                                                 team["dragonsKilled"],
                                                 team["firstBlood"],
                                                 team["firstTower"],
                                                 team["firstInhibitor"],
                                                 team["towersKilled"],
                                                 score_team_points(team["matchVictory"],
                                                                   team["baronsKilled"],
                                                                   team["dragonsKilled"],
                                                                   team["firstBlood"],
                                                                   team["towersKilled"])))

    # Adding players and player stats
    for gamekey, game in region_stats["playerStats"].items():
        for playerkey, player in game.items():
            if "player" in playerkey:  # JSON element is a player point summary

                # Add the players that are not already known to the DB and the known players list
                if player["playerId"] not in known_players:
                    teams = []
                    for teamkey, team in region_stats["teamStats"][gamekey].items():
                        if "team" in teamkey:
                            teams.append(team["teamId"])
                    cur.execute(insert_player % (player["playerId"],
                                                 teams[0] if list(game.keys()).index(playerkey) <= 7 else teams[1],
                                                 player["playerName"],
                                                 player["role"]))
                    known_players.append(player["playerId"])

                # Add the player scores to the DB
                cur.execute(insert_player_score % (player["playerId"],
                                                   game["matchId"],
                                                   player["kills"],
                                                   player["deaths"],
                                                   player["assists"],
                                                   player["minionKills"],
                                                   player["doubleKills"],
                                                   player["tripleKills"],
                                                   player["quadraKills"],
                                                   player["pentaKills"],
                                                   score_player_points(player["kills"],
                                                                       player["deaths"],
                                                                       player["assists"],
                                                                       player["minionKills"],
                                                                       player["tripleKills"],
                                                                       player["quadraKills"],
                                                                       player["pentaKills"])))

cur.close()
db_conn.close()