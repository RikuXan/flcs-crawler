import json
import time
import calendar
import requests
import pymysql

# Configuration values for database
db_host = "host"
db_port = 3306
db_user = "username"
db_pass = "password"
db_database = "database"

# Constants used in the program
tournaments = {"S4SUMMERNA": 104, "S4SUMMEREU": 102}
# Variables: Region ID, Start time for Matches (seconds since epoch), End time for matches (seconds since epoch)
stats_api = "http://euw.lolesports.com/api/gameStatsFantasy.json?tournamentId=%s&dateBegin=%s&dateEnd=%s"
match_api = "http://euw.lolesports.com/api/match/%s.json"  # Variables: Match ID
tournament_api = "http://euw.lolesports.com/api/tournament/%s.json"  # Variables: Tournament ID
team_api = "http://euw.lolesports.com/api/team/%s.json"  # Variable: Team ID


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
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

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
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

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
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

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
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

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
) ENGINE=MYISAM DEFAULT CHARSET=latin1;""")

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

# DB connection
db_conn = pymysql.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_database, autocommit=False)

# SQL structure creation
cur = db_conn.cursor()
cur.execute(sql_structure)

stats = {}
start_times = {}
end_times = {}

for tournamentkey, tournament in tournaments.items():
    tournament_data = json.loads(requests.get(tournament_api % tournament).text)
    # Creating tournament data dictionaries
    start_times[tournamentkey] = calendar.timegm(time.strptime(tournament_data["dateBegin"], "%Y-%m-%dT%H:%MZ"))
    end_times[tournamentkey] = calendar.timegm(time.strptime(tournament_data["dateEnd"], "%Y-%m-%dT%H:%MZ"))
    stats[tournamentkey] = json.loads(requests.get(stats_api % (tournaments[tournamentkey], start_times[tournamentkey], end_times[tournamentkey])).text)

    # Adding tournament teams
    for contestant in tournament_data["contestants"].values():
        cur.execute(insert_team % (contestant["id"],
                                   contestant["name"].strip(),
                                   contestant["acronym"],
                                   tournament_data["name"][:2]))

        # Adding their players
        team_data = json.loads(requests.get(team_api % contestant["id"]).text)
        for player in team_data["roster"].values():
            cur.execute(insert_player % (player["playerId"],
                                         contestant["id"],
                                         player["name"],
                                         player["role"]))

    # Adding matches and team stats
    for teamgamekey, teamgame in stats[tournamentkey]["teamStats"].items():
        match_data = json.loads(requests.get(match_api % teamgame["matchId"]).text)
        team_blue = match_data["contestants"]["blue"]
        team_red = match_data["contestants"]["red"]

        # Add the match data to the DB
        cur.execute(insert_match % (match_data["matchId"],
                                    team_blue["id"],
                                    team_red["id"],
                                    match_data["dateTime"],
                                    match_data["tournament"]["round"]))

        # Add the team scores to the DB
        for teamkey, team in teamgame.items():
            if "team" in teamkey:  # JSON element is a team point summary
                cur.execute(insert_team_score % (team["teamId"],
                                                 teamgame["matchId"],
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

        # Add the player scores to the DB
    for playergamekey, playergame in stats[tournamentkey]["playerStats"].items():
        for playerkey, player in playergame.items():
            if "player" in playerkey:  # JSON element is a player point summary
                cur.execute(insert_player_score % (player["playerId"],
                                                   playergame["matchId"],
                                                   player["kills"],
                                                   player["deaths"],
                                                   player["assists"],
                                                   player["minionKills"],
                                                   player["doubleKills"] - player["tripleKills"],
                                                   player["tripleKills"] - player["quadraKills"],
                                                   player["quadraKills"] - player["pentaKills"],
                                                   player["pentaKills"],
                                                   score_player_points(player["kills"],
                                                                       player["deaths"],
                                                                       player["assists"],
                                                                       player["minionKills"],
                                                                       player["tripleKills"] - player["quadraKills"],
                                                                       player["quadraKills"] - player["pentaKills"],
                                                                       player["pentaKills"])))

db_conn.commit()
cur.close()
db_conn.close()