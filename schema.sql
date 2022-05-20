
CREATE DATABASE nfl_historic;

CREATE TABLE games (
    id                  SERIAL PRIMARY KEY,
    home_team           VARCHAR(100) NOT NULL,
    away_team           VARCHAR(100) NOT NULL,
    season              INT NOT NULL,
    week                VARCHAR(100) NOT NULL,
    reg_season          INT NOT NULL,
    game_date           DATE,   -- local calendar date for when the game started
    start_time          TIME,   -- local timezone for the start of the game, 24h format
    game_duration       INT,    -- length of the game in minutes
    stadium             VARCHAR(100),
    home_score_final    INT NOT NULL,
    home_score_q1       INT NOT NULL,
    home_score_q2       INT NOT NULL,
    home_score_q3       INT NOT NULL,
    home_score_q4       INT NOT NULL,
    home_score_ot       INT,
    away_score_final    INT NOT NULL,
    away_score_q1       INT NOT NULL,
    away_score_q2       INT NOT NULL,
    away_score_q3       INT NOT NULL,
    away_score_q4       INT NOT NULL,
    away_score_ot       INT,
    first_possession    VARCHAR(100) NOT NULL DEFAULT '',   -- team name that started with the ball, ie received the kickoff
    roof                VARCHAR(100) NOT NULL DEFAULT '',   -- outdoors/dome/etc
    surface             VARCHAR(100) NOT NULL DEFAULT '',   -- grass or a truf
    weather             VARCHAR(100) NOT NULL DEFAULT '',   -- not always availalbe
    vegas_line          FLOAT,  -- the spread of the game, negative means the home team was favored
    over_under          FLOAT,  -- total number of expected points, not includeing the over/under, just the float for the expected total points
    home_first_downs    INT,
    home_total_yards    INT,
    home_turnovers      INT,    -- these are turnovers recovered by the other team
    home_penalties      INT,
    home_penalty_yards  INT,
    home_sacks          INT,
    home_time_of_possession INT,    -- seconds the home team had the ball
    away_first_downs    INT,
    away_total_yards    INT,
    away_turnovers      INT,    -- these are turnovers recovered by the other team
    away_penalties      INT,
    away_penalty_yards  INT,
    away_sacks          INT,
    away_time_of_possession INT,    -- seconds the away team had the ball
    last_updated        TIMESTAMP
);
