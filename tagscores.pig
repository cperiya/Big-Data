glmer = LOAD '/taggenome/input/scores/glmer.csv'
    USING PigStorage(',')
    AS (tag:chararray, item_id:int, score:double);

grp = GROUP glmer BY tag;

avgscore = FOREACH grp GENERATE
    group AS tag,
    AVG(glmer.score) AS avg_score;

STORE avgscore INTO '/taggenome/output/pig/pig_tagscores';
