-- Load CSV
data = LOAD '/taggenome/input/scores/glmer.csv'
       USING PigStorage(',') 
       AS (tag:chararray, item_id:int, score:double);

-- Group by tag_id
grp = GROUP data BY tag;

-- Compute average relevance per tag
avg_per_tag = FOREACH grp GENERATE
                 group AS tag,
                 AVG(data.score) AS avg_score;

STORE avg_per_tag INTO '/taggenome/output/pig/tagavgrelevance'
     USING PigStorage('\t');
