-- Load the Tag Genome score file
data = LOAD '/taggenome/input/scores/glmer.csv'
       USING PigStorage(',')
       AS (tag:chararray, item_id:int, score:double);

-- Group all tag scores by book (item_id)
grp = GROUP data BY item_id;

-- Compute the average relevance score per book
avg_per_book = FOREACH grp GENERATE
                  group AS item_id,
                  AVG(data.score) AS avg_score;

-- Store the results in HDFS
STORE avg_per_book INTO '/taggenome/output/pig/avgtagrelperbook'
     USING PigStorage('\t');
