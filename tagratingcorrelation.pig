REGISTER '/usr/local/pig/lib/commons-collections-3.2.2.jar';
REGISTER /usr/local/pig/contrib/piggybank/java/piggybank.jar;

-- ===============================
-- 1. Load Ratings (JSON)
-- ===============================
ratings_raw = LOAD '/taggenome/input/raw/ratings.json'
    USING JsonLoader(
        'item_id:int, user_id:int, rating:int'
    );

-- Compute average rating and rating count per book
ratings_grp = GROUP ratings_raw BY item_id;

ratings_stats = FOREACH ratings_grp GENERATE
      group AS item_id,
      AVG(ratings_raw.rating) AS avg_rating,
      COUNT(ratings_raw) AS rating_count;

-- ===============================
-- 2. Load Tag Genome Relevance Scores (CSV)
-- ===============================
tags = LOAD '/taggenome/input/scores/glmer.csv'
      USING PigStorage(',')
      AS (tag:chararray, item_id:int, score:double);

-- Compute average tag relevance & total tags per book
tags_grp = GROUP tags BY item_id;

tags_stats = FOREACH tags_grp GENERATE
      group AS item_id,
      AVG(tags.score) AS avg_tag_relevance,
      COUNT(tags) AS total_tags;

-- ===============================
-- 3. Join Ratings + Tag Data
-- ===============================
joined = JOIN ratings_stats BY item_id
              LEFT OUTER, 
              tags_stats BY item_id;

-- ===============================
-- 4. Final projection for correlation analysis
-- ===============================
correlation_input = FOREACH joined GENERATE
      ratings_stats::item_id AS item_id,
      avg_rating,
      rating_count,
      avg_tag_relevance,
      total_tags;

-- ===============================
-- 5. Store Output
-- ===============================
STORE correlation_input INTO '/taggenome/output/pig/tagratingcorrelation'
     USING PigStorage('\t');
