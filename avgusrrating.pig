REGISTER /usr/local/pig/contrib/piggybank/java/piggybank.jar;
REGISTER '/usr/local/pig/lib/commons-collections-3.2.2.jar';

--Step 1: Load JSON using JsonLoader
ratings = LOAD '/taggenome/input/raw/ratings.json'
          USING JsonLoader('item_id:chararray, user_id:int, rating:int');

-- Step 2: Group by item_id
grouped = GROUP ratings BY item_id;

-- Step 3: Compute average rating per book
averages = FOREACH grouped GENERATE
              group AS item_id,
              (double) SUM(ratings.rating)/COUNT(ratings.rating) AS avg_rating;
--sorted = ORDER averages BY item_id;
sorted = ORDER averages BY item_id;
-- Step 4: Store averages
STORE sorted INTO '/taggenome/output/pig/avgratingperbook' USING PigStorage('\t');
