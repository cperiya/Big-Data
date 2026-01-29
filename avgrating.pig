-- Step 1: Load raw JSON as text
raw = LOAD '/taggenome/input/raw/ratings.json' USING TextLoader() AS (line:chararray);

-- Step 2: Parse JSON fields using REGEX
parsed = FOREACH raw GENERATE
    (int) REGEX_EXTRACT(line, '"item_id": *([0-9]+)', 1) AS item_id,
    (int) REGEX_EXTRACT(line, '"user_id": *([0-9]+)', 1) AS user_id,
    (int) REGEX_EXTRACT(line, '"rating": *([0-9]+)', 1) AS rating;

-- Step 3: Group records by item_id
grouped = GROUP parsed BY item_id;

-- Step 4: Compute average rating
averages = FOREACH grouped GENERATE
    group AS item_id,
    AVG(parsed.rating) AS avg_rating;

-- Step 5: Store output
STORE averages INTO '/taggenome/output/avg_ratings';
