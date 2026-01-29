REGISTER /usr/local/pig/contrib/piggybank/java/piggybank.jar;

data = LOAD '/taggenome/input/clean/tag_count_clean.json'
       USING JsonLoader('item_id:int, tag_id:int, num:int');

grp = GROUP data BY item_id;

top5 = FOREACH grp {
          sorted = ORDER data BY num DESC;
          limited = LIMIT sorted 5;
	  pairs = FOREACH limited GENERATE
            CONCAT(
                '(', 
                CONCAT(
                    (chararray)tag_id,
                    CONCAT(
                        ',', 
                        CONCAT((chararray)num, ')')
                    )
                )
            ) AS pair;

         
	  pair_list = BagToString(pairs, ', ');
          GENERATE group AS item_id, pair_list AS tags;
       };

STORE top5 INTO '/taggenome/output/pig/topntags';
