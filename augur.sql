CREATE EXTERNAL TABLE english_1grams (
 gram string,
 year int,
 occurrences bigint,
 pages bigint,
 books bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE
LOCATION 's3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/';

CREATE TABLE stripped_down (
 gram string,
 year int,
 occurrences bigint
);

INSERT OVERWRITE TABLE stripped_down
SELECT
 lower(gram),
 year,
 occurrences
FROM
 english_1grams
WHERE
 year >= 1890 AND
 gram REGEXP "^[A-Za-z+'-]+$";
 
CREATE TABLE by_grp (
 gram string,
 grp int,
 ratio double
); 

INSERT OVERWRITE TABLE by_grp
SELECT
 a.gram,
 b.grp,
 sum(a.occurrences) / b.total
FROM
 stripped_down a
JOIN ( 
 SELECT 
  round(year / 1, 0) as grp, 
  sum(occurrences) as total
 FROM 
  stripped_down
 GROUP BY 
  round(year / 1, 0)
) b
ON
 round(a.year / 1, 0) = b.grp
GROUP BY
 a.gram,
 b.grp,
 b.total;
 
CREATE EXTERNAL TABLE csvexport ( gram STRING, grp STRING, ratio DOUBLE, increase DOUBLE ) row format delimited fields terminated by ',' lines terminated by '\n' STORED AS TEXTFILE LOCATION 's3n://auguremr/out_all/';
INSERT OVERWRITE TABLE csvexport SELECT
 a.gram as gram,
 a.grp as grp,
 a.ratio as ratio,
 a.ratio / b.ratio as increase
FROM 
 by_grp a 
JOIN 
 by_grp b
ON
 a.gram = b.gram and
 a.grp - 1 = b.grp
WHERE
 a.ratio > 0.000001 and
 a.grp >= 190
DISTRIBUTE BY
 grp
SORT BY
 grp ASC,
 increase DESC;
