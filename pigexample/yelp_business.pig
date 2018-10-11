REGISTER /usr/hdp/current/pig-client/piggybank.jar
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

yelp_business = LOAD '/samples/yelp/yelp_business/yelp_business.csv' using CSVLoader AS (
    business_id:chararray,
    name:chararray,
    neighborhood:chararray,
    address:chararray,
    city:chararray,
    state:chararray,
    postal_code:int,
    latitude:double,
    longitude:double,
    stars:float,
    review_count:int,
    is_open:boolean,
    categories:chararray);

grouped_business = GROUP yelp_business BY state;
counted_business = FOREACH grouped_business GENERATE group, COUNT(yelp_business);
STORE counted_business INTO '/user/${username}/pig-output' USING PigStorage(',');
