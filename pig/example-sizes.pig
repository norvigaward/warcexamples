-- The warcutils, jwat-common, jwat-gzip and jwat-warc jars need to be available for Pig.
-- You can put them in HDFS so you don't need to upload them every time. 
REGISTER hdfs://namenode.hathi.surfsara.nl/user/nawardXX/*.jar;

DEFINE WarcFileLoader nl.surfsara.warcutils.pig.WarcSequenceFileLoader();

-- Load the data, here we use all the SequenceFiles from the TEST set.
in = LOAD '/data/public/common-crawl/crawl-data/CC-TEST-2014-10/*/*/seq/*' USING WarcFileLoader AS (url, length:int, type);

-- Remove all the records that have no length.
filtered = FILTER in BY length IS NOT NULL;

-- Group all the records for the same type together.
grouped = GROUP filtered BY type;

-- For every group we calculate the aver
averages = FOREACH grouped GENERATE group, AVG(filtered.length);

-- Write the results to the terminal. Use 'STORE' to write the output to to a file on HDFS.
DUMP averages;
