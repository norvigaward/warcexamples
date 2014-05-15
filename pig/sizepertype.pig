-- Pig script that generates the average content length per content type.

-- The script needs the following jars:
-- jwat-common-1.0.0.jar
-- jwat-gzip-1.0.0.jar
-- jwat-warc-1.0.0.jar
-- warcutils.jar

-- The jwat-*.jar's are in HDFS on a shared locations:
REGISTER 'hdfs://head02.hathi.surfsara.nl/jars/jwat-*.jar';

-- You might want to adapt the warcutils PigLoader to expose additional fields
-- from the warc files. If you do this, change the line below to point to your
-- version of warcutils.jar.
REGISTER 'hdfs://head02.hathi.surfsara.nl/jars/warcutils.jar';
--REGISTER '/home/naward/warcutils.jar';
--REGISTER 'hdfs://head02.hathi.surfsara.nl/user/nawardXX/warcutils.jar';

DEFINE WarcFileLoader nl.surfsara.warcutils.pig.WarcSequenceFileLoader();

-- Load the data, here we use all the SequenceFiles from the TEST set.
in = LOAD '/data/public/common-crawl/crawl-data/CC-TEST-2014-10/*/*/seq/*'
     USING WarcFileLoader AS (url, length:int, type);

-- Remove all the records that have no length field.
filtered = FILTER in BY length IS NOT NULL;

-- Group all the records for the same content type together.
grouped = GROUP filtered BY type;

-- For every group we calculate the average
averages = FOREACH grouped GENERATE group AS T, AVG(filtered.length);

-- Write the results to the terminal.
-- Use 'STORE' to write the output to to a folder on HDFS.
DUMP averages;
