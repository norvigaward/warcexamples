-- Pig script that generates a ranking of all the top level domains
-- (com, net, nl, etc.) in the web crawl.

-- The script needs the following jars:
-- jwat-common-1.0.0.jar
-- jwat-gzip-1.0.0.jar
-- jwat-warc-1.0.0.jar
-- warcutils.jar

-- The jwat-*.jar's are in HDFS on a shared locations:
REGISTER 'hdfs://head02.hathi.surfsara.nl/jars/jwat-*.jar';

-- You might want to adapt the warcutils PigLoader to expose additional fields
-- from the warc files. If you do this, change the line below to point to your
-- version of warcutils.jar on the VM or on HDFS.
REGISTER 'hdfs://head02.hathi.surfsara.nl/jars/warcutils.jar';
--REGISTER '/home/naward/warcutils.jar';
--REGISTER 'hdfs://head02.hathi.surfsara.nl/user/nawardXX/warcutils.jar';

DEFINE WarcFileLoader nl.surfsara.warcutils.pig.WarcSequenceFileLoader();

-- Load the data, here we use all the SequenceFiles from the TEST set.
in = LOAD '/data/public/common-crawl/crawl-data/CC-TEST-2014-10/*/*/seq/*'
     USING WarcFileLoader AS (url, length, type);
uris = FOREACH in GENERATE LOWER(url) as url;

-- We extract the tld with a regex. This is a bit of a hack, a better approach
-- would use an UDF written in Java or Python.
tlds = FOREACH uris GENERATE
       REGEX_EXTRACT(url, '(.*)://([a-z0-9-]*)\\.([a-z]+)/(.*)', 3) AS tld;

grouped = GROUP tlds BY tld;
result = FOREACH grouped GENERATE group as tld, COUNT(tlds.tld) AS count;
ordered = ORDER result BY count DESC;

STORE ordered INTO 'tld-full-output';
