package nl.surfsara.warcexamples.hadoop.warc;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

class HrefExtracter extends Mapper<LongWritable, WarcRecord, Text, Text> {
	private static enum Counters {
		CURRENT_RECORD, NUM_HTTP_RESPONSE_RECORDS
	}

	public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
		context.setStatus(Counters.CURRENT_RECORD + ": " + key.get());

		// Only process http response content. Note that the outlinks can also be found in the wat metadata.
		if ("application/http; msgtype=response".equals(value.header.contentTypeStr)) {
			context.getCounter(Counters.NUM_HTTP_RESPONSE_RECORDS).increment(1);
			// Get the html payload
			Payload payload = value.getPayload();
			if (payload == null) {
				// NOP
			} else {
				String warcContent = IOUtils.toString(payload.getInputStreamComplete());
				String targetURI = value.header.warcTargetUriStr;
				Document doc = Jsoup.parse(warcContent);

				Elements links = doc.select("a");
				for (Element link : links) {
					String absHref = link.attr("abs:href");
					// Omit nulls and empty strings
					if (absHref != null && !("".equals(absHref))) {
						context.write(new Text(targetURI), new Text(absHref));
					}
				}
			}
		}
	}
}