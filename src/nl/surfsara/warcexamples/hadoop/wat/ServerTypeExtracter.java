/**
 * Copyright 2014 SURFsara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.surfsara.warcexamples.hadoop.wat;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

/**
 * Map function that from a WarcRecord (wat) parses the JSON content and extracts the servertype string. The resulting key, values: servertype, 1.  
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
class ServerTypeExtracter extends Mapper<LongWritable, WarcRecord, Text, LongWritable> {
	private static final Logger logger = Logger.getLogger(ServerTypeExtracter.class);

	private static enum Counters {
		CURRENT_RECORD, NUM_JSON_RECORDS
	}

	@Override
	public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
		context.setStatus(Counters.CURRENT_RECORD + ": " + key.get());

		// Only try to parse json content
		if ("application/json".equals(value.header.contentTypeStr)) {
			context.getCounter(Counters.NUM_JSON_RECORDS).increment(1);
			// Get the json payload
			Payload payload = value.getPayload();
			if (payload == null) {
				// NOP
			} else {
				String warcContent = IOUtils.toString(payload.getInputStreamComplete());
				JSONObject json = new JSONObject(warcContent);
				try {
					String server = json.getJSONObject("Envelope").getJSONObject("Payload-Metadata").getJSONObject("HTTP-Response-Metadata").getJSONObject("Headers").getString("Server");
					context.write(new Text(server), new LongWritable(1));
				} catch (JSONException e) {
					// Not the JSON we were looking for.. 
					logger.error(e);
				}
			}
		}
	}
}