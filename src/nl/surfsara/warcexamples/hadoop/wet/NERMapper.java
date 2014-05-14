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
package nl.surfsara.warcexamples.hadoop.wet;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;

/**
 * Map function that from a WarcRecord (wet) reads the text/plain content and
 * performs named entityt recognition. The resulting key, values: tag, term.
 * Note that only the first 100 response: text/plain records are processed.
 * Doing this on the full record takes a lot of time. When extending this sample
 * please perform your own filtering of relevant records.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
class NERMapper extends Mapper<LongWritable, WarcRecord, Text, Text> {
	private static final Logger logger = Logger.getLogger(NERMapper.class);
	private static final int MAX_RECORDS = 100; 	// Maximum records to process
	private int numrecords = 0;
	private AbstractSequenceClassifier<CoreLabel> classifier;

	private static enum Counters {
		CURRENT_RECORD, NUM_TEXT_RECORDS
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		numrecords = 0;
		try {
			classifier = ((AbstractSequenceClassifier<CoreLabel>) CRFClassifier.getClassifier(NERMapper.class.getResourceAsStream("/nl/surfsara/warcexamples/hadoop/wet/resources/english.all.3class.distsim.crf.ser")));
		} catch (Exception e) {
			logger.error(e);
		}
	}

	@Override
	public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
		context.setStatus(Counters.CURRENT_RECORD + ": " + key.get());
		// Only process text/plain content
		if ("text/plain".equals(value.header.contentTypeStr)) {
			context.getCounter(Counters.NUM_TEXT_RECORDS).increment(1);
			// Get the text payload
			Payload payload = value.getPayload();
			if (payload == null) {
				// NOP
			} else {
				if (numrecords < MAX_RECORDS) {
					String warcContent = IOUtils.toString(payload.getInputStreamComplete());
					if (warcContent == null && "".equals(warcContent)) {
						// NOP
					} else {
						// Classify text		
						List<List<CoreLabel>> classify = classifier.classify(warcContent);
						for (List<CoreLabel> coreLabels : classify) {
							for (CoreLabel coreLabel : coreLabels) {
								String term = coreLabel.word();
								String tag = coreLabel.get(CoreAnnotations.AnswerAnnotation.class);

								if (!"O".equals(tag)) {

									context.write(new Text(tag), new Text(term));
								}
							}
						}
						numrecords++;
					}
				}
			}
		}
	}
}