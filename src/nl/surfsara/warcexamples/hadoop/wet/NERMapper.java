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

class NERMapper extends Mapper<LongWritable, WarcRecord, Text, Text> {
	private static final Logger logger = Logger.getLogger(NERMapper.class);

	private static enum Counters {
		CURRENT_RECORD, NUM_TEXT_RECORDS
	}

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
				String warcContent = IOUtils.toString(payload.getInputStreamComplete());
				try {
					// Load classifier from resource; For proof of concept only one classifier is provided from the Stanford NER package
					@SuppressWarnings("unchecked")
					AbstractSequenceClassifier<CoreLabel> classifier = ((AbstractSequenceClassifier<CoreLabel>) CRFClassifier.getClassifier(NERMapper.class.getResourceAsStream("/nl/surfsara/warcexamples/hadoop/wet/resources/english.all.3classdistsim.crf.ser")));

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
				} catch (Exception e) {
					e.printStackTrace();
					logger.error(e);
				}
			}
		}
	}
}