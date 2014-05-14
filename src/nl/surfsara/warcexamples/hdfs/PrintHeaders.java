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
package nl.surfsara.warcexamples.hdfs;

import java.security.PrivilegedAction;
import java.util.List;

import nl.surfsara.warcutils.WarcIOConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcHeader;
import org.jwat.warc.WarcReaderCompressed;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

/**
 * Accessing HDFS needs to be performed with privileges for a principal (user)
 * enabled. This is an implementation of a PriviligedAction that, as the logged
 * in user, parses a warc file and dumps the headers of each record to standard
 * out.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class PrintHeaders implements PrivilegedAction<Object> {
	private Configuration conf;
	private String path;

	public PrintHeaders(Configuration conf, String path) {
		this.conf = conf;
		this.path = path;
	}

	@Override
	public Object run() {
		try {
			// You could modify this to read the sequence files. Something along these lines:
			// Option optPath = SequenceFile.Reader.file(new Path(path));
			// SequenceFile.Reader r = new SequenceFile.Reader(conf, optPath);
			//
			// LongWritable key = new LongWritable();
			// Text val = new Text();
			//
			// while (r.next(key, val)) {
			//     InputStream in = new ByteArrayInputStream(val.getBytes());
			//     WarcReaderUncompressed reader = WarcReaderFactory.getReaderUncompressed(in);
			//	   ...
			// }
			// ....
			// r.close();

			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path(path));

			WarcReaderCompressed reader = WarcReaderFactory.getReaderCompressed(in);
			reader.setBlockDigestEnabled(WarcIOConstants.BLOCKDIGESTENABLED);
			reader.setPayloadDigestEnabled(WarcIOConstants.PAYLOADDIGESTENABLED);
			reader.setPayloadHeaderMaxSize(WarcIOConstants.PAYLOADHEADERMAXSIZE);
			reader.setRecordHeaderMaxSize(WarcIOConstants.HEADERMAXSIZE);
			WarcRecord warcRecord;
			while ((warcRecord = reader.getNextRecord()) != null) {
				System.out.println("---- begin headers ---");
				WarcHeader header = warcRecord.header;
				List<HeaderLine> headerList = header.getHeaderList();
				for (HeaderLine hl : headerList) {
					System.out.println(hl.name + " <--> " + hl.value);
				}
				System.out.println("---- end headers ---");
			}

		} catch (Exception e) {
			// Just dump the error..
			e.printStackTrace();
		}
		return null;
	}
}
