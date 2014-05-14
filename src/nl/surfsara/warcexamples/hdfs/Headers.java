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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Runnable class that uses an existing Kerberos tgt to read a specified warc,
 * wet or wat file from HDFS and dumps the headers for each WarcRecord.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class Headers implements Runnable {
	private String path;

	public Headers(String path) {
		this.path = path;
	}

	@Override
	public void run() {
		// PropertyConfigurator.configure("log4jconfig.properties");
		final Configuration conf = new Configuration();
		// The core-site.xml and hdfs-site.xml are cluster specific. If you wish to use this on other clusters adapt the files as needed.
		conf.addResource(Headers.class.getResourceAsStream("/nl/surfsara/warcexamples/hdfs/resources/core-site.xml"));
		conf.addResource(Headers.class.getResourceAsStream("/nl/surfsara/warcexamples/hdfs/resources/hdfs-site.xml"));

		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("hadoop.security.authorization", "true");

		System.setProperty("java.security.krb5.realm", "CUA.SURFSARA.NL");
		System.setProperty("java.security.krb5.kdc", "kdc.hathi.surfsara.nl");

		UserGroupInformation.setConfiguration(conf);

		UserGroupInformation loginUser;
		try {
			loginUser = UserGroupInformation.getLoginUser();
			System.out.println("Logged in as: " + loginUser.getUserName());
			PrintHeaders printHeaders = new PrintHeaders(conf, path);
			loginUser.doAs(printHeaders);
		} catch (IOException e) {
			// Just dump the error..
			e.printStackTrace();
		}
	}

}
