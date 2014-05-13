package nl.surfsara.warcexamples.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.PropertyConfigurator;

public class Headers implements Runnable {
	private String path;

	public Headers(String path) {
		this.path = path;	
	}
	
	@Override
	public void run() {
		PropertyConfigurator.configure("log4jconfig.properties");
		final Configuration conf = new Configuration();
		conf.addResource(new Path("core-site.xml"));
		conf.addResource(new Path("hdfs-site.xml"));

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
