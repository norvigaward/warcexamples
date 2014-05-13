package nl.surfsara.warcexamples;

import java.util.Arrays;

import nl.surfsara.warcexamples.hadoop.warc.Hrefs;
import nl.surfsara.warcexamples.hadoop.wat.ServerType;
import nl.surfsara.warcexamples.hadoop.wet.NER;
import nl.surfsara.warcexamples.hdfs.Headers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
	public enum Programs {
		NER("ner", "Perform named entity recognition on wet (extracted text) files."), SERVERTYPE("servertype", "Extract server type from wat (metadata) files."), HREF("href", "Extract links from http responses in warc (full crawl output) files."), HEADERS("headers", "Dumps all headers from a file (this is not a mapreduce job).");

		private final String name;
		private final String description;

		private Programs(String name, String description) {
			this.name = name;
			this.description = description;
		}

		public String getName() {
			return name;
		}

		public String getDescription() {
			return description;
		}
	}

	public static void main(String[] args) {
		int retval = 0;
		boolean showUsage = false;
		if(args.length <= 0) {
			showUsage();
			System.exit(0);
		}
		String tool = args[0];
		String[] toolArgs = Arrays.copyOfRange(args, 1, args.length);
		try {
			if (Programs.NER.getName().equals(tool)) {
				retval = ToolRunner.run(new Configuration(), new NER(), toolArgs);
			} else if (Programs.SERVERTYPE.getName().equals(tool)) {
				retval = ToolRunner.run(new Configuration(), new ServerType(), toolArgs);
			} else if (Programs.HREF.getName().equals(tool)) {
				retval = ToolRunner.run(new Configuration(), new Hrefs(), toolArgs);
			} else if (Programs.HEADERS.getName().equals(tool)) {
				Headers h = new Headers(args[1]);
				h.run();
			}
			if (showUsage) {
				showUsage();
			}
		} catch (Exception e) {
			showErrorAndExit(e);
		}
		System.exit(retval);
	}

	private static void showErrorAndExit(Exception e) {
		System.out.println("Something didn't quite work like expected: [" + e.getMessage() + "]");
		showUsage();
		System.exit(1);
	}

	private static void showUsage() {
		System.out.println("An example program must be given as the first argument.");
		System.out.println("Valid program names are:");
		for (Programs prog : Programs.values()) {
			System.out.println(" " + prog.getName() + ": " + prog.getDescription());
		}
	}
}
