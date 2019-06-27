package com.trigger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.event.S3EventNotification;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * AWS Lambda function with S3 trigger -> script runner -> S3 writer
 */
public class Handler implements RequestHandler<S3EventNotification, String> {
	
	static final Logger log = LoggerFactory.getLogger(Handler.class);

	@Override
	public String handleRequest(S3EventNotification s3EventNotification, Context context) {
		String jsonEvent = s3EventNotification.toJson();

		log.info("Going to write into S3 bucket");
//		putIntoS3(jsonEvent);
		log.info("Lambda function is invoked:" + jsonEvent);
		try {
			shellTest();
			runshFile();
			readFile();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void putIntoS3(String event) {
		AmazonS3 client = new AmazonS3Client();
		client.putObject("testoutputbucket2209", "JavaOutput/testOutput1", event);
		log.info("Write complete");
	}

	private static void shellTest() throws IOException, InterruptedException {
		Runtime run = Runtime.getRuntime();
		Process pr;

		pr = run.exec("cp /var/task/test.sh /tmp");
		pr.waitFor();
		pr = run.exec("chmod 777 /tmp/test.sh");
		pr.waitFor();
		pr = run.exec("ls -lart /tmp");
		pr.waitFor();

		BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
		String line;
		while ((line=buf.readLine())!=null) {
			log.info("CMD output: " + line);
		}
	}

	private static void runshFile() throws IOException {
		log.info("About to run .sh file:");
		ProcessBuilder pb = new ProcessBuilder("/tmp/test.sh");
		Process p = pb.start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		while ((line = reader.readLine()) != null) {
			System.out.println(line);
		}
	}

	private static void readFile() throws IOException {
		log.info("Reading file:");
		AmazonS3 client = new AmazonS3Client();
		S3Object xFile = client.getObject("testbuc2209", "tFile1");
		InputStream contents = xFile.getObjectContent();

		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int length;
		while ((length = contents.read(buffer)) != -1) {
			result.write(buffer, 0, length);
		}

		log.info(result.toString(StandardCharsets.UTF_8.name()));
	}

}