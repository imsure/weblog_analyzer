package cloudera.lab;

import java.util.regex.*;

public class LogRecordParser {
	private String record;
	private static final String IPRegex = "^\\d+\\.\\d+\\.\\d+\\.\\d+";
	
	public void feedInput(String input) {
		record = input;
	}
	
	public String extractIPAddress() {
		Pattern pattern = Pattern.compile(IPRegex);
		Matcher matcher = pattern.matcher(record);
		
		if (matcher.find()) {
			return matcher.group();
		} else {
			return null;
		}
	}
	
	public String getRequestedFileName() {
		String[] fields = this.record.split("\"");
		if (fields.length > 1) {
			String request = fields[1];
			if (request.startsWith("GET")) {
				String filename = request.split(" ")[1];
				return filename;
			}
		}
		return null;
	}
	
	public static void main(String[] args) {
		LogRecordParser parser = new LogRecordParser();
		parser.feedInput("10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] \"GET / HTTP/1.1\" 403 202");
		String ip = parser.extractIPAddress();
		System.out.println(ip);
		parser.feedInput("10.153.239.5 - - [29/Jul/2009:09:17:54 -0700] \"GET /assets/css/reset.css HTTP/1.1\" 304 -");
		String filename = parser.getRequestedFileName();
		System.out.println(filename);
	}
}
