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
	
	public static void main(String[] args) {
		LogRecordParser parser = new LogRecordParser();
		parser.feedInput("10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] \"GET / HTTP/1.1\" 403 202");
		String ip = parser.extractIPAddress();
		System.out.println(ip);
	}
}
