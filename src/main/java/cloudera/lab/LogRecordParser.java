package cloudera.lab;

import java.util.regex.*;

public class LogRecordParser {
	private String record;
	private static final String IPRegex = "^\\d+\\.\\d+\\.\\d+\\.\\d+";
	private static final String MonthRegex = "\\[\\d+/(\\w+)/";
	
	private Pattern pattern;
	private Matcher matcher;
	
	public void feedInput(String input) {
		record = input;
	}
	
	public String extractIPAddress() {
		pattern = Pattern.compile(IPRegex);
		matcher = pattern.matcher(record);
		
		if (matcher.find()) {
			return matcher.group();
		} else {
			return null;
		}
	}
	
	public String extractMonth() {
		pattern = Pattern.compile(MonthRegex);
		matcher = pattern.matcher(record);
		
		if (matcher.find()) {
			return matcher.group(1);
		} else {
			return null;
		}
	}
	
	public static void main(String[] args) {
		LogRecordParser parser = new LogRecordParser();
		parser.feedInput("10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] \"GET / HTTP/1.1\" 403 202");
		String ip = parser.extractIPAddress();
		System.out.println(ip);
		String month = parser.extractMonth();
		System.out.println(month);
	}
}
