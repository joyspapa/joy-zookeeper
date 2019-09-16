package com.joy.util;

public class Test {
	public static void main(String[] args) {
		
		extractZnodeName("DE1567052466-ENT3243-1");
		extractZnodeName("DE1567052466-ENT3243");
		extractZnodeName("/logplanet/memorygrid/timeoutevent/DE1567052466ENT3243");
		extractZnodeName("/logplanet/memorygrid/timeoutevent/DE1567052466-ENT3243");
		extractZnodeName("/logplanet/memorygrid/timeoutevent/DE1567052466-ENT3243-1");
		extractZnodeName("DE1567052466ENT3243");
	}
	
	public static String extractZnodeName(String src) {
		String znodeName = src;

		if (znodeName.contains("/")) {
			String[] znodePaths = znodeName.split("/");
			znodeName = znodePaths[znodePaths.length - 1];
		}

		if (znodeName.contains("-")) {
			String[] znodeNames = znodeName.split("-");
			znodeName = (znodeNames.length > 1) ? znodeNames[0] + znodeNames[1] : znodeNames[0];
		}
		
		System.out.println("[extractZnodeName] znodeName : " + znodeName);
		return znodeName;
	}
}
