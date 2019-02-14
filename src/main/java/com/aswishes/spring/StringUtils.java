package com.aswishes.spring;

public class StringUtils {

	public static boolean contain(String[] arr, String v) {
    	if (arr == null || arr.length < 1 || v == null) {
    		throw new IllegalStateException("The source array or value is null");
    	}
    	for (String s : arr) {
    		if (v.equals(s)) {
    			return true;
    		}
    	}
    	return false;
    }
	
	public static boolean isBlank(String s) {
		if (s == null) {
			return true;
		}
		if (s.trim().length() < 1) {
			return true;
		}
		return false;
	}
	
	public static boolean isNotBlank(String s) {
		return !isBlank(s);
	}
	
}
