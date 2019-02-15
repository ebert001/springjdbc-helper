package com.aswishes.spring;

import java.util.ArrayList;
import java.util.List;

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
	
	/**
	 * Example:
	 * <pre>
     * Restriction.addEach([1, 2, 3], 'hello', true)   = ['hello1', 'hello2', 'hello3']
     * Restriction.addEach([1, 2, 3], 'hello', false)   = ['1hello', '2hello', '3hello']
     * </pre>
     * @param content basic string
	 * @param list prefix or suffix
	 * @param addBefore add content before or after
	 * @return Recombine string
	 */
	public static List<String> addEach(String content, List<String> list, boolean addBefore) {
		List<String> result = new ArrayList<String>();
		for (String s : list) {
			result.add(addBefore ? (content + s) : (s + content));
		}
		return result;
	}
	
}
