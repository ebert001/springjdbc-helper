package com.aswishes.spring;

import java.util.ArrayList;
import java.util.Collection;
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
	
	public static String toString(Object obj) {
		if (obj == null) {
			return "";
		}
		return obj.toString();
	}
	
	public static <T> T isTrue(boolean condition, T trueValue, T falseValue) {
		return condition ? trueValue : falseValue;
	}
	
	public static <T> T isNull(Object obj, T trueValue, T falseValue) {
		return isTrue(obj == null, trueValue, falseValue);
	}
	
	public static boolean isEmpty(Collection<?> collection) {
		return collection == null || collection.size() < 1;
	}
	
	public static <T> boolean isEmpty(T[] array) {
		return array == null || array.length < 1;
	}
	
	public static boolean isNotNull(Object value) {
		return value != null;
	}
	
	public static boolean isNotEmpty(Object value) {
		return value != null && String.valueOf(value).length() > 0;
	}
	
	public static boolean isNotBlank(Object value) {
		return value != null && String.valueOf(value).trim().length() > 0;
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
