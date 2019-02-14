package com.aswishes.spring.mapper;

public class BooleanConverter implements TypeConverter {

	public Object convert(Object v) {
		if (v == null) {
			return false;
		}
		if (v instanceof Byte) {
			return ((Byte) v).byteValue() != 0;
		}
		if (v instanceof Short) {
			return ((Short) v).shortValue() != 0;
		}
		if (v instanceof Integer) {
			return ((Integer) v).intValue() != 0;
		}
		if (v instanceof Long) {
			return ((Long) v).longValue() != 0;
		}
		if (v instanceof String) {
			String s = String.valueOf(v).trim();
			return "YES".equalsIgnoreCase(s) ||
					"OK".equalsIgnoreCase(s) ||
					"Y".equalsIgnoreCase(s) ||
					"1".equalsIgnoreCase(s) ||
					"是".equalsIgnoreCase(s);
		}
		return false;
	}

}
