package com.aswishes.spring.mapper;

/**
 * Usage: DB type convert to Java type.
 * @author lizhou
 *
 */
public interface TypeConverter {

	public Object convert(Object v);
}
