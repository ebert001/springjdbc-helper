package com.aswishes.spring.mapper;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * RowMapper annotation writen
 * @author lizhou
 */
@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface Mapper {

	/**
	 * @return column name
	 */
	String name() default "";
	
	/**
	 * @return table name
	 */
	String tableName() default "";

	/**
	 * @return primary key
	 */
	String[] primaryKey() default {};

	/**
	 * @return  true, the field will be not in result list.
	 */
	boolean ignore() default false;

	/**
	 * database data type and java data type convert interface.
	 * @return custom convert menthod
	 */
	Class<? extends TypeConverter> typeConvert() default DefaultConverter.class;
}
