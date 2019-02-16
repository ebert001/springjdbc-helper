package com.aswishes.spring.mapper;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.ConcurrentReferenceHashMap;

import com.aswishes.spring.StringUtils;

public class MapperHelper {
	private static Map<Class<?>, Map<String, MapperField>> cache = new ConcurrentHashMap<Class<?>, Map<String, MapperField>>();
	private static Map<Class<?>, TypeConverter> converterCache = new ConcurrentHashMap<Class<?>, TypeConverter>();
	private static final Map<Class<?>, Field[]> declaredFieldsCache = new ConcurrentReferenceHashMap<>(256);

	/**
	 * 构建 Mapper 对象
	 * @param <T> the class of the object
	 * @param clazz the class which will be convert to RowMapper object.
	 * @return RowMapper object
	 */
	public static <T> RowMapper<T> getMapper(Class<T> clazz) {
		Map<String, MapperField> fmap = cache.get(clazz);
		if (fmap != null) {
			return new RsMapper<T>(clazz, fmap);
		}
		fmap = new HashMap<String, MapperField>();
		Class<?> targetClass = clazz;
		do {
			Field[] fields = getDeclaredFields(targetClass);
			for (Field field : fields) {
				Mapper mapper = field.getAnnotation(Mapper.class);
				String columnName = field.getName();
				Class<? extends TypeConverter> typeConvert = null;
				if (mapper == null) {
				}
				// 不需要此判断，这样实体可以作为值对象使用.简化对象定义
//				else if (mapper.ignore()) {
//					continue;
//				} 
				else {
					if (StringUtils.isNotBlank(mapper.name())) {
						columnName = mapper.name();
					}
					typeConvert = mapper.typeConvert();
				}
				if (field.isAccessible() == false) {
					field.setAccessible(true);
				}
				fmap.put(columnName, new MapperField(field, typeConvert));
			}
			targetClass = targetClass.getSuperclass();
		} while (targetClass != null && targetClass != Object.class);
		cache.put(clazz, fmap);
		return new RsMapper<T>(clazz, fmap);
	}

	/**
	 * Construct Mapper Object of given class
	 */
	public static class RsMapper<T> implements RowMapper<T> {
		private Class<T> clazz;
		private Map<String, MapperField> fieldMap;
		public RsMapper(Class<T> clazz, Map<String, MapperField> fieldMap) {
			this.clazz = clazz;
			this.fieldMap = fieldMap;
		}
		@Override
		public T mapRow(ResultSet rs, int rowNum) throws SQLException {
			T rowObj = null;
			try {
				rowObj = clazz.newInstance();
			} catch (Exception e) {
				throw new SQLException("construct result object error", e);
			}
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			List<String> columnNames = new ArrayList<String>();
			for (int i = 1; i <= columnCount; i++) {
				columnNames.add(metaData.getColumnName(i));
			}
			for (Entry<String, MapperField> entry : fieldMap.entrySet()) {
				String column = entry.getKey();
				if (!columnNames.contains(column)) {
					continue;
				}
				MapperField mapperField = entry.getValue();
				setValue(rowObj, mapperField, rs.getObject(column));
			}
			return rowObj;
		}
		private void setValue(Object rowObj, MapperField mapperField, Object dbValue) throws SQLException {
			String setterName = getSetterName(mapperField.field.getName());
			try {
				Method setterMethod = getDeclaredMethod(clazz, setterName, mapperField.field.getType());
				if (setterMethod == null) {
					throw new NoSuchMethodException(clazz.getName() + "." + setterName);
				}
				Class<? extends TypeConverter> typeConverter = mapperField.typeConverter;
				if (typeConverter == null || DefaultConverter.class.equals(typeConverter)) {
					setterMethod.invoke(rowObj, dbValue);
					return;
				}
				TypeConverter converterObj = converterCache.get(typeConverter);
				if (converterObj == null) {
					converterObj = typeConverter.newInstance();
					converterCache.put(typeConverter, converterObj);
				}
				setterMethod.invoke(rowObj, converterObj.convert(dbValue));
			} catch (Exception e) {
				throw new SQLException("Set value error. The setter method name:" + setterName, e);
			}
		}
		private String getSetterName(String fieldName) {
			char[] arr = fieldName.toCharArray();
			arr[0] = Character.toUpperCase(arr[0]);
			return "set" + new String(arr);
		}
	}

	private static class MapperField {
		Class<? extends TypeConverter> typeConverter;
		Field field;
		public MapperField(Field field, Class<? extends TypeConverter> typeConvert) {
			this.field = field;
			this.typeConverter = typeConvert;
		}
	}
	
	private static Field[] getDeclaredFields(Class<?> clazz) {
		Field[] result = declaredFieldsCache.get(clazz);
		if (result != null) {
			return result;
		}
		try {
			result = clazz.getDeclaredFields();
			declaredFieldsCache.put(clazz, (result.length == 0 ? new Field[0] : result));
			return result;
		} catch (Throwable ex) {
			throw new IllegalStateException("Failed to introspect Class [" + clazz.getName() +
					"] from ClassLoader [" + clazz.getClassLoader() + "]", ex);
		}
	}
	
	private static Method getDeclaredMethod(Class<?> clazz, String name, Class<?>...parameterTypes) {
		Class<?> targetClass = clazz;
		do {
			try {
				return targetClass.getDeclaredMethod(name, parameterTypes);
			} catch (NoSuchMethodException e) {
				targetClass = targetClass.getSuperclass();
			}
		} while (targetClass != null && targetClass != Object.class);
		return null;
	}
}
