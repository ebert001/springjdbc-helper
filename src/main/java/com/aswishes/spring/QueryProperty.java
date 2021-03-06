package com.aswishes.spring;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.ConvertUtils;

/**
 * Format: 
 * Q-S-LIKE-username
 * Q-L-IN-id
 * Q-OA-id
 * Q-OD-username
 * @author lizhou
 */
public class QueryProperty {
	
	private String propertyName = null;
	private Object propertyValue = null;
	private Class<?> propertyClass = null;
	private MatchType matchType = null;

	/** 比较类型. */
	public enum MatchType {
		IN("in"), NI("not in"), LIKE("like"),  
		EQ("="), LT("<"), GT(">"), LE("<="), GE(">="), 
		OA("ASC"), OD("DESC");
		private String name;
		private MatchType(String name) {
			this.name = name;
		}
		public String getName() {
			return name;
		}
	}

	/** 数据类型. */
	public enum PropertyType {
		S(String.class), I(Integer.class), L(Long.class), N(Double.class), D(Date.class), B(Boolean.class), C(Character.class);

		private Class<?> clazz;
		private PropertyType(Class<?> clazz) {
			this.clazz = clazz;
		}
		public Class<?> getValue() {
			return clazz;
		}
	}

	/**
	 * @param name property name. Such as: S-LIKE-username
	 * @param value property value.
	 */
	public QueryProperty(String name, String value) {
		String[] ss = name.split("-", 3);
		
		propertyClass = Enum.valueOf(PropertyType.class, ss[0]).getValue();
		matchType = Enum.valueOf(MatchType.class, ss[1]);
		propertyName = ss[2];
		
		if (matchType == MatchType.IN || matchType == MatchType.NI) {
			propertyValue = ConvertUtils.convert(value.split(","), propertyClass);
		} else {
			propertyValue = ConvertUtils.convert(value, propertyClass);
		}
	}
	
	/**
	 * @param name OA-id, OD-name
	 */
	public QueryProperty(String name) {
		String[] ss = name.split("-", 2);
		matchType = Enum.valueOf(MatchType.class, ss[0]);
		propertyName = ss[1];
	}
	
	public Restriction toRestriction() {
		return new Restriction(matchType.getName(), propertyName, propertyValue);
	}

	public static List<QueryProperty> toQueryProperty(Map<String, String> param, String prefix) {
		List<QueryProperty> list = new ArrayList<QueryProperty>();
		for (Map.Entry<String, String> entry : param.entrySet()) {
			String name = entry.getKey();
			if (!name.startsWith(prefix)) {
				continue;
			}
			String value = entry.getValue();
			if (value == null) {
				continue;
			}
			if (value.isEmpty()) {
				continue;
			}
			String pname = name.substring(name.indexOf("-") + 1);
			if (pname.startsWith("O")) {
				list.add(new QueryProperty(pname));
			} else {
				list.add(new QueryProperty(pname, value));
			}
		}
		return list;
	}
	
	public static List<QueryProperty> toQueryProperty(Map<String, String> param) {
		return toQueryProperty(param, "Q");
	}
	
	public static Restriction[] toRestrictions(List<QueryProperty> list) {
		Restriction[] arr = new Restriction[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = list.get(i).toRestriction();
		}
		return arr;
	}
	
	public static Restriction[] toRestrictions(Map<String, String> param) {
		return toRestrictions(toQueryProperty(param));
	}
}
