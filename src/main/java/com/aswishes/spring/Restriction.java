package com.aswishes.spring;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * sql语句的条件约束
 * @author lizhou
 *
 */
public class Restriction {
	/** 等于 */
	public static final String EQ = "=";
	/** 不等于 */
	public static final String NOT_EQ = "!=";
	/** 小于 */
	public static final String LT = "<";
	/** 大于 */
	public static final String GT = ">";
	/** 小于等于 */
	public static final String LE = "<=";
	/** 大于等于 */
	public static final String GE = ">=";
	/** 模糊like */
	public static final String LIKE = "like";
	/** 模糊like 取反 */
	public static final String NOT_LIKE = "not like";
	/** 之间 between */
	public static final String BETWEEN = "between";
	/** in */
	public static final String IN = "in";
	/** not in */
	public static final String NOT_IN = "not in";

	/** 空 null */
	public static final String IS_NULL = "is null";
	/** 不为空 not null */
	public static final String IS_NOT_NULL = "is not null";
	/** 升序 */
	public static final String ORDER_ASC = "asc";
	/** 降序 */
	public static final String ORDER_DESC = "desc";

	private String fieldName;
	/** 属性值 */
	private Object value;
	/** between, in, not in会使用到 */
	private List<Object> values = new ArrayList<Object>();
	/** 比较类型 */
	private String type;
	/** 连接符 */
	private String boundSymbol = "and";
	/** 是否匹配，辅助进行业务逻辑处理 */
	private boolean matched = true;

	public Restriction() {
	}

	public Restriction(String type, String fieldName) {
		this.type = type.toLowerCase();
		this.fieldName = fieldName;
	}

	public Restriction(boolean matched, String type, String fieldName) {
		this(type, fieldName);
		this.matched = matched;
	}

	public Restriction(String type, String fieldName, Object value) {
		this(type, fieldName);
		this.value = value;
	}

	public Restriction(boolean matched, String type, String fieldName, Object value) {
		this(type, fieldName, value);
		this.matched = matched;
	}

	// between 和 in 使用
	public Restriction(String type, String fieldName, List<? extends Object> values) {
		this(type, fieldName);
		this.values.addAll(values);
	}

	public Restriction(boolean matched, String type, String fieldName, List<? extends Object> values) {
		this(type, fieldName, values);
		this.matched = matched;
	}

	public Restriction(String type, String fieldName, Object... values) {
		this(type, fieldName);
		this.values.addAll(Arrays.asList(values));
	}

	public Restriction(boolean matched, String type, String fieldName, Object... values) {
		this(type, fieldName, values);
		this.matched = matched;
	}

	public static Restriction le(String fieldName, Object value) {
		return new Restriction(LE, fieldName, value);
	}

	public static Restriction le(boolean matched, String fieldName, Object value) {
		return new Restriction(matched, LE, fieldName, value);
	}

	public static Restriction ge(String fieldName, Object value) {
		return new Restriction(GE, fieldName, value);
	}

	public static Restriction ge(boolean matched, String fieldName, Object value) {
		return new Restriction(matched, GE, fieldName, value);
	}

	public static Restriction eq(String fieldName, Object value) {
		return new Restriction(EQ, fieldName, value);
	}

	public static Restriction eq(boolean matched, String fieldName, Object value) {
		return new Restriction(matched, EQ, fieldName, value);
	}

	public static Restriction notEq(String fieldName, Object value) {
		return new Restriction(NOT_EQ, fieldName, value);
	}

	public static Restriction notEq(boolean matched, String fieldName, Object value) {
		return new Restriction(matched, NOT_EQ, fieldName, value);
	}

	public static Restriction lt(String fieldName, Object value) {
		return new Restriction(LT, fieldName, value);
	}

	public static Restriction lt(boolean matched, String fieldName, Object value) {
		return new Restriction(matched, LT, fieldName, value);
	}

	public static Restriction gt(String fieldName, Object value) {
		return new Restriction(GT, fieldName, value);
	}

	public static Restriction gt(boolean matched, String fieldName, Object value) {
		return new Restriction(matched, GT, fieldName, value);
	}

	public static Restriction likeBefore(String fieldName, Object value) {
		return likeBefore(true, fieldName, value);
	}

	public static Restriction likeBefore(boolean matched, String fieldName, Object value) {
		if (value == null || "".equals(String.valueOf(value))) {
			return null;
		}
		return new Restriction(matched, LIKE, fieldName, "%" + value);
	}

	public static Restriction likeAfter(String fieldName, Object value) {
		return likeAfter(true, fieldName, value);
	}

	public static Restriction likeAfter(boolean matched, String fieldName, Object value) {
		if (value == null || "".equals(String.valueOf(value))) {
			return null;
		}
		return new Restriction(matched, LIKE, fieldName, value + "%");
	}

	public static Restriction like(String fieldName, Object value) {
		return like(true, fieldName, value);
	}

	public static Restriction like(boolean matched, String fieldName, Object value) {
		if (value == null || "".equals(String.valueOf(value))) {
			return null;
		}
		return new Restriction(matched, LIKE, fieldName, "%" + value + "%");
	}

	public static Restriction notLike(String fieldName, Object value) {
		return new Restriction(NOT_LIKE, fieldName, value);
	}

	public static Restriction notLike(boolean matched, String fieldName, Object value) {
		return new Restriction(matched, NOT_LIKE, fieldName, value);
	}

	public static Restriction between(String fieldName, Object value1, Object value2) {
		return new Restriction(fieldName, BETWEEN, Arrays.asList(value1, value2));
	}

	public static Restriction between(boolean matched, String fieldName, Object value1, Object value2) {
		return new Restriction(matched, BETWEEN, fieldName, Arrays.asList(value1, value2));
	}

	public static Restriction in(String fieldName, Object... values) {
		return new Restriction(IN, fieldName, Arrays.asList(values));
	}

	public static Restriction in(boolean matched, String fieldName, Object... values) {
		return new Restriction(matched, IN, fieldName, Arrays.asList(values));
	}

	public static Restriction in(String fieldName, List<? extends Object> values) {
		return new Restriction(IN, fieldName, values);
	}

	public static Restriction in(boolean matched, String fieldName, List<? extends Object> values) {
		return new Restriction(matched, IN, fieldName, values);
	}

	public static Restriction notIn(String fieldName, Object... values) {
		return new Restriction(NOT_IN, fieldName, Arrays.asList(values));
	}

	public static Restriction notIn(boolean matched, String fieldName, Object... values) {
		return new Restriction(matched, NOT_IN, fieldName, Arrays.asList(values));
	}

	public static Restriction notIn(String fieldName, List<Object> values) {
		return new Restriction(NOT_IN, fieldName, values);
	}

	public static Restriction notIn(boolean matched, String fieldName, List<Object> values) {
		return new Restriction(matched, NOT_IN, fieldName, values);
	}

	public static Restriction isNull(String fieldName) {
		return new Restriction(IS_NULL, fieldName);
	}

	public static Restriction isNull(boolean matched, String fieldName) {
		return new Restriction(matched, IS_NULL, fieldName);
	}

	public static Restriction isNotNull(String fieldName) {
		return new Restriction(IS_NOT_NULL, fieldName);
	}

	public static Restriction isNotNull(boolean matched, String fieldName) {
		return new Restriction(matched, IS_NOT_NULL, fieldName);
	}

	public static Restriction orderByAsc(String fieldNames) {
		return new Restriction(ORDER_ASC, fieldNames);
	}

	public static Restriction orderByAsc(boolean matched, String fieldNames) {
		return new Restriction(matched, ORDER_ASC, fieldNames);
	}

	public static Restriction orderByDesc(String fieldNames) {
		return new Restriction(ORDER_DESC, fieldNames);
	}

	public static Restriction orderByDesc(boolean matched, String fieldNames) {
		return new Restriction(matched, ORDER_DESC, fieldNames);
	}

	public static Restriction or(Restriction restriction) {
		Restriction r = new Restriction();
		r.boundSymbol = "or";
		r.fieldName = restriction.fieldName;
		r.type = restriction.type;
		r.value = restriction.value;
		r.values = restriction.values;
		return r;
	}

	public String toSqlString() {
		if (EQ.equalsIgnoreCase(type) || NOT_EQ.equalsIgnoreCase(type) || LT.equalsIgnoreCase(type)
				|| GT.equalsIgnoreCase(type) || LE.equalsIgnoreCase(type) || GE.equalsIgnoreCase(type)
				|| LIKE.equalsIgnoreCase(type) || NOT_LIKE.equalsIgnoreCase(type)) {
			return buildString();
		}
		if (IS_NULL.equalsIgnoreCase(type) || IS_NOT_NULL.equalsIgnoreCase(type)) {
			return buildStringOfNull();
		}
		if (BETWEEN.equalsIgnoreCase(type)) {
			return buildStringOfBetween();
		}
		if (IN.equalsIgnoreCase(type) || NOT_IN.equalsIgnoreCase(type)) {
			return buildStringOfIn();
		}
		if (ORDER_ASC.equalsIgnoreCase(type) || ORDER_DESC.equalsIgnoreCase(type)) {
			return buildStringOfOrder();
		}
		return getBoundSymbol(boundSymbol) + fieldName + " = ? ";
	}

	/** 获取连接符(and, or)。如果是第一个块，不需要使用and,or */
	private String getBoundSymbol(String boundSymbol) {
		return (boundSymbol == null || boundSymbol.length() < 1) ? "" : (boundSymbol + " ");
	}
	private String buildString() {
		return new StringBuilder(getBoundSymbol(boundSymbol))
			.append(fieldName).append(" ").append(type).append(" ? ")
			.toString();
	}
	private String buildStringOfOrder() {
		StringBuilder sb = new StringBuilder("order by ");
		sb.append(fieldName).append(" ").append(type).append(" ");
		return sb.toString();
	}
	private String buildStringOfNull() {
		return new StringBuilder(getBoundSymbol(boundSymbol))
			.append(fieldName).append(" ").append(type).append(" ")
			.toString();
	}
	private String buildStringOfBetween() {
		if (values.size() != 2) {
			throw new IllegalArgumentException("between must have two values.");
		}
		return new StringBuilder(getBoundSymbol(boundSymbol))
			.append(fieldName).append(" ").append(type).append(" ? and ? ")
			.toString();
	}
	private String buildStringOfIn() {
		StringBuilder sb = new StringBuilder(getBoundSymbol(boundSymbol));
		sb.append(fieldName).append(" ").append(type).append("(");
		sb.append(repeat("?", ",", values.size()));
		sb.append(") ");
		return sb.toString();
	}

	public static String restrictionSql(Restriction... restrictions) {
		return restrictionSql(Arrays.asList(restrictions));
	}

	public static String restrictionSql(List<Restriction> restrictions) {
		StringBuilder sb = new StringBuilder("");
		if (restrictions == null || restrictions.size() < 1) {
			return sb.toString();
		}
		List<Restriction> orderRestrictions = new ArrayList<Restriction>();
		int num = 0;
		for (int i = 0; i < restrictions.size(); i++) {
			Restriction restriction = restrictions.get(i);
			if (restriction == null || !restriction.matched) {
				continue;
			}
			String type = restriction.getType();
			if (Restriction.IN.equals(type) || Restriction.NOT_IN.equals(type) || Restriction.BETWEEN.equals(type)) { // 多值
				if (restriction.getValues().size() < 1) { // 除去空值，这部分将不构成sql语句
					continue;
				}
			} else if (Restriction.IS_NULL.equals(type) || Restriction.IS_NOT_NULL.equals(type)) { // 无值
				// 不存在空值，但是是sql语句的一部分
			} else if (Restriction.ORDER_ASC.equals(type) || Restriction.ORDER_DESC.equals(type)) { // 无值
				// 考虑到多个order by选项的情况，order by 语句单独处理
				orderRestrictions.add(restriction);
				continue;
			} else { // 一个值
				if (restriction.getValue() == null) { // 除去空值，这部分将不构成sql语句
					continue;
				}
			}
			if (num == 0) {
				restriction.boundSymbol = "";
			}
			sb.append(restriction.toSqlString());
			num++;
		}
		for (int i = 0; i < orderRestrictions.size(); i++) {
			Restriction restriction = orderRestrictions.get(i);
			if (i == 0) {
				sb.append("order by ");
				sb.append(restriction.fieldName).append(" ").append(restriction.type).append(" ");
			} else {
				sb.append(", ").append(restriction.fieldName).append(" ").append(restriction.type).append(" ");
			}
		}
		return sb.toString();
	}

	public static String whereSql(Restriction...restrictions) {
		return whereSql(Arrays.asList(restrictions));
	}

	public static String whereSql(List<Restriction> restrictions) {
		String restrictionSql = restrictionSql(restrictions);
		if ("".equals(restrictionSql.trim())) {
			return restrictionSql;
		}
		if (restrictionSql.startsWith("order by")) { // 不需要前缀 where
			return restrictionSql;
		}
		return restrictionSql;
	}

	public static Object[] whereValueArray(Restriction...restrictions) {
		return whereValueList(restrictions).toArray();
	}

	public static List<Object> whereValueList(Restriction...restrictions) {
		List<Object> list = new ArrayList<Object>();
		if (restrictions == null || restrictions.length < 1) {
			return list;
		}
		for (Restriction restriction : restrictions) {
			if (restriction == null || !restriction.matched) {
				continue;
			}
			String type = restriction.getType();
			if (Restriction.IN.equals(type) || Restriction.NOT_IN.equals(type) || Restriction.BETWEEN.equals(type)) { // 多值
				if (restriction.getValues().size() < 1) {
					continue;
				}
				list.addAll(restriction.getValues());

			} else if (Restriction.IS_NULL.equals(type) || Restriction.IS_NOT_NULL.equals(type)
					|| Restriction.ORDER_ASC.equals(type) || Restriction.ORDER_DESC.equals(type)) { // 无值
				continue;

			} else { // 一个值
				if (restriction.getValue() == null) {
					continue;
				}
				list.add(restriction.getValue());
			}
		}
		return list;
	}

	public static Map<String, Object> valueMap(Restriction...restrictions) {
		Map<String, Object> values = new HashMap<String, Object>();
		if (restrictions == null || restrictions.length < 1) {
			return values;
		}
		for (Restriction restriction : restrictions) {
			if (restriction == null) {
				continue;
			}
			String type = restriction.getType();

			if (Restriction.IN.equals(type) || Restriction.NOT_IN.equals(type) || Restriction.BETWEEN.equals(type)) { // 多值
				values.put(restriction.getFieldName(), restriction.getValues());

			} else if (Restriction.IS_NULL.equals(type) || Restriction.IS_NOT_NULL.equals(type)
					|| Restriction.ORDER_ASC.equals(type) || Restriction.ORDER_DESC.equals(type)) { // 无值
				continue;

			} else { // 一个值
				values.put(restriction.getFieldName(), restriction.getValue());
			}
		}
		return values;
	}

	public static String repeat(String content, String separator, int num) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < num - 1; i++) {
			sb.append(content).append(separator);
		}
		sb.append(content);
		return sb.toString();
	}

	public static String join(String[] values, String separator, String appendLast) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < values.length - 1; i++) {
			sb.append(values[i]).append(separator);
		}
		sb.append(values[values.length - 1]).append(appendLast == null ? "" : appendLast);
		return sb.toString();
	}

	public static String join(String[] values, String separator) {
		return join(values, separator, null);
	}

	public static String join(List<String> values, String separator, String appendLast) {
		return join(values.toArray(new String[values.size()]), separator, appendLast);
	}

	public static String join(List<String> values, String separator) {
		return join(values, separator, null);
	}

	public static List<Restriction> convert(List<String> columns, List<Object> values) {
		List<Restriction> result = new ArrayList<Restriction>();
		for (int i = 0; i < columns.size(); i++) {
			result.add(Restriction.eq(columns.get(i), values.get(i)));
		}
		return result;
	}

	public String getType() {
		return type;
	}

	public String getFieldName() {
		return fieldName;
	}

	public List<Object> getValues() {
		return values;
	}

	public Object getValue() {
		return value;
	}

}

