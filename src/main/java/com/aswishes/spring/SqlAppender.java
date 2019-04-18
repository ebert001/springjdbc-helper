package com.aswishes.spring;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aswishes.spring.exception.RDbException;

public class SqlAppender {
	private static final Logger logger = LoggerFactory.getLogger(SqlAppender.class);
	private StringBuilder sql = new StringBuilder(512);
	private Map<String, Object> paramMap = new HashMap<String, Object>();
	private List<Object> paramList = new ArrayList<Object>();
	private boolean appendWhiteSpace = false;
	
	private SqlAppender(boolean appendWhiteSpace) {
		this.appendWhiteSpace = appendWhiteSpace;
	}
	
	public static SqlAppender create(boolean appendWhiteSpace) {
		return new SqlAppender(appendWhiteSpace);
	}

	public static SqlAppender create(String sqlPhrase) {
		return new SqlAppender(false).append(sqlPhrase);
	}
	
	public static SqlAppender create(String sqlPhrase, String keys, Object...values) {
		return create(true, sqlPhrase, keys, values);
	}

	public static SqlAppender create(boolean condition, String sqlPhrase, String keys, Object...values) {
		return new SqlAppender(false).appendMultiKeys(condition, sqlPhrase, keys, values);
	}
	
	public static SqlAppender create(String sqlPhrase, Object...values) {
		return create(true, sqlPhrase, values);
	}

	public static SqlAppender create(boolean condition, String sqlPhrase, Object...values) {
		return new SqlAppender(false).appendValues(condition, sqlPhrase, values);
	}

	public SqlAppender append(boolean condition, String sqlPhrase) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		return this;
	}
	
	public SqlAppender append(String sqlPhrase) {
		return append(true, sqlPhrase);
	}
	
	//-----------------------------------------------------------------------------------------------
	// placeholders
	//-----------------------------------------------------------------------------------------------
	public SqlAppender append(boolean condition, String sqlPhrase, Object value) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramList.add(value);
		return this;
	}

	public SqlAppender append(String sqlPhrase, Object value) {
		return append(true, sqlPhrase, value);
	}
	
	public SqlAppender appendIfNotNull(String sqlPhrase, Object value) {
		return append(StringUtils.isNotNull(value), sqlPhrase, value);
	}
	
	public SqlAppender appendIfNotEmpty(String sqlPhrase, Object value) {
		return append(StringUtils.isNotEmpty(value), sqlPhrase, value);
	}
	
	public SqlAppender appendIfNotBlank(String sqlPhrase, Object value) {
		return append(StringUtils.isNotBlank(value), sqlPhrase, value);
	}

	public SqlAppender appendValues(boolean condition, String sqlPhrase, Object...values) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramList.addAll(Arrays.asList(values));
		return this;
	}
	
	public SqlAppender appendValues(String sqlPhrase, Object...values) {
		return appendValues(true, sqlPhrase, values);
	}
	
	/**
	 * sql "in" condition. Parameter buffered by list.
	 * Usage: appendIn(true, "and owner_id in ", list, true);
	 * @param condition true or false.
	 * @param sqlPhrase Example: "and owner_id in ". The kit will complete the  placeholders. The result maybe "and owner_id in (?,?,?)"
	 * @param list The caller should be promise that the list has values.
	 * @return
	 */
	public SqlAppender appendIn(boolean condition, String sqlPhrase, List<Object> list) {
		if (!condition) {
			return this;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < list.size(); i++) {
			sb.append(",?");
		}
		if (sb.length() > 1) {
			sb.deleteCharAt(0);
		}
		sql.append(sqlPhrase).append(" (").append(sb.toString()).append(") ");
		appendWhiteSpace();
		paramList.addAll(list);
		return this;
	}
	
	@SuppressWarnings("unchecked")
	public SqlAppender appendIn(boolean condition, String sqlPhrase, Object listOrArray) {
		if (listOrArray instanceof List) {
			return appendIn(condition, sqlPhrase, (List<Object>) listOrArray);
		} else if (listOrArray.getClass().isArray()) {
			Object[] arr = (Object[]) listOrArray;
			return appendIn(condition, sqlPhrase, Arrays.asList(arr));
		}
		return this;
	}
	
	public SqlAppender appendIn(boolean condition, String sqlPhrase, Object[] arr) {
		return appendIn(condition, sqlPhrase, Arrays.asList(arr));
	}
	
	
	public SqlAppender appendIn(String sqlPhrase, Object[] arr) {
		return appendIn(true, sqlPhrase, arr);
	}
	
	/**
	 * @see #appendIn(boolean, String, List)
	 * Usage: appendIn("and owner_id in ", list, true);
	 */
	public SqlAppender appendIn(String sqlPhrase, List<? extends Object> list) {
		return appendIn(true, sqlPhrase, list);
	}
	
	/**
	 * Usage: appendIn("and owner_id in ", list);
	 */
	public SqlAppender appendIn(String sqlPhrase, Object obj) {
		return appendIn(true, sqlPhrase, obj);
	}
	
	/**
	 * owner_id like '%value%'
	 */
	public SqlAppender appendLike(boolean condition, String sqlPhrase, Object value) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramList.add("%" + value + "%");
		return this;
	}
	
	public SqlAppender appendLike(String sqlPhrase, Object value) {
		return appendLike(true, sqlPhrase, value);
	}
	
	public SqlAppender appendLikeIfNotNull(String sqlPhrase, Object value) {
		return appendLike(StringUtils.isNotNull(value), sqlPhrase, value);
	}
	
	public SqlAppender appendLikeIfNotEmpty(String sqlPhrase, Object value) {
		return appendLike(StringUtils.isNotEmpty(value), sqlPhrase, value);
	}
	
	public SqlAppender appendLikeIfNotBlank(String sqlPhrase, Object value) {
		return appendLike(StringUtils.isNotBlank(value), sqlPhrase, value);
	}
	
	/**
	 * owner_id like '%value'
	 */
	public SqlAppender appendLikeLeft(boolean condition, String sqlPhrase, Object value) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramList.add("%" + value);
		return this;
	}
	
	public SqlAppender appendLikeLeft(String sqlPhrase, Object value) {
		return appendLikeLeft(true, sqlPhrase, value);
	}
	
	public SqlAppender appendLikeLeftIfNotNull(String sqlPhrase, Object value) {
		return appendLikeLeft(StringUtils.isNotNull(value), sqlPhrase, value);
	}
	
	public SqlAppender appendLikeLeftIfNotEmpty(String sqlPhrase, Object value) {
		return appendLikeLeft(StringUtils.isNotEmpty(value), sqlPhrase, value);
	}
	
	public SqlAppender appendLikeLeftIfNotBlank(String sqlPhrase, Object value) {
		return appendLikeLeft(StringUtils.isNotBlank(value), sqlPhrase, value);
	}
	
	public SqlAppender appendLikeRight(boolean condition, String sqlPhrase, String value) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramList.add(value + "%");
		return this;
	}
	
	/**
	 * owner_id like 'value%'
	 */
	public SqlAppender appendLikeRight(String sqlPhrase, String value) {
		return appendLikeRight(true, sqlPhrase, value);
	}
	
	public SqlAppender appendLikeRightIfNotNull(String sqlPhrase, String value) {
		return appendLikeRight(StringUtils.isNotNull(value), sqlPhrase, value);
	}
	
	public SqlAppender appendLikeRightIfNotEmpty(String sqlPhrase, String value) {
		return appendLikeRight(StringUtils.isNotEmpty(value), sqlPhrase, value);
	}
	
	public SqlAppender appendLikeRightIfNotBlank(String sqlPhrase, String value) {
		return appendLikeRight(StringUtils.isNotBlank(value), sqlPhrase, value);
	}
	
	//-----------------------------------------------------------------------------------------------
	// named(keys)
	//-----------------------------------------------------------------------------------------------
	public SqlAppender append(boolean condition, String sqlPhrase, String key, Object value) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramMap.put(key, value);
		return this;
	}
	
	public SqlAppender append(String sqlPhrase, String key, Object value) {
		return append(true, sqlPhrase, key, value);
	}

	public SqlAppender appendIfNotNull(String sqlPhrase, String key, Object value) {
		return append(StringUtils.isNotNull(value), sqlPhrase, key, value);
	}
	
	public SqlAppender appendIfNotEmpty(String sqlPhrase, String key, Object value) {
		return append(StringUtils.isNotEmpty(value), sqlPhrase, key, value);
	}
	
	public SqlAppender appendIfNotBlank(String sqlPhrase, String key, Object value) {
		return append(StringUtils.isNotBlank(value), sqlPhrase, key, value);
	}
	
	/**
	 * 注意: 如果是in操作，value应该放List，不能放Array.
	 */
	public SqlAppender appendMultiKeys(boolean condition, String sqlPhrase, String keys, Object...values) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		
		String[] keyArr = keys.split(",");
		List<String> keyList = new ArrayList<String>();
		for (String key : keyArr) {
			if (StringUtils.isBlank(key)) {
				continue;
			}
			keyList.add(key.trim());
		}
		if (keyList.size() != values.length) {
			throw new RDbException("Key count[" + keyList.size() + "] and value count[" + values.length + "] is inequality.");
		}
		for (int i = 0; i < keyList.size(); i++) {
			paramMap.put(keyList.get(i), values[i]);
		}
		return this;
	}
	
	public SqlAppender appendMultiKeys(String sqlPhrase, String keys, Object...values) {
		return appendMultiKeys(true, sqlPhrase, keys, values);
	}
	
	/**
	 * 如果SQL语句使用Map组织参数，可以使用此方式
	 */
	public SqlAppender append(boolean condition, String sqlPhrase, KV...kvs) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		if (kvs != null && kvs.length > 0) {
			for (KV kv : kvs) {
				paramMap.put(kv.getK(), kv.getV());
			}
		}
		return this;
	}
	
	/**
	 * @see SqlAppender#append(boolean, String, KV...)
	 */
	public SqlAppender append(String sqlPhrase, KV...kvs) {
		return append(true, sqlPhrase, kvs);
	}

	public SqlAppender appendIn(boolean condition, String sqlPhrase, String key, List<Object> list) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramMap.put(key, list);
		return this;
	}
	
	public SqlAppender appendIn(String sqlPhrase, String key, List<Object> list) {
		return appendIn(true, sqlPhrase, key, list);
	}
	
	@SuppressWarnings("unchecked")
	public SqlAppender appendIn(boolean condition, String sqlPhrase, String key, Object listOrArray) {
		if (listOrArray instanceof List) {
			return appendIn(condition, sqlPhrase, key, (List<Object>) listOrArray);
		} else if (listOrArray.getClass().isArray()) {
			Object[] arr = (Object[]) listOrArray;
			return appendIn(condition, sqlPhrase, key, Arrays.asList(arr));
		}
		return this;
	}
	
	public SqlAppender appendIn(String sqlPhrase, String key, Object listOrArray) {
		return appendIn(true, sqlPhrase, key, listOrArray);
	}
	
	public SqlAppender appendLike(boolean condition, String sqlPhrase, String key, Object value) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramMap.put(key, "%" + value + "%");
		return this;
	}
	
	public SqlAppender appendLike(String sqlPhrase, String key, Object value) {
		return appendLike(true, sqlPhrase, key, value);
	}

	public SqlAppender appendLikeLeft(boolean condition, String sqlPhrase, String key, Object value) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramMap.put(key, "%" + value);
		return this;
	}

	public SqlAppender appendLikeLeft(String sqlPhrase, String key, Object value) {
		return appendLikeLeft(true, sqlPhrase, key, value);
	}
	
	public SqlAppender appendLikeLeftIfNotNull(String sqlPhrase, String key, Object value) {
		return appendLikeLeft(StringUtils.isNotNull(value), sqlPhrase, key, value);
	}
	
	public SqlAppender appendLikeLeftIfNotEmpty(String sqlPhrase, String key, Object value) {
		return appendLikeLeft(StringUtils.isNotEmpty(value), sqlPhrase, key, value);
	}
	
	public SqlAppender appendLikeLeftIfNotBlank(String sqlPhrase, String key, Object value) {
		return appendLikeLeft(StringUtils.isNotBlank(value), sqlPhrase, key, value);
	}

	public SqlAppender appendLikeRight(boolean condition, String sqlPhrase, String key, Object value) {
		if (!condition) {
			return this;
		}
		sql.append(sqlPhrase);
		appendWhiteSpace();
		paramMap.put(key, value + "%");
		return this;
	}
	
	public SqlAppender appendLikeRight(String sqlPhrase, String key, Object value) {
		return appendLikeRight(true, sqlPhrase, key, value);
	}
	
	public SqlAppender appendLikeRightIfNotNull(String sqlPhrase, String key, Object value) {
		return appendLikeRight(StringUtils.isNotNull(value), sqlPhrase, key, value);
	}
	
	public SqlAppender appendLikeRightIfNotEmpty(String sqlPhrase, String key, Object value) {
		return appendLikeRight(StringUtils.isNotEmpty(value), sqlPhrase, key, value);
	}
	
	public SqlAppender appendLikeRightIfNotBlank(String sqlPhrase, String key, Object value) {
		return appendLikeRight(StringUtils.isNotBlank(value), sqlPhrase, key, value);
	}
	//-----------------------------------------------------------------------------------------------
	// others
	//-----------------------------------------------------------------------------------------------
	private void appendWhiteSpace() {
		if (appendWhiteSpace) {
			sql.append(" ");
		}
	}

	public String getSql() {
		return getSql(false);
	}
	
	public String getSql(boolean showSql) {
		String str = sql.toString();
		if (showSql) {
			logger.debug("Sql: {}", str);
		}
		return str;
	}

	public Map<String, Object> getParamMap() {
		return paramMap;
	}

	public List<Object> getParamList() {
		return paramList;
	}

	public Object[] getParamArray() {
		return paramList.toArray();
	}
}

