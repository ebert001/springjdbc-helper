package com.aswishes.spring.dao;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;

import com.aswishes.spring.PageResult;

@Transactional
public abstract class AbstractNamedJdbcDao extends AbstractJdbcDao {
	protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	@Autowired
	public void setNamedParameterJdbcTemplate(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
		this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
	}

	public long namedQueryLong(String sql, Map<String, ?> param) {
		Long r = namedQueryObject(sql, param, Long.class);
		if (r == null) {
			return 0;
		}
		return r.longValue();
	}

	public int namedQueryInt(String sql, Map<String, ?> param) {
		Integer r = namedQueryObject(sql, param, Integer.class);
		if (r == null) {
			return 0;
		}
		return r.intValue();
	}

	public <E> E namedQueryObject(String sql, Map<String, ?> param, Class<E> requiredType) {
		try {
			return namedParameterJdbcTemplate.queryForObject(sql, param, requiredType);
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	public <E> E namedQueryObject(String sql, Map<String, ?> param, RowMapper<E> mapper) {
		try {
			return namedParameterJdbcTemplate.queryForObject(sql, param, mapper);
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	public <E> List<E> namedQueryList(String sql, Map<String, ?> param, RowMapper<E> mapper) {
		return namedParameterJdbcTemplate.query(sql, param, mapper);
	}

	public <E> List<E> namedQueryList(String sql, Map<String, ?> param, RowMapper<E> mapper, int pageNo, int pageSize) {
		sql += getLimitSql(pageNo, pageSize);
		return namedParameterJdbcTemplate.query(sql, param, mapper);
	}

	public <E> PageResult<E> namedQueryPage(final String countSql, final String dataSql, final Map<String, ?> param, final RowMapper<E> mapper, int pageNo, int pageSize) {
		PageResult<E> wrapper = new PageResult<E>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return namedQueryInt(countSql, param);
			}
			@Override
			public List<E> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return namedQueryList(dataSql, param, mapper, pageNo, pageSize);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return wrapper;
	}

	public List<Map<String, Object>> namedQueryList(String sql, Map<String, ?> param, int pageNo, int pageSize) {
		sql += getLimitSql(pageNo, pageSize);
		return namedParameterJdbcTemplate.queryForList(sql, param);
	}

	public PageResult<Map<String, Object>> namedQueryPage(final String countSql, final String dataSql, final Map<String, ?> param, int pageNo, int pageSize) {
		PageResult<Map<String, Object>> wrapper = new PageResult<Map<String, Object>>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return namedQueryInt(countSql, param);
			}
			@Override
			public List<Map<String, Object>> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return namedQueryList(dataSql, param, startIndex, pageSize);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return wrapper;
	}

	public Long namedSave(final String sql, final Map<String, ?> values) {
		KeyHolder holder = new GeneratedKeyHolder();
		namedParameterJdbcTemplate.update(sql, new MapSqlParameterSource(values), holder);
		return holder.getKey().longValue();
	}

	public int namedUpdate(final String sql, final Map<String, ?> values) {
		return namedParameterJdbcTemplate.update(sql, values);
	}
}
