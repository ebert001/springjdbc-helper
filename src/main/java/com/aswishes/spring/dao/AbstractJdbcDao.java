package com.aswishes.spring.dao;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.FieldCallback;

import com.aswishes.spring.PageResult;
import com.aswishes.spring.Restriction;
import com.aswishes.spring.StringUtils;
import com.aswishes.spring.SqlHelper;
import com.aswishes.spring.SqlHelper.Columns;
import com.aswishes.spring.SqlHelper.Delete;
import com.aswishes.spring.SqlHelper.Insert;
import com.aswishes.spring.SqlHelper.Update;
import com.aswishes.spring.exception.RDbException;
import com.aswishes.spring.mapper.Mapper;
import com.aswishes.spring.mapper.MapperHelper.RsMapper;

@Transactional
public abstract class AbstractJdbcDao {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractJdbcDao.class);
	protected JdbcTemplate jdbcTemplate;
	protected String tableName;
	protected boolean showSql = false;

	public AbstractJdbcDao() {
		setTableName();
	}

	@Autowired
	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	protected abstract void setTableName();

	/**
	 * @param <E> the class of the object
	 * @param sql Select SQL, the result only contain one field
	 * @param requiredType 只能是基本类型的包装类型
	 * @param args Condition arguments
	 * @return 基本类型的数据对象
	 */
	@Transactional(noRollbackFor = {EmptyResultDataAccessException.class})
	public <E> E getObject(String sql, Class<E> requiredType, Object...args) {
		try {
			return jdbcTemplate.queryForObject(sql, requiredType, args);
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	@Transactional(noRollbackFor = {EmptyResultDataAccessException.class})
	public <E> E getObject(String sql, Class<E> requiredType, Restriction...restrictions) {
		try {
			return jdbcTemplate.queryForObject(sql, requiredType, Restriction.whereValueArray(restrictions));
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	@Transactional(noRollbackFor = {EmptyResultDataAccessException.class})
	public <E> E getObject(String sql, RowMapper<E> mapper, Object...args) {
		try {
			return jdbcTemplate.queryForObject(sql, mapper, args);
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	@Transactional(noRollbackFor = {EmptyResultDataAccessException.class})
	public <E> E getObject(String sql, RowMapper<E> mapper, Restriction...restrictions) {
		try {
			return jdbcTemplate.queryForObject(sql, mapper, Restriction.whereValueArray(restrictions));
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	@Transactional(noRollbackFor = {EmptyResultDataAccessException.class})
	public <E> E getObjectBy(RowMapper<E> mapper, Restriction...restrictions) {
		String sql = SqlHelper.select(getTableName(mapper, tableName)).columns("*").where(restrictions).toSqlString();
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		try {
			return jdbcTemplate.queryForObject(sql, mapper, Restriction.whereValueArray(restrictions));
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	@Transactional
	public int getCount() {
		String sql = SqlHelper.select(tableName).count("*").where("").toCountString();
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		Integer result = jdbcTemplate.queryForObject(sql, Integer.class);
		return result == null ? 0 : result.intValue();
	}

	@Transactional
	public <E> List<E> getList(RowMapper<E> mapper) {
		String sql = SqlHelper.select(getTableName(mapper, tableName)).columns("*").where("").toSqlString();
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		return jdbcTemplate.query(sql, mapper);
	}

	@Transactional
	public List<Map<String, Object>> getList() {
		String sql = SqlHelper.select(tableName).columns("*").where("").toSqlString();
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		return jdbcTemplate.queryForList(sql);
	}

	@Transactional
	public int getCount(Restriction...restrictions) {
		String sql = SqlHelper.select(tableName).count("*").where(restrictions).toCountString();
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		Integer result = jdbcTemplate.queryForObject(sql, Integer.class, Restriction.whereValueArray(restrictions));
		return result == null ? 0 : result;
	}

	@Transactional
	public <E> List<E> getList(RowMapper<E> mapper, Restriction...restrictions) {
		String sql = SqlHelper.select(getTableName(mapper, tableName)).columns("*").where(restrictions).toSqlString();
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		return jdbcTemplate.query(sql, mapper, Restriction.whereValueArray(restrictions));
	}

	@Transactional
	public List<Map<String, Object>> getList(Restriction...restrictions) {
		String sql = SqlHelper.select(tableName).columns("*").where(restrictions).toSqlString();
		return jdbcTemplate.queryForList(sql, Restriction.whereValueArray(restrictions));
	}

	@Transactional
	public <E> List<E> getList(RowMapper<E> mapper, int pageNo, int pageSize) {
		String sql = SqlHelper.select(getTableName(mapper, tableName)).columns("*").where().toSqlString();
		sql += getLimitSql(pageNo, pageSize);
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		return jdbcTemplate.query(sql, mapper);
	}

	@Transactional
	public List<Map<String, Object>> getList(int pageNo, int pageSize) {
		String sql = SqlHelper.select(tableName).columns("*").where().toSqlString();
		sql += getLimitSql(pageNo, pageSize);
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		return jdbcTemplate.queryForList(sql);
	}

	@Transactional
	public <E> List<E> getList(RowMapper<E> mapper, int pageNo, int pageSize, Restriction...restrictions) {
		String sql = SqlHelper.select(getTableName(mapper, tableName)).columns("*").where(restrictions).toSqlString();
		sql += getLimitSql(pageNo, pageSize);
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		return jdbcTemplate.query(sql, mapper, Restriction.whereValueArray(restrictions));
	}

	@Transactional
	public List<Map<String, Object>> getList(int pageNo, int pageSize, Restriction...restrictions) {
		String sql = SqlHelper.select(tableName).columns("*").where(restrictions).toSqlString();
		sql += getLimitSql(pageNo, pageSize);
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		return jdbcTemplate.queryForList(sql, Restriction.whereValueArray(restrictions));
	}

	@Transactional
	public <E> PageResult<E> getPage(final RowMapper<E> bean, int pageNo, int pageSize) {
		PageResult<E> wrapper = new PageResult<E>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return getCount();
			}
			@Override
			public List<E> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return getList(bean, pageNo, pageSize);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			throw new RDbException(e.getCause());
		}
		return wrapper;
	}

	@Transactional
	public PageResult<Map<String, Object>> getPage(int pageNo, int pageSize) {
		PageResult<Map<String, Object>> wrapper = new PageResult<Map<String, Object>>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return getCount();
			}
			@Override
			public List<Map<String, Object>> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return getList(pageNo, pageSize);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			throw new RDbException(e.getCause());
		}
		return wrapper;
	}

	@Transactional
	public <E> PageResult<E> getPage(final RowMapper<E> bean, int pageNo, int pageSize, final Restriction...restrictions) {
		PageResult<E> wrapper = new PageResult<E>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return getCount(restrictions);
			}
			@Override
			public List<E> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return getList(bean, pageNo, pageSize, restrictions);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			throw new RDbException(e.getCause());
		}
		return wrapper;
	}

	@Transactional
	public PageResult<Map<String, Object>> getPage(int pageNo, int pageSize, final Restriction...restrictions) {
		PageResult<Map<String, Object>> wrapper = new PageResult<Map<String, Object>>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return getCount(restrictions);
			}
			@Override
			public List<Map<String, Object>> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return getList(pageNo, pageSize, restrictions);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			throw new RDbException(e.getCause());
		}
		return wrapper;
	}

	@Transactional
	public int getCount(String sql, Restriction...restrictions) {
		if (restrictions == null || restrictions.length < 1) {
			return jdbcTemplate.queryForObject(sql, Integer.class);
		}
		return jdbcTemplate.queryForObject(sql, Integer.class, Restriction.whereValueArray(restrictions));
	}

	@Transactional
	public <E> List<E> getList(String sql, RowMapper<E> bean, Restriction...restrictions) {
		if (restrictions == null || restrictions.length < 1) {
			return jdbcTemplate.query(sql, bean);
		}
		return jdbcTemplate.query(sql, bean, Restriction.whereValueArray(restrictions));
	}

	@Transactional
	public List<Map<String, Object>> getList(String sql, Restriction...restrictions) {
		if (restrictions == null || restrictions.length < 1) {
			return jdbcTemplate.queryForList(sql);
		}
		return jdbcTemplate.queryForList(sql, Restriction.whereValueArray(restrictions));
	}

	@Transactional
	public <E> List<E> getList(String sql, RowMapper<E> bean, int pageNo, int pageSize, Restriction...restrictions) {
		sql += getLimitSql(pageNo, pageSize);
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		if (restrictions == null || restrictions.length < 1) {
			return jdbcTemplate.query(sql, bean);
		}
		return jdbcTemplate.query(sql, bean, Restriction.whereValueArray(restrictions));
	}

	@Transactional
	public List<Map<String, Object>> getList(String sql, int pageNo, int pageSize, Restriction...restrictions) {
		sql += getLimitSql(pageNo, pageSize);
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		if (restrictions == null || restrictions.length < 1) {
			return jdbcTemplate.queryForList(sql);
		}
		return jdbcTemplate.queryForList(sql, Restriction.whereValueArray(restrictions));
	}

	@Transactional
	public <E> PageResult<E> getPage(final String countSql, final String dataSql, final RowMapper<E> bean, int pageNo, int pageSize, final Restriction...restrictions) {
		PageResult<E> wrapper = new PageResult<E>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return getCount(countSql, restrictions);
			}
			@Override
			public List<E> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return getList(dataSql, bean, pageNo, pageSize, restrictions);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			throw new RDbException(e.getCause());
		}
		return wrapper;
	}

	@Transactional
	public PageResult<Map<String, Object>> getPage(final String countSql, final String dataSql, int pageNo, int pageSize, final Restriction...restrictions) {
		PageResult<Map<String, Object>> wrapper = new PageResult<Map<String, Object>>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return getCount(countSql, restrictions);
			}
			@Override
			public List<Map<String, Object>> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return getList(dataSql, pageNo, pageSize, restrictions);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			throw new RDbException(e.getCause());
		}
		return wrapper;
	}

	@Transactional
	public int getCount(String sql, Object...args) {
		Integer result = jdbcTemplate.queryForObject(sql, Integer.class, args);
		return result == null ? 0 : result.intValue();
	}

	@Transactional
	public <E> List<E> getList(String sql, RowMapper<E> bean, Object...args) {
		return jdbcTemplate.query(sql, bean, args);
	}

	@Transactional
	public List<Map<String, Object>> getList(String sql, Object...args) {
		return jdbcTemplate.queryForList(sql, args);
	}

	@Transactional
	public <E> List<E> getList(String sql, RowMapper<E> bean, int pageNo, int pageSize, Object...args) {
		sql += getLimitSql(pageNo, pageSize);
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		return jdbcTemplate.query(sql, bean, args);
	}

	@Transactional
	public List<Map<String, Object>> getList(String sql, int pageNo, int pageSize, Object...args) {
		sql += getLimitSql(pageNo, pageSize);
		if (showSql) {
			logger.debug("Select SQL: {}", sql);
		}
		return jdbcTemplate.queryForList(sql, args);
	}

	@Transactional
	public <E> PageResult<E> getPage(final String countSql, final String dataSql, final RowMapper<E> bean, int pageNo, int pageSize, final Object...args) {
		PageResult<E> wrapper = new PageResult<E>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return getCount(countSql, args);
			}
			@Override
			public List<E> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return getList(dataSql, bean, pageNo, pageSize, args);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			throw new RDbException(e.getCause());
		}
		return wrapper;
	}

	@Transactional
	public PageResult<Map<String, Object>> getPage(final String countSql, final String dataSql, int pageNo, int pageSize, final Object...args) {
		PageResult<Map<String, Object>> wrapper = new PageResult<Map<String, Object>>(pageNo, pageSize) {
			@Override
			public int queryCount() throws Exception {
				return getCount(countSql, args);
			}
			@Override
			public List<Map<String, Object>> query(int startIndex, int pageNo, int pageSize) throws Exception {
				return getList(dataSql, pageNo, pageSize, args);
			}
		};
		try {
			wrapper.paging();
		} catch (Exception e) {
			throw new RDbException(e.getCause());
		}
		return wrapper;
	}

	protected String getLimitSql(int pageNo, int pageSize) {
		return " limit " + getStartIndex(pageNo, pageSize) + "," + pageSize;
	}

	protected int getStartIndex(int pageNo, int pageSize) {
		return (pageNo - 1) * pageSize;
	}

	public void delete(Restriction...restrictions) {
		String sql = SqlHelper.delete(tableName).where(Restriction.whereSql(restrictions));
		if (showSql) {
			logger.debug("Delete SQL: {}", sql);
		}
		jdbcTemplate.update(sql, Restriction.whereValueArray(restrictions));
	}

	public <T> void save(T t) {
		List<Object> values = new ArrayList<Object>();
		List<String> columns = new ArrayList<String>();
		ReflectionUtils.doWithFields(t.getClass(), new FieldCallback() {
			@Override
			public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
				if (!field.isAccessible() ) {
					field.setAccessible(true);
				}
				Object value = ReflectionUtils.getField(field, t);
				if (value == null) {
					return;
				}
				Mapper mapper = field.getAnnotation(Mapper.class);
				String name = field.getName();
				if (mapper == null) {
				} else if (mapper.ignore()) {
					return;
				} else if (StringUtils.isNotBlank(mapper.name())) {
					name = mapper.name();
				}
				columns.add(name);
				values.add(value);
			}
		});
		String sql = Insert.table(getTableName(t, tableName)).columns(columns);
		if (showSql) {
			logger.debug("Insert SQL: {}", sql);
		}
		jdbcTemplate.update(sql, values.toArray());
	}

	public <T> Long saveAndGetId(T t) {
		List<Object> values = new ArrayList<Object>();
		List<String> columns = new ArrayList<String>();
		ReflectionUtils.doWithFields(t.getClass(), new FieldCallback() {
			@Override
			public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
				if (!field.isAccessible() ) {
					field.setAccessible(true);
				}
				Object value = ReflectionUtils.getField(field, t);
				if (value == null) {
					return;
				}
				Mapper mapper = field.getAnnotation(Mapper.class);
				String name = field.getName();
				if (mapper == null) {
					name = field.getName();
				} else if (mapper.ignore()) {
					return;
				} else if (StringUtils.isNotBlank(mapper.name())) {
					name = mapper.name();
				}
				columns.add(name);
				values.add(value);
			}
		});
		String sql = Insert.table(getTableName(t, tableName)).columns(columns);
		if (showSql) {
			logger.debug("Insert SQL: {}", sql);
		}
		return saveAndGetId(sql, values.toArray());
	}

	/**
	 * 仅适用于 数据库主键自动增长 类型的表
	 * @param sql insert语句
	 * @param values 值
	 * @return Database auto increment number
	 */
	@Transactional
	public Long saveAndGetId(final String sql, final Object...values) {
		KeyHolder holder = new GeneratedKeyHolder();
		jdbcTemplate.update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
				for (int i = 0; i < values.length; i++) {
					ps.setObject(i + 1, values[i]);
				}
                return ps;
			}
		}, holder);
		return holder.getKey().longValue();
	}

	/**
	 * 调用此方法前应当先查询数据库，确保对象是最新的．否则保存的数据可能出现偏差.
	 * @param <T> Entity Object
	 * @param t Entity object
	 */
	@Transactional
	public <T> void updateByPK(T t, boolean ignoreNull) {
		List<Object> values = new ArrayList<Object>();
		List<String> columns = new ArrayList<String>();
		List<Object> pkValues = new ArrayList<Object>();
		List<String> pkColumns = new ArrayList<String>();
		Mapper classMapper = t.getClass().getAnnotation(Mapper.class);
		String[] pks = classMapper.primaryKey();
		if (pks == null || pks.length < 1) {
			throw new IllegalStateException("Mapper#primaryKey annotation not found at class: " + t.getClass());
		}
		ReflectionUtils.doWithFields(t.getClass(), new FieldCallback() {
			@Override
			public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
				Mapper mapper = field.getAnnotation(Mapper.class);
				String name = field.getName();
				if (mapper == null) {
				} else if (mapper.ignore()) {
					return;
				} else if (StringUtils.isNotBlank(mapper.name())) {
					name = mapper.name();
				}
				if (!field.isAccessible() ) {
					field.setAccessible(true);
				}
				Object v = ReflectionUtils.getField(field, t);
				if (ignoreNull && v == null) {
					return;
				}
				if (StringUtils.contain(pks, name)) {
					pkColumns.add(name);
					pkValues.add(v);
					return;
				}
				columns.add(name);
				values.add(v);
			}
		});
		String sql = Update.table(getTableName(t, tableName)).setColumns(columns).whereColumns(pkColumns);
		values.addAll(pkValues);
		if (showSql) {
			logger.debug("Update SQL: {}", sql);
		}
		jdbcTemplate.update(sql, values.toArray());
	}
	
	/**
	 * Don't ignore null. Before doing update, please get the entity from database.
	 */
	@Transactional
	public <T> void updateByPK(T t) {
		updateByPK(t, false);
	}

	public void update(String sql, Object...values) {
		jdbcTemplate.update(sql, values);
	}

	public void update(Columns columns, Restriction...restrictions) {
		String sql = Update.table(tableName).set(columns.getSetPhrase()).whereColumns(Restriction.whereSql(restrictions));
		List<Object> values = columns.getSetValues();
		values.addAll(Restriction.whereValueList(restrictions));
		if (showSql) {
			logger.debug("Update SQL: {}", sql);
		}
		jdbcTemplate.update(sql, values.toArray());
	}

	@Transactional
	public <T> void deleteByPK(T t) {
		List<Object> pkValues = new ArrayList<Object>();
		List<String> pkColumns = new ArrayList<String>();
		Mapper classMapper = t.getClass().getAnnotation(Mapper.class);
		String[] pks = classMapper.primaryKey();
		if (pks == null || pks.length < 1) {
			throw new IllegalStateException("Mapper#primaryKey annotation not found at class: " + t.getClass());
		}
		ReflectionUtils.doWithFields(t.getClass(), new FieldCallback() {
			@Override
			public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
				Mapper mapper = field.getAnnotation(Mapper.class);
				String name = field.getName();
				if (mapper == null) {
				} else if (mapper.ignore()) {
					return;
				} else if (StringUtils.isNotBlank(mapper.name())) {
					name = mapper.name();
				}
				if (!field.isAccessible() ) {
					field.setAccessible(true);
				}
				if (StringUtils.contain(pks, name)) {
					pkColumns.add(name);
					pkValues.add(ReflectionUtils.getField(field, t));
					return;
				}
			}
		});
		String sql = Delete.table(getTableName(t, tableName)).whereColumns(pkColumns);
		if (showSql) {
			logger.debug("Update SQL: {}", sql);
		}
		jdbcTemplate.update(sql, pkValues.toArray());
	}

	/**
	 * 仅适用于 数据库主键自动增长 类型的表
	 * @param id primary key
	 */
	public void delete(Long id) {
		String sql = "delete from " + tableName + " where id = ?";
		if (showSql) {
			logger.debug("Delete SQL: {}", sql);
		}
		jdbcTemplate.update(sql, id);
	}

	private String getTableName(Object mapperObj, String tableName) {
		if (mapperObj instanceof RsMapper) {
			Mapper tmapper = ((RsMapper<?>) mapperObj).getMapper();
			return getTableName(tmapper, tableName);
		}
		Mapper tmapper = mapperObj.getClass().getAnnotation(Mapper.class);
		return getTableName(tmapper, tableName);
	}
	
	private String getTableName(Mapper mapper, String tableName) {
		if (mapper == null) {
			return tableName;
		}
		if (StringUtils.isNotBlank(mapper.tableName())) {
			return mapper.tableName().trim();
		}
		return tableName;
	} 
}
