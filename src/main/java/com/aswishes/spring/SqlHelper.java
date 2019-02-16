package com.aswishes.spring;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqlHelper {

	public static Insert insert(String tableName) {
		return Insert.table(tableName);
	}

	public static Delete delete(String tableName) {
		return Delete.table(tableName);
	}

	public static Select select(String tableName) {
		return Select.table(tableName);
	}

	public static Update update(String tableName) {
		return Update.table(tableName);
	}

	public static class Insert {
		private StringBuilder sql = new StringBuilder("insert into ");
		public static Insert table(String tableName) {
			return new Insert().tableName(tableName);
		}
		private Insert tableName(String tableName) {
			sql.append(tableName);
			return this;
		}
		public String columns(List<String> columns) {
			sql.append("(");
			String names = Restriction.join(columns, ",");
			String values = Restriction.repeat("?", ", ", columns.size());
			sql.append(names);
			sql.append(") values (");
			sql.append(values);
			sql.append(")");
			return sql.toString();
		}
		/**
		 * 指定insert语句需要插入的字段名称列表
		 * @param columns 字段名称列表．如：["name", "age", "birthday"]
		 * @return 最终sql语句
		 */
		public String columns(String... columns) {
			return columns(Arrays.asList(columns));
		}
		/**
		 * 指定insert语句需要插入的字段名称列表
		 * @param phrase 字段名称列表．如： "name,age,birthday"
		 * @return 最终sql语句
		 */
		public String columns(String phrase) {
			String[] ss = phrase.split(",");
			return columns(ss);
		}
	}

	public static class Delete {
		private StringBuilder sql = new StringBuilder("delete from ");
		public static Delete table(String tableName) {
			return new Delete().tableName(tableName);
		}
		private Delete tableName(String tableName) {
			sql.append(tableName).append(" ");
			return this;
		}
		public String whereColumns(List<String> columns) {
			if (columns == null || columns.size() < 1) {
				return sql.toString();
			}
			sql.append("where ").append(Restriction.join(columns, " = ? and ", " = ?"));
			return sql.toString();
		}
		/**
		 * 设置delete语句的where条件．只需设置条件列名称
		 * @param columns 条件列名称．如："name", "age", "birthday"
		 * @return 最终sql语句
		 */
		public String whereColumns(String... columns) {
			return whereColumns(Arrays.asList(columns));
		}
		/**
		 * 设置delete语句的where条件．该方法会自动添加where关键字，并附加空格
		 * @param where 条件语句．如：name = ? and sex = ? and birthday = ?
		 * @return 最终的delete语句
		 */
		public String where(String where) {
			if (where == null || "".equals(where.trim())) {
				return sql.toString();
			}
			sql.append("where ").append(where);
			return sql.toString();
		}
		public String toSqlString() {
			return sql.toString();
		}
	}

	public static class Select {
		private StringBuilder sql = new StringBuilder();
		private String tableName;
		private String countColumns = "*";
		private String columns = "*";
		private boolean useCount = false;
		public static Select table(String tableName) {
			Select select = new Select();
			select.tableName = tableName;
			return select;
		}
		public Select count(String columns) {
			useCount = true;
			this.countColumns = columns;
			return this;
		}
		/**
		 * 设置select语句的输出结果列．
		 * @param columns 输出结果列表．如："name, age, birthday"
		 * @return Select对象
		 */
		public Select columns(String columns) {
			this.columns = columns;
			return this;
		}
		/**
		 * 设置select语句的输出结果列．
		 * @param columns 输出结果列表．如：["name", "age", "birthday"]
		 * @return Select对象
		 */
		public Select columns(String... columns) {
			this.columns = Restriction.join(Arrays.asList(columns), ",");
			return this;
		}
		public Select leftJoin(String tableName) {
			sql.append("left join ").append(tableName).append(" ");
			return this;
		}
		public Select rightJoin(String tableName) {
			sql.append("right join ").append(tableName).append(" ");
			return this;
		}
		public Select innerJoin(String tableName) {
			sql.append("inner join ").append(tableName).append(" ");
			return this;
		}
		public Select on(String matchColumn) {
			sql.append("on ").append(matchColumn).append(" ");
			return this;
		}
		public Select where(Restriction...restrictions) {
			if (restrictions == null || restrictions.length < 1) {
				return this;
			}
			String str = Restriction.whereSql(restrictions);
			if (str.startsWith("order by")) {
				sql.append(str);
			} else {
				sql.append("where ").append(str);
			}
			return this;
		}
		public Select where(String phrase) {
			if (phrase == null || "".equals(phrase.trim())) {
				return this;
			}
			sql.append("where ").append(phrase);
			return this;
		}
		public Select groupBy(String columns) {
			sql.append("group by ").append(columns).append(" ");
			return this;
		}
		public Select having(String express) {
			sql.append("having ").append(express).append(" ");
			return this;
		}
		public String toCountString() {
			StringBuilder r = new StringBuilder();
			r.append("select count(").append(countColumns).append(") from ").append(tableName).append(" ");
			if (sql.length() > 0) {
				r.append(sql).append(" ");
			}
			return r.toString();
		}
		public String toSqlString() {
			StringBuilder r = new StringBuilder();
			r.append("select ").append(columns).append(" from ").append(tableName).append(" ");
			if (sql.length() > 0) {
				r.append(sql).append(" ");
			}
			return r.toString();
		}
		@Override
		public String toString() {
			if (useCount == true) {
				return toCountString();
			}
			return toSqlString();
		}
	}

	public static class Update {
		private StringBuilder sql = new StringBuilder("update ");
		public static Update table(String tableName) {
			Update update = new Update();
			update.sql.append(tableName).append(" ").append("set ");
			return update;
		}
		/**
		 * 设置update语句中需要更新的列
		 * @param columns 列名称．不需要带问号.如：["name", "age", "sex"]
		 * @return 当前Update语句对象
		 */
		public Update setColumns(List<String> columns) {
			if (columns == null || columns.size() < 1) {
				throw new IllegalStateException("Column list can not be empty.");
			}
			sql.append(Restriction.join(columns, " = ?, ", " = ? "));
			return this;
		}
		/**
		 * 设置update语句中需要更新的列
		 * @param columns 列名称．不需要带问号.如：["name", "age", "sex"]
		 * @return 当前Update语句对象
		 */
		public Update setColumns(String... columns) {
			return setColumns(Arrays.asList(columns));
		}
		/**
		 * 设置update语句中需要更新的列
		 * @param phrase 拼写的列语句．如："name = ?, age = ?, birthday = ?"
		 * @return 当前Update语句对象
		 */
		public Update set(String phrase) {
			sql.append(phrase).append(" ");
			return this;
		}
		/**
		 * 设置update语句中条件列
		 * @param columns 条件列名称．不需要带问号.如：["name", "age", "sex"]
		 * @return 最终sql语句
		 */
		public String whereColumns(List<String> columns) {
			if (columns == null || columns.size() < 1) {
				throw new IllegalStateException("Column list can not be empty.");
			}
			sql.append("where ").append(Restriction.join(columns, " = ? and ", " = ?"));
			return sql.toString();
		}
		/**
		 * 设置update语句中条件列
		 * @param columns 条件列名称．不需要带问号.如：["name", "age", "sex"]
		 * @return 最终sql语句
		 */
		public String whereColumns(String... columns) {
			return whereColumns(Arrays.asList(columns));
		}
		/**
		 * 设置条件语句．若条件语句不为空，该方法会自动产生一个where前缀
		 * @param where 条件语句．如："name = ? and age = ? and sex = ?"
		 * @return 最终sql语句
		 */
		public String where(String where) {
			if (where == null || "".equals(where.trim())) {
				return sql.append(" ").toString();
			}
			sql.append("where ").append(where).append(" ");
			return sql.toString();
		}
		public String toSqlString() {
			return sql.toString();
		}
	}

	public static class Columns {
		private StringBuilder sets = new StringBuilder();
		private List<Object> setValues = new ArrayList<Object>();

		public static Columns create(String column, Object value) {
			return new Columns().set(column, value);
		}

		public Columns set(String column, Object value) {
			sets.append(setValues.size() > 0 ? ", " : "").append(column).append(" = ?");
			setValues.add(value);
			return this;
		}

		/**
		 * 获取set短语
		 * @return set部分语句．如: name = ?, age = ?
		 */
		public String getSetPhrase() {
			return sets.toString();
		}

		public List<Object> getSetValues() {
			return setValues;
		}

	}
}
