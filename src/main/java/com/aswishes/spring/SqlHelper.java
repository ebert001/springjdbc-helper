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
		public String columns(String... columns) {
			return columns(Arrays.asList(columns));
		}
		/**
		 * @param phrase Such as: name,age,birthday
		 * @return insert SQL
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
		public String where(List<String> columns) {
			if (columns == null || columns.size() < 1) {
				return sql.toString();
			}
			sql.append("where ").append(Restriction.join(columns, " = ? and ", " = ?"));
			return sql.toString();
		}
		public String where(String... columns) {
			return where(Arrays.asList(columns));
		}
		public String where(String where) {
			if (where == null || "".equals(where.trim())) {
				return sql.toString();
			}
			sql.append("where ").append(where);
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
		public Select columns(String columns) {
			this.columns = columns;
			return this;
		}
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
			sql.append("where ").append(Restriction.whereSql(restrictions));
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
		public Update set(List<String> columnList) {
			if (columnList == null || columnList.size() < 1) {
				throw new IllegalStateException("Column list can not be empty.");
			}
			sql.append(Restriction.join(columnList, " = ?, ", " = ? "));
			return this;
		}
		public Update set(String... columns) {
			return set(Arrays.asList(columns));
		}
		public Update set(String phrase) {
			sql.append(phrase).append(" ");
			return this;
		}
		public String where(List<String> columnList) {
			if (columnList == null || columnList.size() < 1) {
				throw new IllegalStateException("Column list can not be empty.");
			}
			sql.append("where ").append(Restriction.join(columnList, " = ? and ", " = ?"));
			return sql.toString();
		}
		public String where(String... columns) {
			return where(Arrays.asList(columns));
		}
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

		public String getSetPhrase() {
			return sets.toString();
		}

		public List<Object> getSetValues() {
			return setValues;
		}

	}
}
