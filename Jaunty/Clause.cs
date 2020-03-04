// ﷽

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using static Jaunty.Jaunty;

namespace Jaunty
{
	public enum JoinType
	{
		InnerJoin = 1,
		LeftOuterJoin = 2,
		RightOuterJoin = 3
	}

	internal class WhereClause
	{
		internal string Column { get; private set; }
		internal ComparisonOperator Oper { get; private set; }
		internal object Value { get; private set; }
		internal Separator Separator { get; private set; }

		internal void Add<T>(string column, ComparisonOperator oper, T value)
		{
			Column = column;
			Oper = oper;
			Value = value;
		}

		internal void Add(Separator separator)
		{
			Separator = separator;
		}

		internal string ToSql()
		{
			return ToString();
		}

		public override string ToString()
		{
			var column = Column.Replace("#", ".", StringComparison.OrdinalIgnoreCase);
			var parameter = SqlDialect == Dialects.Postgres ? column : Column.Replace(".", "__", StringComparison.OrdinalIgnoreCase);
			column = column.Replace("$", "", StringComparison.OrdinalIgnoreCase);
			column = ColumnNameFormatter?.Invoke(column) ?? column;
			parameter = ParameterFormatter?.Invoke(parameter) ?? $"@{parameter}";
			return Separator == Separator.Empty ? $"{column} {Oper.ToSqlString()} {parameter}"
				: $"{column} {Oper.ToSqlString()} {parameter} {Separator.ToSqlString()}";
		}
	}

	public abstract class Clause
	{
		protected Clause(IDbConnection connection)
		{
			Connection = connection ?? throw new ArgumentNullException(nameof(connection));
		}

		protected Clause(Clause clause) : this(clause?.Connection)
		{
			PreviousClause = clause ?? throw new ArgumentNullException(nameof(clause));
		}

		internal IDbConnection Connection { get; }
		internal Clause PreviousClause { get; set; }

		internal abstract string Sql { get; }
	}

	//public class Select : Clause
	//{
	//	private string sql;

	//	public Select(IDbConnection connection, Type entity, string alias = null)
	//		: base(connection)
	//	{
	//		Entity = entity ?? throw new ArgumentNullException(nameof(entity));
	//		Alias = alias;
	//	}

	//	public Select(IDbConnection connection, string clause)
	//		: base(connection)
	//	{
	//		Clause = clause ?? throw new ArgumentNullException(nameof(clause));
	//	}

	//	internal Type Entity { get; }
	//	internal string Alias { get; private set; }
	//	internal string Clause { get; }
	//	internal bool FullyQualify { get; set; }

	//	internal override string Sql
	//	{
	//		get
	//		{
	//			if (sql.IsNullOrEmpty())
	//			{
	//				sql = Clause.IsNullOrWhiteSpace() ? $"SELECT {GetColumns()} " : $"SELECT {Clause} ";
	//			}

	//			return sql;
	//		}
	//	}

	//	private string GetColumns()
	//	{
	//		var columnNames = new List<string>(GetColumnsCache(Entity).Keys);

	//		if (!FullyQualify)
	//		{
	//			return string.Join(", ", columnNames);
	//		}

	//		Alias ??= GetTypeName(Entity);
	//		var prependedColumns = columnNames.Prepend(Alias + ".");
	//		return string.Join(", ", prependedColumns);
	//	}
	//}

	public class Distinct : Clause
	{
		private string sql;

		public Distinct(IDbConnection connection)
			:base(connection)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));
		}

		internal override string Sql
		{
			get
			{
				if (sql.IsNullOrEmpty())
				{
					sql = "DISTINCT";
				}

				return sql;
			}
		}
	}

	public class Top : Clause
	{
		private string sql;

		public Top(IDbConnection connection, int count) : base(connection)
		{
			Count = count;
		}

		internal int Count { get; }

		internal override string Sql
		{
			get
			{
				if (sql.IsNullOrEmpty())
				{
					sql = PreviousClause != null
						? $"{PreviousClause.Sql} TOP {Count}"
						: $"TOP {Count}";
				}

				return sql;
			}
		}
	}

	public class From : Clause
	{
		private string sql;

		public From(IDbConnection connection, Type entity, string alias = null)
			: base(connection)
		{
			Entity = entity ?? throw new ArgumentNullException(nameof(entity));
			Alias = alias;
		}

		public From(Clause clause, Type entity, string alias = null)
			: this(clause?.Connection, entity, alias)
		{
			PreviousClause = clause ?? throw new ArgumentNullException(nameof(clause));
		}

		internal Type Entity { get; }
		internal string Name => GetTypeName(Entity);
		internal string Alias { get; }

		internal override string Sql
		{
			get
			{
				if (sql.IsNullOrEmpty())
				{
					sql = PreviousClause != null
						? $"{PreviousClause.Sql} FROM {Name}{(Alias.NotNullOrWhiteSpace() ? " " + Alias : "")}"
						: $"FROM {Name}{(Alias.NotNullOrWhiteSpace() ? " " + Alias : "")}";
				}

				return sql;
			}
		}
	}

	public class Join : Clause
	{
		public Join(Clause clause, Type entity, string alias = null, JoinType joinType = JoinType.InnerJoin)
			: base(clause)
		{
			Entity = entity ?? throw new ArgumentNullException(nameof(entity));
			Alias = alias;
			JoinType = joinType;
			//SetFullyQualify(true);
		}

		internal Type Entity { get; }
		internal string Name => GetTypeName(Entity);
		internal string Alias { get; }
		internal JoinType JoinType { get; }

		internal override string Sql => $"{PreviousClause.Sql} {JoinType.ToSqlString()} {Name}{(Alias.NotNullOrWhiteSpace() ? " " + Alias : "")}";

		//private void SetFullyQualify(bool isQualified)
		//{
		//	while (PreviousClause != null)
		//	{
		//		if (PreviousClause is Select select)
		//		{
		//			select.FullyQualify = isQualified;
		//		}

		//		PreviousClause = PreviousClause.PreviousClause;
		//	}
		//}
	}

	public class JoinOn : Clause
	{
		private string sql;

		public JoinOn(Clause clause, string column1, string column2) : base(clause)
		{
			if (string.IsNullOrWhiteSpace(column1))
			{
				throw new ArgumentNullException(nameof(column1));
			}

			if (string.IsNullOrWhiteSpace(column2))
			{
				throw new ArgumentNullException(nameof(column2));
			}

			Column1 = column1;
			Column2 = column2;
		}

		internal string Column1 { get; }
		internal string Column2 { get; }

		internal override string Sql
		{
			get
			{
				if (sql.IsNullOrEmpty())
				{
					sql = $"{PreviousClause.Sql} ON {Column1} = {Column2}";
				}

				return sql;
			}
		}
	}

	public class Condition : Clause
	{
		private string sql;

		internal readonly List<WhereClause> whereClauses;

		internal Condition(Clause clause) : base(clause)
		{
			whereClauses = new List<WhereClause>();
		}

		internal Condition(PartialConditionalClause clause) : base(clause)
		{
			whereClauses = new List<WhereClause>();
		}

		internal void Add<T>(string column, ComparisonOperator oper, T value)
		{
			if (string.IsNullOrWhiteSpace(column))
				return;

			var setClause = PreviousClause as SetClause ?? PreviousClause.PreviousClause as SetClause;

			if (setClause != null && setClause.Sets.ContainsKey(column))
			{
				column += "$";
			}

			//column = column.Replace(".", "#");
			var whereClause = new WhereClause();
			whereClause.Add(column, oper, value);
			whereClauses.Add(whereClause);
		}

		internal void Add(Separator separator)
		{
			whereClauses.Last().Add(separator);
		}

		internal void Add<T>(Expression<Func<T, bool>> expression)
		{
			var setClause = PreviousClause as SetClause;
			expression.Body.WalkThrough((n, o, v) =>
			{
				if (n is null && v is null)
				{
					Add(o.ToSeparator());
				}

				Add(n, o.ToComparisonOperator(), v);
			});
		}

		internal Dictionary<string, object> GetParameters()
		{
			var dict = new Dictionary<string, object>();
			var setClause = PreviousClause as SetClause ?? PreviousClause.PreviousClause as SetClause;

			if (setClause != null)
			{
				var sets = setClause.Sets;

				for (int i = 0; i < sets.Count; i++)
				{
					dict.Add(sets.ElementAt(i).Key, sets.ElementAt(i).Value);
				}
			}

			for (int i = 0; i < whereClauses.Count; i++)
			{
				var column = SqlDialect == Dialects.Postgres
					? whereClauses[i].Column
					: whereClauses[i].Column.Replace(".", "__", StringComparison.CurrentCultureIgnoreCase);
				dict.Add(column, whereClauses[i].Value);
			}

			return dict;
		}

		internal string GetSetClause()
		{
			return ((SetClause)PreviousClause).ToString();
		}

		internal override string Sql
		{
			get
			{
				if (sql.IsNullOrEmpty())
				{
					var sb = new StringBuilder();
					sb.Append(PreviousClause.Sql + " ");
					sb.Append("WHERE ");

					for (int i = 0; i < whereClauses.Count; i++)
					{
						sb.Append(whereClauses[i].ToSql());

						if (i < whereClauses.Count - 1)
						{
							sb.Append(" ");
						}
					}

					sql = sb.ToString();
				}
				
				return sql;
			}
		}
	}

	public class PartialConditionalClause : Clause
	{
		public PartialConditionalClause(Condition conditionClause) : base(conditionClause)
		{
		}

		internal string Column { get; private set; }
		internal object Value { get; private set; }

		internal void AddColumn(string column)
		{
			if (string.IsNullOrWhiteSpace(column))
				throw new ArgumentNullException(nameof(column));

			if (PreviousClause.PreviousClause is SetClause setClause)
			{
				if (setClause.Sets.ContainsKey(column))
				{
					column += "$";
				}
			}

			Column = column;
		}

		internal override string Sql => "";
	}

	public class AndClause : Clause
	{
		public AndClause(Clause clause) : base(clause)
		{
		}

		internal override string Sql => "AND";
	}

	public class OrClause : Clause
	{
		public OrClause(Clause clause) : base(clause)
		{
		}

		internal override string Sql => "OR";
	}

	public class SetClause : Clause
	{
		public SetClause(IDbConnection connection) : base(connection)
		{
			Sets = new Dictionary<string, object>();
		}

		internal IDictionary<string, object> Sets { get; private set; }

		internal void Add<TValue>(string column, TValue value)
		{
			if (string.IsNullOrWhiteSpace(column))
				return;

			//column = column.Replace(".", "#");
			Sets.Add(column, value);
		}

		internal string ToSetClause()
		{
			return ToString();
		}

		public override string ToString()
		{
			return Sets.ToSetClause();
		}

		internal override string Sql => "";
	}

	public class GroupBy : Clause
	{
		public GroupBy(Clause clause) : base(clause)
		{
		}

		internal IList<string> GroupBys { get; private set; }

		internal void Add(params string[] columns)
		{
			GroupBys = GroupBys ?? new List<string>(columns);
		}

		internal override string Sql => $"{PreviousClause.Sql} GROUP BY {string.Join(", ", GroupBys)}";
	}

	public class Having : Clause
	{
		public Having(Clause clause) : base(clause)
		{
			Havings = new List<string>();
		}

		internal IList<string> Havings { get; private set; }

		internal void Add(params string[] havings)
		{
			// todo 
		}

		internal override string Sql => "";
	}

	public class OrderBy : Clause
	{
		public OrderBy(Clause clause) : base(clause)
		{
			OrderBys = new Dictionary<string, SortOrder?>();
		}

		internal IDictionary<string, SortOrder?> OrderBys { get; private set; }

		internal void Add(string column, SortOrder? sortOrder)
		{
			if (!string.IsNullOrWhiteSpace(column))
			{
				OrderBys.Add(column, sortOrder);
			}
		}

		internal override string Sql
		{
			get
			{
				var sb = new StringBuilder(PreviousClause.Sql + " ");

				for (int i = 0; i < OrderBys.Count; i++)
				{
					KeyValuePair<string, SortOrder?> kvp = OrderBys.ElementAt(i);
					sb.AppendIf(i == 0, "ORDER BY ");
					sb.Append($"{kvp.Key}");
					sb.AppendIf(kvp.Value.HasValue, $" {(kvp.Value == SortOrder.Descending ? "DESC" : "ASC")}");
					sb.AppendIf(i < OrderBys.Count - 1, ", ");
				}

				return sb.ToString();
			}
		}
	}

	public class Limit : Clause
	{
		public Limit(Clause clause, int count) : base(clause)
		{
			Count = count;
		}

		internal int Count { get; private set; }

		internal override string Sql => $"{PreviousClause.Sql} LIMIT {Count}";
	}

	public class Offset : Clause
	{
		public Offset(Clause clause, int number) : base(clause)
		{
			Number = number;
		}

		internal int Number { get; private set; }

		internal override string Sql => $"{PreviousClause.Sql} OFFSET {Number}{(SqlDialect == Dialects.SqlServer ? " ROWS" : "")}";
	}

	public class FetchFirst : Clause
	{
		public FetchFirst(Clause clause, int rowCount) : base(clause)
		{
			RowCount = rowCount;
		}

		internal int RowCount { get; private set; }

		internal override string Sql => $"{PreviousClause.Sql} FETCH FIRST {RowCount} ROWS ONLY";
	}

	public class FetchNext : Clause
	{
		public FetchNext(Clause clause, int rowCount) : base(clause)
		{
			RowCount = rowCount;
		}

		internal int RowCount { get; private set; }

		internal override string Sql => $"{PreviousClause.Sql} FETCH NEXT {RowCount} ROWS ONLY";
	}
}
