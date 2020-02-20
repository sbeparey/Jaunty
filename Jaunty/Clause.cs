using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Jaunty
{
	public static partial class Jaunty
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

		//public interface ISqlClause
		//{
		//	string Sql { get; }
		//}

		public abstract class Clause
		{
			protected Clause(IDbConnection connection)
			{
				Connection = connection ?? throw new ArgumentNullException(nameof(connection));
			}

			protected Clause(Clause clause) : this(clause.Connection)
			{
				PreviousClause = clause ?? throw new ArgumentNullException(nameof(clause));
			}

			internal IDbConnection Connection { get; }
			internal Clause PreviousClause { get; set; }

			internal abstract string Sql { get; }
		}

		public class FromClause : Clause
		{
			public FromClause(IDbConnection connection, Type entityType, string alias = null)
				: base(connection)
			{
				if (entityType is null)
				{
					throw new ArgumentNullException(nameof(entityType));
				}

				EntityType = entityType;
				Alias = alias;
			}

			public FromClause(Clause clause, Type entityType, string alias = null)
				: this(clause?.Connection, entityType, alias)
			{
				PreviousClause = clause ?? throw new ArgumentNullException(nameof(clause));
			}

			internal Type EntityType { get; }
			internal string Name => GetTableName(EntityType);
			internal string Alias { get; }

			internal override string Sql => !string.IsNullOrWhiteSpace(Alias)
														? $"FROM {Name} {Alias}"
														: $"FROM {Name}";
		}

		public class JoinClause : Clause
		{
			public JoinClause(Clause clause, Type entityType, string alias = null, JoinType joinType = JoinType.InnerJoin)
				: base(clause)
			{
				if (entityType is null)
					throw new ArgumentNullException(nameof(entityType));

				EntityType = entityType;
				Alias = alias;
				JoinType = joinType;
			}

			internal Type EntityType { get; }
			internal string Name => GetTableName(EntityType);
			internal string Alias { get; }
			internal JoinType JoinType { get; }

			internal override string Sql => !string.IsNullOrEmpty(Alias)
														? $"{JoinType.ToSqlString()} {Name} {Alias}"
														: $"{JoinType.ToSqlString()} {Name}";
		}

		public class JoinOnClause : Clause
		{
			public JoinOnClause(Clause clause, string column1, string column2) : base(clause)
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

			internal override string Sql => $"ON {Column1} = {Column2}";
		}

		public class ConditionalClause : Clause
		{
			internal readonly List<WhereClause> whereClauses;

			internal ConditionalClause(Clause clause) : base(clause)
			{
				whereClauses = new List<WhereClause>();
			}

			internal ConditionalClause(PartialConditionalClause clause) : base(clause)
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

			internal string ToWhereClause()
			{
				var sb = new StringBuilder();
				sb.Append("WHERE ");

				for (int i = 0; i < whereClauses.Count; i++)
				{
					sb.Append(whereClauses[i].ToSql());

					if (i < whereClauses.Count - 1)
					{
						sb.Append(" ");
					}
				}

				return sb.ToString();
			}

			internal override string Sql => ToWhereClause();
		}

		public class PartialConditionalClause : Clause
		{
			public PartialConditionalClause(ConditionalClause conditionClause) : base(conditionClause)
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

		public class GroupByClause : Clause
		{
			public GroupByClause(Clause clause) : base(clause)
			{
			}

			internal IList<string> GroupBys { get; private set; }

			internal void Add(params string[] columns)
			{
				GroupBys = GroupBys ?? new List<string>(columns);
			}

			internal override string Sql => $"GROUP BY {string.Join(", ", GroupBys)}";
		}

		public class HavingClause : Clause
		{
			public HavingClause(Clause clause) : base(clause)
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

		public class OrderByClause : Clause
		{
			public OrderByClause(Clause clause) : base(clause)
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
					var sb = new StringBuilder();

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

		public class LimitClause : Clause
		{
			public LimitClause(Clause clause, int limit) : base(clause)
			{
				Limit = limit;
			}

			internal int Limit { get; private set; }

			internal override string Sql => $"LIMIT {Limit}";
		}

		public class TopClause : Clause
		{
			public TopClause(IDbConnection connection, int top) : base(connection)
			{
				Top = top;
			}

			internal int Top { get; private set; }

			internal override string Sql => $"TOP {Top}";
		}
	}
}
