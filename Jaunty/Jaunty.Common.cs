// ﷽

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Jaunty
{
	public static partial class Jaunty
	{
		private static string ExtractInsert<T>(Clause clause)
		{
			var type = GetType(typeof(T));
			string sql = clause.ToSql();

			string entity = GetTypeName(type);
			var nonKeyColumns = GetNonKeyColumnsCache(type);
			string columns = nonKeyColumns.ToClause();
			string values = nonKeyColumns.ToClause(prefix: "@");
			sql = sql.Replace("{{table}}", entity, StringComparison.OrdinalIgnoreCase)
					 .Replace("{{columns}}", columns, StringComparison.OrdinalIgnoreCase)
					 .Replace("{{values}}", values, StringComparison.OrdinalIgnoreCase);
			return sql;
		}

		private static string ExtractSql<T>(ClauseType clauseType, Clause clause, string alias = null, string selectClause = null)
		{
			if (clauseType == ClauseType.Insert)
				return ExtractInsert<T>(clause);

			var type = GetType(typeof(T));
			string sql = clause.ToSql();
			string selectedAlias = alias;

			bool hasJoin = false;
			bool hasTop = false;
			bool hasDistinct = false;

			while (clause != null)
			{
				hasJoin = hasJoin || clause is Join;
				hasTop = hasTop || clause is Top;
				hasDistinct = hasDistinct || clause is Distinct;
				selectedAlias ??= GetSelectedAlias(clause, type);
				clause = clause.PreviousClause;
			}

			var builder = new StringBuilder();

			switch (clauseType)
			{
				case ClauseType.Select:
					builder.Append("SELECT ");
					break;
				case ClauseType.Delete:
					builder.Append("DELETE ");
					break;
				case ClauseType.Update:
					builder.Append("UPDATE ");
					break;
			}

			string entity = GetTypeName(type);
			string columns = selectClause;

			switch (clauseType)
			{
				case ClauseType.Select:
					columns ??= GetFormattedColumns(type, selectedAlias ?? (hasJoin ? entity : null));
					break;
				default:
					break;
			}

			if (hasDistinct || hasTop)
			{
				sql = sql.InsertBefore("FROM", columns + " ");
				builder.Append(sql);
			}
			else
			{
				switch (clauseType)
				{
					case ClauseType.Select:
						builder.Append(columns + " " + sql);
						break;
					case ClauseType.Delete:
						builder.Append(sql);
						break;
					case ClauseType.Update:
						builder.Append(entity + " " + sql);
						break;
				}
			}

			builder.Append(";");
			return builder.ToString();
		}

		private static string GetSelectedAlias(Clause clause, Type selectedType)
		{
			switch (clause)
			{
				case From from when from.Entity.Equals(selectedType):
					return from.Alias;
				case Join join when join.Entity.Equals(selectedType):
					return join.Alias;
				default:
					break;
			}

			return null;
		}

		private static string GetFormattedColumns(Type type, string alias)
		{
			var columnNames = new List<string>(GetColumnsCache(type).Keys);

			if (!string.IsNullOrWhiteSpace(alias))
			{
				var prependedColumns = columnNames.Prepend(alias + ".");
				return string.Join(", ", prependedColumns);
			}

			return string.Join(", ", columnNames);
		}

		private static Dictionary<string, object> KeyToParameter<T, TKey>(TKey key)
		{
			Type type = GetType(typeof(T));
			var keyColumnsList = GetKeysCache(type).Keys.ToList();

			if (keyColumnsList.Count == 0)
			{
				throw new ArgumentException("This entity does not have any key columns.");
			}

			if (keyColumnsList.Count > 1)
			{
				throw new ArgumentException("This entity has more than one key columns.");
			}

			var parameters = new Dictionary<string, object>();
			parameters.Add(keyColumnsList[0], key);
			return parameters;
		}

		private static Dictionary<string, object> KeysToParameters<T, TKey1, TKey2>(TKey1 key1, TKey2 key2)
		{
			Type type = GetType(typeof(T));
			var keys = GetKeysCache(type).Keys.ToList();

			if (keys.Count != 2)
			{
				throw new ArgumentException("This entity does not have two key columns.");
			}

			var parameters = new Dictionary<string, object>();
			parameters.Add(keys[0], key1);
			parameters.Add(keys[1], key2);
			return parameters;
		}

		private static Dictionary<string, object> ExpressionToParameters<T>(Expression<Func<T, bool>> expression)
		{
			if (expression is null)
				throw new ArgumentNullException(nameof(expression));

			Type type = GetType(typeof(T));
			var dictionary = new Dictionary<string, object>();
			expression.Body.WalkThrough((name, _, value) => dictionary.AddIf(name != null, name, value));
			return dictionary;
		}

		private static string BuildSelectAllSql<T>()
		{
			Type type = GetType(typeof(T));
			var columns = GetColumnsCache(type).Keys.ToList();
			string sql = SqlTemplates.Select.Replace("{{columns}}", columns.ToClause(), StringComparison.OrdinalIgnoreCase)
											.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase);
			sql += ";";
			return sql;
		}

		private static string BuildSql<T>(ClauseType clauseType, Expression<Func<T, bool>> expression)
		{
			Type type = GetType(typeof(T));
			var columns = GetColumnsCache(type).Keys.ToList();
			var where = new StringBuilder();
			expression.Body.WalkThrough((name, oper, _) =>
			{
				where.AppendIf(name != null, $"{name} {oper} @{name}");
				where.AppendIf(name is null, $" {oper} ");
			});

			return BuildSql(clauseType, GetTypeName(type), columns.ToClause(), where.ToString());
		}

		private static string BuildSql<T>(ClauseType clauseType, IDictionary<string, object> parameters)
		{
			Type type = GetType(typeof(T));
			var keys = GetKeysCache(type).Keys.ToList();
			var columns = GetColumnsCache(type).Keys.ToList();

			if (clauseType == ClauseType.Update)
			{
				columns.Reduce(keys);
				return BuildSql(clauseType, GetTypeName(type), columns.ToSetClause(), keys.ToWhereClause());
			}

			return BuildSql(clauseType, GetTypeName(type), columns.ToClause(), parameters.ToWhereClause());
		}

		private static string BuildSql(ClauseType clauseType, string name, string columns, string where) => clauseType switch
		{
			ClauseType.Select => SqlTemplates.SelectWhere.Replace("{{columns}}", columns, StringComparison.OrdinalIgnoreCase)
														 .Replace("{{table}}", name, StringComparison.OrdinalIgnoreCase)
														 .Replace("{{where}}", where, StringComparison.OrdinalIgnoreCase),
			ClauseType.Delete => SqlTemplates.DeleteWhere.Replace("{{table}}", name, StringComparison.OrdinalIgnoreCase)
														 .Replace("{{where}}", where, StringComparison.OrdinalIgnoreCase),
			ClauseType.Update => SqlTemplates.UpdateWhere.Replace("{{table}}", name, StringComparison.OrdinalIgnoreCase)
														 .Replace("{{columns}}", columns, StringComparison.OrdinalIgnoreCase)
														 .Replace("{{where}}", where, StringComparison.OrdinalIgnoreCase),
			ClauseType.Insert => throw new NotImplementedException(),
			_ => throw new NotImplementedException()
		};
	}
}
