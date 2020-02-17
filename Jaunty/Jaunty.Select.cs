using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

using Dapper;

namespace Jaunty
{
	public static partial class Jaunty
	{
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> getQueriesCache = new ConcurrentDictionary<RuntimeTypeHandle, string>();
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> getAllQueriesCache = new ConcurrentDictionary<RuntimeTypeHandle, string>();

		public static event EventHandler<SqlEventArgs> OnSelecting;

		/// <summary>
		/// Gets all of the rows in a table.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/>.</returns>
		public static IEnumerable<T> GetAll<T>(this IDbConnection connection, object token = null, IDbTransaction transaction = null)
		{
			string sql = BuildSelectAllSql<T>();
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return connection.Query<T>(sql, transaction: transaction);
		}

		/// <summary>
		/// Gets an entity by the specified key.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The primary key type.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key">The key.</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="T"/></returns>
		public static T Get<T, TKey>(this IDbConnection connection, TKey key, object token = null, IDbTransaction transaction = null)
		{
			var parameters = new Dictionary<string, object>();
			string sql = BuildSelectSql<T, TKey>(key, parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return connection.QuerySingleOrDefault<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Gets entities by the lambda expression.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="expression">The key.</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		/// <exception cref="ArgumentNullException">expression</exception>
		public static IEnumerable<T> Query<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, object token = null, IDbTransaction transaction = null)
		{
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			var parameters = new Dictionary<string, object>();
			string sql = BuildSelectSql(expression, parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return connection.Query<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Gets entities by an anonymous object.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="nameValuePairs">An anonymous object.</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		/// <exception cref="ArgumentNullException">expression</exception>
		public static IEnumerable<T> Select<T>(this IDbConnection connection, object nameValuePairs, object token = null, IDbTransaction transaction = null)
		{
			if (nameValuePairs is null)
			{
				throw new ArgumentNullException(nameof(nameValuePairs));
			}

			var parameters = nameValuePairs.ToDictionary();
			string sql = BuildSelectSql<T>(parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return connection.Query<T>(sql, nameValuePairs, transaction);
		}

		/// <summary>
		/// Selects on From 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="fromClause">From<T></param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this FromClause fromClause, object token = null, IDbTransaction transaction = null)
		{
			string sql = GetSelectSql<T>(fromClause);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return fromClause.Connection.Query<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on From 
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="fromClause">From<T></param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> SelectDistinct<T>(this FromClause fromClause, object token = null, IDbTransaction transaction = null)
		{
			string sql = GetSelectSql<T>(fromClause, distinct: true);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return fromClause.Connection.Query<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on Join
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="joinOn">InnerJoin<T> or LeftOuterJoin<T> or RightOuterJoin<T></param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this JoinOnClause joinOn, object token = null, IDbTransaction transaction = null)
		{
			Type selectedType = GetType(typeof(T));
			var sql = new StringBuilder();
			BuildJoinOnClause(joinOn, selectedType, null, sql);
			var eventArgs = new SqlEventArgs { Sql = sql.ToString() };
			OnSelecting?.Invoke(token, eventArgs);
			return joinOn.Connection.Query<T>(sql.ToString(), transaction: transaction);
		}

		/// <summary>
		/// Selects on Where
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="conditionalClause">Where clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this ConditionalClause conditionalClause, object token = null, IDbTransaction transaction = null)
		{
			Type type = GetType(typeof(T));
			var sql = new StringBuilder();
			var parameters = conditionalClause.GetParameters();
			BuildConditionClause(conditionalClause, type, sql);
			var eventArgs = new SqlEventArgs { Sql = sql.ToString(), Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return conditionalClause.Connection.Query<T>(sql.ToString(), parameters, transaction);
		}

		/// <summary>
		/// Selects on GroupBy 
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="groupByClause">GroupBy clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this GroupByClause groupByClause, string clause, object token = null, IDbTransaction transaction = null)
		{
			var sql = new StringBuilder();
			BuildGroupBy(groupByClause, GetType(typeof(T)), clause, sql);
			var eventArgs = new SqlEventArgs { Sql = sql.ToString() };
			OnSelecting?.Invoke(token, eventArgs);
			return groupByClause.Connection.Query<T>(sql.ToString(), null, transaction);
		}

		/// <summary>
		/// Selects on OrderBy
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="orderByClause">OrderBy clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this OrderByClause orderByClause, object token = null, IDbTransaction transaction = null)
		{
			var sql = new StringBuilder();
			BuildOrderBy(orderByClause, GetType(typeof(T)), sql);
			var eventArgs = new SqlEventArgs { Sql = sql.ToString() };
			OnSelecting?.Invoke(token, eventArgs);
			return orderByClause.Connection.Query<T>(sql.ToString(), transaction: transaction);
		}

		public static IEnumerable<T> Select<T>(this LimitClause limitClause, object token = null, IDbTransaction transaction = null)
		{
			var sql = new StringBuilder();
			BuildLimit(limitClause, GetType(typeof(T)), sql);
			var eventArgs = new SqlEventArgs { Sql = sql.ToString() };
			OnSelecting?.Invoke(token, eventArgs);
			return limitClause.Connection.Query<T>(sql.ToString(), transaction: transaction);
		}

		// Todo: Complete
		//public static IEnumerable<T> Select<T>(this HavingClause havingClause, string clause, object token = null, IDbTransaction transaction = null)
		//{
		//	var sql = new StringBuilder();
		//	BuildHaving(havingClause, GetType(typeof(T)), clause, sql);
		//	var eventArgs = new SqlEventArgs { Sql = sql.ToString() };
		//	OnSelecting?.Invoke(token, eventArgs);
		//	return havingClause.Connection.Query<T>(sql.ToString(), null, transaction);
		//}

		public static LimitClause Limit(this FromClause fromClause, int limit)
		{
			return new LimitClause(fromClause, limit);
		}

		public static LimitClause Top(this FromClause fromClause, int top)
		{
			return new LimitClause(fromClause, top);
		}

		public static FromClause From<T>(this IDbConnection connection, string alias = null)
		{
			return new FromClause(connection, GetType(typeof(T)), alias);
		}

		public static JoinClause InnerJoin<T>(this FromClause fromClause, string alias = null)
		{
			return new JoinClause(fromClause, GetType(typeof(T)), alias);
		}

		public static JoinOnClause On(this JoinClause joinClause, string column1, string column2)
		{
			return new JoinOnClause(joinClause, column1, column2);
		}

		public static JoinClause InnerJoin<T>(this JoinOnClause joinOn, string alias = null)
		{
			return new JoinClause(joinOn, GetType(typeof(T)), alias);
		}

		public static OrderByClause OrderBy(this FromClause fromClause, string orderByColumn, SortOrder? sortOrder = null)
		{
			return CreateOrderBy(fromClause, orderByColumn, sortOrder);
		}

		public static OrderByClause OrderBy(this ConditionalClause conditionClause, string orderByColumn, SortOrder? sortOrder = null)
		{
			return CreateOrderBy(conditionClause, orderByColumn, sortOrder);
		}

		public static OrderByClause OrderBy(this GroupByClause groupByClause, string orderByColumn, SortOrder? sortOrder = null)
		{
			return CreateOrderBy(groupByClause, orderByColumn, sortOrder);
		}

		public static OrderByClause OrderBy(this HavingClause havingClause, string orderByColumn, SortOrder? sortOrder = null)
		{
			return CreateOrderBy(havingClause, orderByColumn, sortOrder);
		}

		public static OrderByClause OrderBy(this OrderByClause orderByClause, string orderByColumn, SortOrder? sortOrder = null)
		{
			orderByClause.Add(orderByColumn, sortOrder);
			return orderByClause;
		}

		private static OrderByClause CreateOrderBy(Clause clause, string orderByColumn, SortOrder? sortOrder)
		{
			var orderBy = new OrderByClause(clause);
			orderBy.Add(orderByColumn, sortOrder);
			return orderBy;
		}

		public static GroupByClause GroupBy(this FromClause fromClause, params string[] groupByColumns)
		{
			return CreateGroupBy(fromClause, groupByColumns);
		}

		public static GroupByClause GroupBy(this ConditionalClause conditionClause, params string[] groupByColumns)
		{
			return CreateGroupBy(conditionClause, groupByColumns);
		}

		private static GroupByClause CreateGroupBy(Clause clause, params string[] columns)
		{
			var groupBy = new GroupByClause(clause);
			groupBy.Add(columns);
			return groupBy;
		}

		public static HavingClause Having(this GroupByClause groupByClause, string raw)
		{
			var having = new HavingClause(groupByClause);
			having.Add(raw);
			return having;
		}

		#region async

		/// <summary>
		/// Gets all of the rows in a table asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/>.</returns>
		public static async Task<IEnumerable<T>> GetAllAsync<T>(this IDbConnection connection, object token = null, IDbTransaction transaction = null)
		{
			string sql = BuildSelectAllSql<T>();
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return await connection.QueryAsync<T>(sql, transaction: transaction);
		}

		/// <summary>
		/// Gets an entity by the specified key asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The primary key type.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key">The key.</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="T"/></returns>
		public static async Task<T> GetAsync<T, TKey>(this IDbConnection connection, TKey key, object token = null, IDbTransaction transaction = null)
		{
			var parameters = new Dictionary<string, object>();
			string sql = BuildSelectSql<T, TKey>(key, parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return await connection.QuerySingleOrDefaultAsync<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Gets entities by the lambda expression asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="expression">The key.</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		/// <exception cref="ArgumentNullException">expression</exception>
		public static async Task<IEnumerable<T>> QueryAsync<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, object token = null, IDbTransaction transaction = null)
		{
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			var parameters = new Dictionary<string, object>();
			string sql = BuildSelectSql(expression, parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return await connection.QueryAsync<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Gets entities by an anonymous object asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="nameValuePairs">An anonymous object.</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		/// <exception cref="ArgumentNullException">expression</exception>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this IDbConnection connection, object nameValuePairs,
			object token = null, IDbTransaction transaction = null)
		{
			if (nameValuePairs is null)
			{
				throw new ArgumentNullException(nameof(nameValuePairs));
			}

			var parameters = nameValuePairs.ToDictionary();
			string sql = BuildSelectSql<T>(parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return await connection.QueryAsync<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Selects on From asynchronously 
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="fromClause">From<T></param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this FromClause fromClause, object token = null, IDbTransaction transaction = null)
		{
			string sql = GetSelectSql<T>(fromClause);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return await fromClause.Connection.QueryAsync<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on Join asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="joinOn">InnerJoin<T> or LeftOuterJoin<T> or RightOuterJoin<T></param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this JoinOnClause joinOn, object token = null, IDbTransaction transaction = null)
		{
			Type selectedType = GetType(typeof(T));
			var sql = new StringBuilder();
			BuildJoinOnClause(joinOn, selectedType, null, sql);
			var eventArgs = new SqlEventArgs { Sql = sql.ToString() };
			OnSelecting?.Invoke(token, eventArgs);
			return await joinOn.Connection.QueryAsync<T>(sql.ToString(), transaction: transaction);
		}

		/// <summary>
		/// Selects on Where asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="conditionalClause">Where clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this ConditionalClause conditionalClause, object token = null, IDbTransaction transaction = null)
		{
			Type type = GetType(typeof(T));
			var sql = new StringBuilder();
			var parameters = conditionalClause.GetParameters();
			BuildConditionClause(conditionalClause, type, sql);
			var eventArgs = new SqlEventArgs { Sql = sql.ToString(), Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return await conditionalClause.Connection.QueryAsync<T>(sql.ToString(), parameters, transaction);
		}

		/// <summary>
		/// Selects on GroupBy asynchronously 
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="groupByClause">GroupBy clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this GroupByClause groupByClause, string clause, object token = null, IDbTransaction transaction = null)
		{
			var sql = new StringBuilder();
			BuildGroupBy(groupByClause, GetType(typeof(T)), clause, sql);
			var eventArgs = new SqlEventArgs { Sql = sql.ToString() };
			OnSelecting?.Invoke(token, eventArgs);
			return await groupByClause.Connection.QueryAsync<T>(sql.ToString(), null, transaction);
		}

		/// <summary>
		/// Selects on OrderBy asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="orderByClause">OrderBy clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this OrderByClause orderByClause, object token = null, IDbTransaction transaction = null)
		{
			var sql = new StringBuilder();
			BuildOrderBy(orderByClause, GetType(typeof(T)), sql);
			var eventArgs = new SqlEventArgs { Sql = sql.ToString() };
			OnSelecting?.Invoke(token, eventArgs);
			return await orderByClause.Connection.QueryAsync<T>(sql.ToString(), transaction: transaction);
		}

		#endregion

		#region private methods

		private static string GetSelectSql<T>(FromClause fromClause, int top = 0, bool distinct = false)
		{
			Type type = GetType(typeof(T));
			var builder = new StringBuilder();
			BuildFromClause(fromClause, type, null, builder, top, distinct);
			return builder.ToString();
		}

		private static string BuildSelectAllSql(FromClause fromClause, Type selectedType)
		{
			if (fromClause.EntityType != selectedType)
			{
				throw new Exception("T in From<T> and SelectAll<T> must match");
			}

			string tableName = GetTableName(fromClause.EntityType);
			return $"SELECT * FROM {tableName} ";
		}

		private static void BuildSelectSql(Clause clause, Type selectedType, string selectedTypeAlias, StringBuilder builder)
		{
			switch (clause)
			{
				case FromClause fromClause:
					BuildFromClause(fromClause, selectedType, selectedTypeAlias, builder);
					break;
				case JoinOnClause joinOnClause:
					BuildJoinOnClause(joinOnClause, selectedType, selectedTypeAlias, builder);
					break;
				case GroupByClause groupByClause:
					BuildGroupBy(groupByClause, selectedType, selectedTypeAlias, builder);
					break;
				case HavingClause havingClause:
					BuildHaving(havingClause, selectedType, selectedTypeAlias, builder);
					break;
				case ConditionalClause conditionClause:
					BuildSelectSql(conditionClause, selectedType, selectedTypeAlias, builder);
					break;
			}
		}

		private static void BuildFromClause(FromClause fromClause, Type selectedType, string selectedTypeAlias, StringBuilder builder, int top = 0, bool distinct = false)
		{
			string tableName = GetTableName(fromClause.EntityType);
			string columns = "{{columns}}";

			if (fromClause.EntityType == selectedType)
			{
				columns = GetFormattedColumns(selectedType, selectedTypeAlias);
			}

			string select = distinct
				              ? SqlTemplates.SelectDistinct.Replace("{{columns}}", columns, StringComparison.OrdinalIgnoreCase)
							  : SqlTemplates.Select.Replace("{{columns}}", columns, StringComparison.OrdinalIgnoreCase);
			select = select.Replace("{{table}}", tableName, StringComparison.OrdinalIgnoreCase) + " ";
			builder.Append(select);
		}

		private static void BuildJoinClause(JoinClause joinClause, Type selectedType, string selectedTypeAlias, StringBuilder builder)
		{
			BuildSelectSql(joinClause.PreviousClause, selectedType, selectedTypeAlias, builder);

			if (joinClause.PreviousClause is FromClause fromClause
				&& !GetTableName(fromClause.EntityType).Equals(fromClause.Alias))
			{
				builder.Append($"{fromClause.Alias} ");
			}

			if (joinClause.EntityType == selectedType)
			{
				string formattedColumns = GetFormattedColumns(joinClause.EntityType, joinClause.Alias);
				builder.Replace("{{columns}}", formattedColumns);
			}

			string tableName = GetTableName(joinClause.EntityType);
			builder.Append($"{joinClause.JoinType.ToSqlString()} {tableName} ");

			if (!tableName.Equals(joinClause.Alias))
			{
				builder.Append($"{joinClause.Alias} ");
			}
		}

		private static void BuildJoinOnClause(JoinOnClause joinOn, Type selectedType, string selectTypeAlias, StringBuilder builder)
		{
			var join = (JoinClause)joinOn.PreviousClause;
			BuildJoinClause(join, selectedType, selectTypeAlias, builder);
			builder.Append("ON ");
			builder.Append($"{joinOn.Column1} = {joinOn.Column2} ");
		}

		private static void BuildConditionClause(ConditionalClause conditionClause, Type selectedType, StringBuilder builder)
		{
			BuildSelectSql(conditionClause.PreviousClause, selectedType, null, builder);
			string where = conditionClause.ToWhereClause();
			builder.Append($"WHERE {where} ");
		}

		private static void BuildGroupBy(GroupByClause groupByClause, Type selectedType, string clause, StringBuilder builder)
		{
			BuildSelectSql(groupByClause.PreviousClause, null, null, builder);

			// TODO: In the future, do validations to allow only valid columns and sql functions
			// listed here: https://docs.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql?view=sql-server-2017
			// for now, just passing the given column names to Select
			//List<string> columnNames = GetColumnNamesCache(selectedType);
			//string[] columnsArray = clause.Split(',');
			//var selectColumns = new List<string>();

			//for (int i = 0; i < columnNames.Count; i++)
			//{
			//    for (int j = 0; j < columnsArray.Length; j++)
			//    {
			//        if (columnsArray[j].Trim().Equals(columnNames[i], StringComparison.InvariantCultureIgnoreCase))
			//        {
			//            selectColumns.Add(columnNames[i]);
			//        }
			//    }
			//}

			//builder.Replace("{{columns}}", string.Join(", ", selectColumns));
			builder.Replace("{{columns}}", clause ?? "{{columns}}");
			string groupByColumns = groupByClause.GroupBys.ToClause();
			builder.Append($"GROUP BY {groupByColumns} ");
		}

		private static void BuildHaving(HavingClause havingClause, Type selectedType, string clause, StringBuilder builder)
		{
			// Todo: Enhance Having
			BuildSelectSql(havingClause.PreviousClause, null, null, builder);
			builder.Replace("{{columns}}", clause ?? "{{columns}}");
			string havings = havingClause.Havings.ToClause();
			builder.Append($"HAVING {havings} ");
		}

		private static void BuildOrderBy(OrderByClause orderByClause, Type selectedType, StringBuilder builder)
		{
			BuildSelectSql(orderByClause.PreviousClause, selectedType, null, builder);
			IDictionary<string, SortOrder?> orderBys = orderByClause.OrderBys;

			for (int i = 0; i < orderBys.Count; i++)
			{
				KeyValuePair<string, SortOrder?> kvp = orderBys.ElementAt(i);
				builder.AppendIf(i == 0, "ORDER BY ");
				builder.Append($"{kvp.Key}");
				builder.AppendIf(kvp.Value.HasValue, $" {(kvp.Value == SortOrder.Descending ? "DESC" : "ASC")}");
				builder.Append(i < orderBys.Count - 1 ? ", " : " ");
			}
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

		private static string BuildSelectAllSql<T>()
		{
			Type type = GetType(typeof(T));

			if (Jaunty.getAllQueriesCache.TryGetValue(type.TypeHandle, out string getAllQueriesCache))
			{
				return getAllQueriesCache;
			}

			string sql = SqlTemplates.Select.Replace("{{columns}}", GetColumnsCache(type).Keys.ToList().ToClause(), StringComparison.OrdinalIgnoreCase)
											.Replace("{{table}}", GetTableName(type), StringComparison.OrdinalIgnoreCase);
			sql += ";";
			Jaunty.getAllQueriesCache[type.TypeHandle] = sql;
			return sql;
		}

		private static string BuildSelectSql<T, TKey>(TKey key, IDictionary<string, object> parameters)
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

			parameters.Add(keyColumnsList[0], key);

			if (getQueriesCache.TryGetValue(type.TypeHandle, out string s))
			{
				return s.Replace("{{where}}", keyColumnsList.ToWhereClause());
			}

			string sqlWithoutWhere = SqlTemplates.SelectWhere.Trim().Replace("{{columns}}", GetColumnsCache(type).Keys.ToList().ToClause())
																		.Replace("{{table}}", GetTableName(type));
			getQueriesCache[type.TypeHandle] = sqlWithoutWhere;
			return sqlWithoutWhere.Replace("{{where}}", keyColumnsList.ToWhereClause());
		}

		private static string BuildSelectSql<T>(Expression<Func<T, bool>> expression, IDictionary<string, object> parameters)
		{
			Type type = GetType(typeof(T));
			var whereClause = new StringBuilder();
			expression.Body.WalkThrough((n, o, v) => ExtractClause(n, o, v, whereClause, parameters.Add));

			if (getQueriesCache.TryGetValue(type.TypeHandle, out string s))
			{
				return s.Replace("{{where}}", whereClause.ToString());
			}

			string sqlWithoutWhere = SqlTemplates.SelectWhere.Trim().Replace("{{columns}}", GetColumnsCache(type).Keys.ToList().ToClause())
																		.Replace("{{table}}", GetTableName(type));
			getQueriesCache[type.TypeHandle] = sqlWithoutWhere;
			return sqlWithoutWhere.Replace("{{where}}", whereClause.ToString());
		}

		private static string BuildSelectSql<T>(IDictionary<string, object> parameters)
		{
			// Todo: cache?
			Type type = GetType(typeof(T));
			var columnsList = GetColumnsCache(type).Keys.ToList();
			string sqlWithoutWhere = SqlTemplates.SelectWhere.Trim().Replace("{{columns}}", columnsList.ToClause())
																		.Replace("{{table}}", GetTableName(type));
			getQueriesCache[type.TypeHandle] = sqlWithoutWhere;
			return sqlWithoutWhere.Replace("{{where}}", parameters.ToWhereClause());
		}

		#endregion
	}
}
