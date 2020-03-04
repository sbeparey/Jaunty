// ﷽

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
		/// <param name="from">From<T></param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this From from, object token = null, IDbTransaction transaction = null)
		{
			if (from is null)
				throw new ArgumentNullException(nameof(from));

			string sql = ExtractSql<T>(from);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return from.Connection.Query<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on From 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="from">From<T></param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static string SelectAsString<T>(this From from)
		{
			if (from is null)
				throw new ArgumentNullException(nameof(from));

			return ExtractSql<T>(from);
		}

		/// <summary>
		/// Selects on Join
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="joinOn">InnerJoin<T> or LeftOuterJoin<T> or RightOuterJoin<T></param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this JoinOn joinOn, object token = null, IDbTransaction transaction = null)
		{
			if (joinOn is null)
				throw new ArgumentNullException(nameof(joinOn));

			string sql = ExtractSql<T>(joinOn);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return joinOn.Connection.Query<T>(sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on Where
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="condition">Where clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this Condition condition, object token = null, IDbTransaction transaction = null)
		{
			if (condition is null)
				throw new ArgumentNullException(nameof(condition));

			var sql = ExtractSql<T>(condition);
			var parameters = condition.GetParameters();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return condition.Connection.Query<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Selects on GroupBy 
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="groupBy">GroupBy clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this GroupBy groupBy, string selectClause, object token = null, IDbTransaction transaction = null)
		{
			if (groupBy is null)
				throw new ArgumentNullException(nameof(groupBy));

			string sql = ExtractSql<T>(groupBy, selectClause: selectClause);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return groupBy.Connection.Query<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on OrderBy
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="orderBy">OrderBy clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this OrderBy orderBy, object token = null, IDbTransaction transaction = null)
		{
			if (orderBy is null)
				throw new ArgumentNullException(nameof(orderBy));

			string sql = ExtractSql<T>(orderBy);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return orderBy.Connection.Query<T>(sql, transaction: transaction);
		}

		public static IEnumerable<T> Select<T>(this Limit limit, object token = null, IDbTransaction transaction = null)
		{
			if (limit is null)
				throw new ArgumentNullException(nameof(limit));

			var sql = ExtractSql<T>(limit);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return limit.Connection.Query<T>(sql, transaction: transaction);
		}

		public static IEnumerable<T> Select<T>(this FetchFirst fetchFirst, object token = null, IDbTransaction transaction = null)
		{
			if (fetchFirst is null)
				throw new ArgumentNullException(nameof(fetchFirst));

			var sql = ExtractSql<T>(fetchFirst);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return fetchFirst.Connection.Query<T>(sql, transaction: transaction);
		}

		public static IEnumerable<T> Select<T>(this FetchNext fetchNext, object token = null, IDbTransaction transaction = null)
		{
			if (fetchNext is null)
				throw new ArgumentNullException(nameof(fetchNext));

			var sql = ExtractSql<T>(fetchNext);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return fetchNext.Connection.Query<T>(sql, transaction: transaction);
		}

		// Todo: Complete
		//public static IEnumerable<T> Select<T>(this HavingClause havingClause, string clause, object token = null, IDbTransaction transaction = null)
		//{
		//	var sql = new StringBuilder();
		//	BuildHaving(havingClause, GetType(typeof(T)), clause, sql);
		//	var eventArgs = new SqlEventArgs { Sql = sql };
		//	OnSelecting?.Invoke(token, eventArgs);
		//	return havingClause.Connection.Query<T>(sql, null, transaction);
		//}

		public static Top Top(this IDbConnection connection, int top)
		{
			return new Top(connection, top);
		}

		public static From From<T>(this IDbConnection connection, string alias = null)
		{
			return new From(connection, GetType(typeof(T)), alias);
		}

		public static From From<T>(this Top top, string alias = null)
		{
			return new From(top, GetType(typeof(T)), alias);
		}

		public static Join InnerJoin<T>(this From from, string alias = null)
		{
			return new Join(from, GetType(typeof(T)), alias);
		}

		public static JoinOn On(this Join join, string column1, string column2)
		{
			return new JoinOn(join, column1, column2);
		}

		public static Join InnerJoin<T>(this JoinOn joinOn, string alias = null)
		{
			return new Join(joinOn, GetType(typeof(T)), alias);
		}

		public static OrderBy OrderBy(this From from, string orderByColumn, SortOrder? sortOrder = null)
		{
			return CreateOrderBy(from, orderByColumn, sortOrder);
		}

		public static OrderBy OrderBy(this Condition condition, string orderByColumn, SortOrder? sortOrder = null)
		{
			return CreateOrderBy(condition, orderByColumn, sortOrder);
		}

		public static OrderBy OrderBy(this GroupBy groupBy, string orderByColumn, SortOrder? sortOrder = null)
		{
			return CreateOrderBy(groupBy, orderByColumn, sortOrder);
		}

		public static OrderBy OrderBy(this Having having, string orderByColumn, SortOrder? sortOrder = null)
		{
			return CreateOrderBy(having, orderByColumn, sortOrder);
		}

		public static OrderBy OrderBy(this OrderBy orderBy, string orderByColumn, SortOrder? sortOrder = null)
		{
			if (orderBy is null)
				throw new ArgumentNullException(nameof(orderBy));

			orderBy.Add(orderByColumn, sortOrder);
			return orderBy;
		}

		private static OrderBy CreateOrderBy(Clause clause, string orderByColumn, SortOrder? sortOrder)
		{
			var orderBy = new OrderBy(clause);
			orderBy.Add(orderByColumn, sortOrder);
			return orderBy;
		}

		public static Limit Limit(this From from, int limit)
		{
			return new Limit(from, limit);
		}

		public static Limit Limit(this Condition condition, int limit)
		{
			return new Limit(condition, limit);
		}

		public static Limit Limit(this OrderBy orderBy, int limit)
		{
			return new Limit(orderBy, limit);
		}

		public static Offset Offset(this Limit limit, int offset)
		{
			return new Offset(limit, offset);
		}

		public static Offset Offset(this OrderBy orderBy, int offset)
		{
			return new Offset(orderBy, offset);
		}

		public static FetchFirst FetchFirst(this Offset offset, int rowCount)
		{
			return new FetchFirst(offset, rowCount);
		}

		public static FetchNext FetchNext(this Offset offset, int rowCount)
		{
			return new FetchNext(offset, rowCount);
		}

		public static GroupBy GroupBy(this From from, params string[] groupByColumns)
		{
			return CreateGroupBy(from, groupByColumns);
		}

		public static GroupBy GroupBy(this Condition condition, params string[] groupByColumns)
		{
			return CreateGroupBy(condition, groupByColumns);
		}

		private static GroupBy CreateGroupBy(Clause clause, params string[] columns)
		{
			var groupBy = new GroupBy(clause);
			groupBy.Add(columns);
			return groupBy;
		}

		public static Having Having(this GroupBy groupBy, string raw)
		{
			var having = new Having(groupBy);
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
		public static async Task<IEnumerable<T>> SelectAsync<T>(this From fromClause, object token = null, IDbTransaction transaction = null)
		{
			string sql = ExtractSql<T>(fromClause);
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
		public static async Task<IEnumerable<T>> SelectAsync<T>(this JoinOn joinOn, object token = null, IDbTransaction transaction = null)
		{
			var sql = ExtractSql<T>(joinOn);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return await joinOn.Connection.QueryAsync<T>(sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on Where asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="conditionalClause">Where clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this Condition conditionalClause, object token = null, IDbTransaction transaction = null)
		{
			var sql = ExtractSql<T>(conditionalClause);
			var parameters = conditionalClause.GetParameters();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(token, eventArgs);
			return await conditionalClause.Connection.QueryAsync<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Selects on GroupBy asynchronously 
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="groupByClause">GroupBy clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this GroupBy groupByClause, string clause, object token = null, IDbTransaction transaction = null)
		{
			var sql = ExtractSql<T>(groupByClause);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return await groupByClause.Connection.QueryAsync<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on OrderBy asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="orderByClause">OrderBy clause</param>
		/// <param name="token">A token object to identify the caller.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this OrderBy orderByClause, object token = null, IDbTransaction transaction = null)
		{
			var sql = ExtractSql<T>(orderByClause);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(token, eventArgs);
			return await orderByClause.Connection.QueryAsync<T>(sql, transaction: transaction);
		}

		#endregion

		#region private methods

		private static string ExtractSql<T>(Clause clause, string alias = null, string selectClause = null)
		{
			var type = GetType(typeof(T));
			string sql = clause.Sql;
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

			string columns = selectClause ?? GetFormattedColumns(type, selectedAlias ?? (hasJoin ? GetTypeName(type) : null));
			var builder = new StringBuilder();
			builder.Append("SELECT ");

			if (hasDistinct || hasTop)
			{
				sql = sql.InsertBefore("FROM", columns + " ");
				builder.Append(sql);
			}
			else
			{
				builder.Append(columns + " " + sql);
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

		private static string BuildSelectAllSql<T>()
		{
			Type type = GetType(typeof(T));

			if (Jaunty.getAllQueriesCache.TryGetValue(type.TypeHandle, out string getAllQueriesCache))
			{
				return getAllQueriesCache;
			}

			string sql = SqlTemplates.Select.Replace("{{columns}}", GetColumnsCache(type).Keys.ToList().ToClause(), StringComparison.OrdinalIgnoreCase)
											.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase);
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
																		.Replace("{{table}}", GetTypeName(type));
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
																		.Replace("{{table}}", GetTypeName(type));
			getQueriesCache[type.TypeHandle] = sqlWithoutWhere;
			return sqlWithoutWhere.Replace("{{where}}", whereClause.ToString());
		}

		private static string BuildSelectSql<T>(IDictionary<string, object> parameters)
		{
			// Todo: cache?
			Type type = GetType(typeof(T));
			var columnsList = GetColumnsCache(type).Keys.ToList();
			string sqlWithoutWhere = SqlTemplates.SelectWhere.Trim().Replace("{{columns}}", columnsList.ToClause())
																		.Replace("{{table}}", GetTypeName(type));
			getQueriesCache[type.TypeHandle] = sqlWithoutWhere;
			return sqlWithoutWhere.Replace("{{where}}", parameters.ToWhereClause());
		}

		#endregion
	}
}
