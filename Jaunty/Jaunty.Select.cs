// ﷽

using Dapper;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Jaunty
{
	public static partial class Jaunty
	{
		public static event EventHandler<SqlEventArgs> OnSelecting;

		#region regular select

		/// <summary>
		/// Gets all of the rows in a table.
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/>.</returns>
		public static IEnumerable<T> GetAll<T>(this IDbConnection connection, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(connection, ticket, true);
			return connection.Query<T>(sql: sql, transaction: transaction);
		}

		/// <summary>
		/// Gets all of the rows in a table asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/>.</returns>
		public static async Task<IEnumerable<T>> GetAllAsync<T>(this IDbConnection connection, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(connection, ticket, true);
			return await connection.QueryAsync<T>(sql, transaction: transaction).ConfigureAwait(false);
		}

		public static string GetAllAsString<T>(this IDbConnection connection, ITicket ticket = null)
		{
			return Select<T>(connection, ticket);
		}

		/// <summary>
		/// Gets an entity by the specified key.
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <typeparam name="TKey">The primary key type.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key">The key.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="T"/></returns>
		public static T Get<T, TKey>(this IDbConnection connection, TKey key, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameter = KeyToParameter<T, TKey>(key);
			string sql = Select<T>(parameter, ticket, true);
			return connection.QuerySingleOrDefault<T>(sql, parameter, transaction);
		}

		/// <summary>
		/// Gets an entity by the specified key asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <typeparam name="TKey">The primary key type.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key">The key.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="T"/></returns>
		public static async Task<T> GetAsync<T, TKey>(this IDbConnection connection, TKey key, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameter = KeyToParameter<T, TKey>(key);
			string sql = Select<T>(parameter, ticket, true);
			return await connection.QuerySingleOrDefaultAsync<T>(sql, parameter, transaction).ConfigureAwait(false);
		}

		public static string GetAsString<T, TKey>(this IDbConnection connection, TKey key, ITicket ticket = null)
		{
			var parameter = KeyToParameter<T, TKey>(key);
			return Select<T>(parameter, ticket);
		}

		/// <summary>
		/// Gets entities by the lambda expression.
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="expression">The key.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		/// <exception cref="ArgumentNullException">expression</exception>
		public static IEnumerable<T> Query<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameters = ExpressionToParameters(expression);
			string sql = Select(expression, parameters, ticket, true);
			return connection.Query<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Gets entities by the lambda expression asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="expression">The key.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		/// <exception cref="ArgumentNullException">expression</exception>
		public static async Task<IEnumerable<T>> QueryAsync<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameters = ExpressionToParameters(expression);
			string sql = Select(expression, parameters, ticket, true);
			return await connection.QueryAsync<T>(sql, parameters, transaction).ConfigureAwait(false);
		}

		public static string QueryAsString<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, ITicket ticket = null)
		{
			var parameters = ExpressionToParameters(expression);
			return Select(expression, parameters, ticket);
		}

		/// <summary>
		/// Gets entities by an anonymous object.
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="nameValuePairs">An anonymous object.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		/// <exception cref="ArgumentNullException">expression</exception>
		public static IEnumerable<T> QueryAnonymous<T>(this IDbConnection connection, object nameValuePairs, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (nameValuePairs is null)
			{
				throw new ArgumentNullException(nameof(nameValuePairs));
			}

			var parameters = nameValuePairs.ToDictionary();
			string sql = Select<T>(parameters, ticket, true);
			return connection.Query<T>(sql, nameValuePairs, transaction);
		}

		/// <summary>
		/// Gets entities by an anonymous object asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="nameValuePairs">An anonymous object.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		/// <exception cref="ArgumentNullException">expression</exception>
		public static async Task<IEnumerable<T>> QueryAnonymousAsync<T>(this IDbConnection connection, object nameValuePairs,
			IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (nameValuePairs is null)
			{
				throw new ArgumentNullException(nameof(nameValuePairs));
			}

			var parameters = nameValuePairs.ToDictionary();
			string sql = Select<T>(parameters, ticket, true);
			return await connection.QueryAsync<T>(sql, parameters, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Gets entities by an anonymous object asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="nameValuePairs">An anonymous object.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		/// <exception cref="ArgumentNullException">expression</exception>
		public static string QueryAnonymousAsString<T>(object nameValuePairs, ITicket ticket = null)
		{
			if (nameValuePairs is null)
			{
				throw new ArgumentNullException(nameof(nameValuePairs));
			}

			var parameters = nameValuePairs.ToDictionary();
			return Select<T>(parameters, ticket);
		}

		#endregion

		#region fluent select

		/// <summary>
		/// Selects on From 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="from">From<T></param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this From from, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(from, ticket, true);
			return from.Connection.Query<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on From asynchronously 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="from">From<T></param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this From from, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(from, ticket, true);
			return await from.Connection.QueryAsync<T>(sql, null, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Selects on From 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="from">From<T></param>
		/// <returns>Returns <see cref="String"/></returns>
		public static string SelectAsString<T>(this From from, ITicket ticket = null)
		{
			return Select<T>(from, ticket, true);
		}

		/// <summary>
		/// Selects on Join
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="joinOn">InnerJoin<T> or LeftOuterJoin<T> or RightOuterJoin<T></param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this JoinOn joinOn, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(joinOn, ticket, true);
			return joinOn.Connection.Query<T>(sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on Join asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="joinOn">InnerJoin<T> or LeftOuterJoin<T> or RightOuterJoin<T></param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this JoinOn joinOn, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(joinOn, ticket, true);
			return await joinOn.Connection.QueryAsync<T>(sql, transaction: transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Selects on From 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="joinOn">JoinOn<T></param>
		/// <returns>Returns <see cref="string"/></returns>
		public static string SelectAsString<T>(this JoinOn joinOn, ITicket ticket = null)
		{
			return Select<T>(joinOn, ticket, true);
		}

		/// <summary>
		/// Selects on Condition
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="condition">Where clause</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this Condition condition, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameters = condition?.GetParameters();
			string sql = Select<T>(condition, parameters, ticket, true);
			return condition.Connection.Query<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Selects on Where asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="conditionalClause">Where clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this Condition condition, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameters = condition?.GetParameters();
			string sql = Select<T>(condition, parameters, ticket, true);
			return await condition.Connection.QueryAsync<T>(sql, parameters, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Selects on Condition
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="condition">The condition clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <see cref="string"/></returns>
		public static string SelectAsString<T>(this Condition condition, ITicket ticket = null)
		{
			var parameters = condition?.GetParameters();
			return Select<T>(condition, parameters, ticket, false);
		}

		/// <summary>
		/// Selects on GroupBy 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="groupBy">GroupBy clause</param>
		/// <param name="selectClause"></param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this GroupBy groupBy, string selectClause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(groupBy, selectClause, ticket, true);
			return groupBy.Connection.Query<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on GroupBy asynchronously 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="groupBy">GroupBy clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this GroupBy groupBy, string selectClause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(groupBy, selectClause, ticket, true);
			return await groupBy.Connection.QueryAsync<T>(sql, null, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Selects on GroupBy
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="groupBy">GroupBy clause</param>
		/// <param name="selectClause">Select clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns></returns>
		public static string SelectAsString<T>(this GroupBy groupBy, string selectClause, ITicket ticket = null)
		{
			return Select<T>(groupBy, selectClause, ticket);
		}

		/// <summary>
		/// Selects on OrderBy
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="orderBy">OrderBy clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static IEnumerable<T> Select<T>(this OrderBy orderBy, string selectClause = null, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(orderBy, selectClause, ticket, true);
			return orderBy.Connection.Query<T>(sql: sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on OrderBy asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="orderByClause">OrderBy clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this OrderBy orderBy, string selectClause = null, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(orderBy, selectClause, ticket, true);
			return await orderBy.Connection.QueryAsync<T>(sql: sql, transaction: transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Select on OrderBy
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="orderBy">OrderBy clause</param>
		/// <param name="ticket"></param>
		/// <returns>Returns <see cref="string"/></returns>
		public static string SelectAsString<T>(this OrderBy orderBy, string selectClause = null, ITicket ticket = null)
		{
			return Select<T>(orderBy, selectClause, ticket, true);
		}

		/// <summary>
		/// Selects on Limit
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="limit"></param>
		/// <param name="transaction"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static IEnumerable<T> Select<T>(this Limit limit, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(limit, ticket, true);
			return limit.Connection.Query<T>(sql: sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on Limit
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="limit"></param>
		/// <param name="transaction"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this Limit limit, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(limit, ticket, true);
			return await limit.Connection.QueryAsync<T>(sql: sql, transaction: transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Selects on Limit
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="limit"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static string SelectAsString<T>(this Limit limit, ITicket ticket = null)
		{
			return Select<T>(limit, ticket, false);
		}

		/// <summary>
		/// Selects on FetchFirst
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="fetchFirst"></param>
		/// <param name="transaction"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static IEnumerable<T> Select<T>(this FetchFirst fetchFirst, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(fetchFirst, ticket, true);
			return fetchFirst.Connection.Query<T>(sql: sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on FetchFirst
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="fetchFirst"></param>
		/// <param name="transaction"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this FetchFirst fetchFirst, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(fetchFirst, ticket, true);
			return await fetchFirst.Connection.QueryAsync<T>(sql: sql, transaction: transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Selects on FetchFirst
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="fetchFirst"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static string SelectAsString<T>(this FetchFirst fetchFirst, ITicket ticket = null)
		{
			return Select<T>(fetchFirst, ticket, false);
		}

		/// <summary>
		/// Selects on FetchNext
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="fetchNext"></param>
		/// <param name="transaction"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static IEnumerable<T> Select<T>(this FetchNext fetchNext, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(fetchNext, ticket, true);
			return fetchNext.Connection.Query<T>(sql: sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on FetchNext
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="fetchNext"></param>
		/// <param name="transaction"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this FetchNext fetchNext, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(fetchNext, ticket, true);
			return await fetchNext.Connection.QueryAsync<T>(sql: sql, transaction: transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Selects on FetchNext
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="fetchNext"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static string SelectAsString<T>(this FetchNext fetchNext, ITicket ticket = null)
		{
			return Select<T>(fetchNext, ticket, true);
		}

		public static IEnumerable<T> Select<T>(this Having having, string clause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(having, clause, ticket, true);
			return having.Connection.Query<T>(sql, null, transaction);
		}

		public static async Task<IEnumerable<T>> SelectAsync<T>(this Having having, string clause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = Select<T>(having, clause, ticket, true);
			return await having.Connection.QueryAsync<T>(sql, null, transaction).ConfigureAwait(false);
		}

		public static string SelectAsString<T>(this Having having, string clause, ITicket ticket = null)
		{
			return Select<T>(having, clause, ticket, false);
		}

		#endregion

		public static Distinct Distinct(this IDbConnection connection)
		{
			return new Distinct(connection);
		}

		public static Top Top(this IDbConnection connection, int top)
		{
			return new Top(connection, top);
		}

		public static Top Top(this Distinct distinct, int top)
		{
			return new Top(distinct, top);
		}

		public static From From<T>(this IDbConnection connection, string alias = null)
		{
			return new From(connection, GetType(typeof(T)), alias);
		}

		public static From From<T>(this Distinct distinct, string alias = null)
		{
			return new From(distinct, GetType(typeof(T)), alias);
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
			return new GroupBy(clause, columns);
		}

		public static Having Having(this GroupBy groupBy, string clause)
		{
			return new Having(groupBy, clause);
		}

		#region private methods

		private static OrderBy CreateOrderBy(Clause clause, string orderByColumn, SortOrder? sortOrder)
		{
			var orderBy = new OrderBy(clause);
			orderBy.Add(orderByColumn, sortOrder);
			return orderBy;
		}

		private static string Select<T>(IDbConnection connection, ITicket ticket = null, bool triggerEvent = false)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			string sql = ticket is null
						? BuildSelectAllSql<T>()
						: _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectAllSql<T>());

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return sql;
		}

		private static string Select<T>(Dictionary<string, object> parameters, ITicket ticket = null, bool triggerEvent = false)
		{
			if (parameters is null)
				throw new ArgumentNullException(nameof(parameters));

			if (parameters.Count <= 0)
				throw new ArgumentOutOfRangeException(nameof(parameters));

			string sql = ticket is null
				? BuildSql<T>(ClauseType.Select, parameters)
				: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql<T>(ClauseType.Select, parameters));

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return sql;
		}

		private static string Select<T>(Expression<Func<T, bool>> expression, Dictionary<string, object> parameters, ITicket ticket, bool triggerEvent = false)
		{
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			string sql = ticket is null
				? BuildSql(ClauseType.Select, expression)
				: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql(ClauseType.Select, expression));

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return sql;
		}

		private static string Select<T>(Clause clause, ITicket ticket, bool triggerEvent = false)
		{
			if (clause is null)
				throw new ArgumentNullException(nameof(clause));

			string sql = ticket is null
				? ExtractSql<T>(ClauseType.Select, clause)
				: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, clause));

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return sql;
		}

		private static string Select<T>(Condition condition, Dictionary<string, object> parameters, ITicket ticket = null, bool triggerEvent = false)
		{
			if (condition is null)
				throw new ArgumentNullException(nameof(condition));

			if (parameters is null)
				throw new ArgumentNullException(nameof(parameters));

			if (parameters.Count <= 0)
				throw new ArgumentOutOfRangeException(nameof(parameters));

			string sql = ticket is null
				? ExtractSql<T>(ClauseType.Select, condition)
				: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, condition));

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return sql;
		}

		private static string Select<T>(Clause clause, string selectClause, ITicket ticket = null, bool triggerEvent = false)
		{
			if (clause is null)
				throw new ArgumentNullException(nameof(clause));

			string sql = ticket is null
				? ExtractSql<T>(ClauseType.Select, clause, selectClause: selectClause)
				: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, clause, selectClause: selectClause));

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return sql;
		}

		#endregion
	}
}
