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
			string sql = ticket is null ? BuildSelectAllSql<T>() : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectAllSql<T>());
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return SqlMapper.Query<T>(connection, sql, transaction: transaction);
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
			var parameters = new Dictionary<string, object>();
			string sql = ticket is null ? BuildSelectAllSql<T>() : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectSql<T, TKey>(key, parameters));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return connection.QuerySingleOrDefault<T>(sql, parameters, transaction);
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
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			var parameters = new Dictionary<string, object>();
			string sql = ticket is null ? BuildSelectSql(expression, parameters) : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectSql(expression, parameters));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return connection.Query<T>(sql, parameters, transaction);
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
			string sql = ticket is null ? BuildSelectSql<T>(parameters) : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectSql<T>(parameters));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return connection.Query<T>(sql, nameValuePairs, transaction);
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
			if (from is null)
				throw new ArgumentNullException(nameof(from));

			string sql = ticket is null ? ExtractSql<T>(from) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(from));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return from.Connection.Query<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on From 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="from">From<T></param>
		/// <returns>Returns <see cref="String"/></returns>
		public static string SelectAsSql<T>(this From from, ITicket ticket = null)
		{
			if (from is null)
				throw new ArgumentNullException(nameof(from));

			return ticket is null ? ExtractSql<T>(from) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(from));
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
			if (joinOn is null)
				throw new ArgumentNullException(nameof(joinOn));

			string sql = ticket is null ? ExtractSql<T>(joinOn) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(joinOn));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return SqlMapper.Query<T>(joinOn.Connection, sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on From 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="joinOn">JoinOn<T></param>
		/// <returns>Returns <see cref="string"/></returns>
		public static string SelectAsSql<T>(this JoinOn joinOn, ITicket ticket = null)
		{
			if (joinOn is null)
				throw new ArgumentNullException(nameof(joinOn));

			return ticket is null ? ExtractSql<T>(joinOn) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(joinOn));
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
			if (condition is null)
				throw new ArgumentNullException(nameof(condition));

			string sql = ticket is null ? ExtractSql<T>(condition) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(condition));
			var parameters = condition.GetParameters();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return condition.Connection.Query<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Selects on Condition
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="condition">The condition clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <see cref="string"/></returns>
		public static string SelectAsSql<T>(this Condition condition, ITicket ticket = null)
		{
			if (condition is null)
				throw new ArgumentNullException(nameof(condition));

			return ticket is null ? ExtractSql<T>(condition) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(condition));
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
			if (groupBy is null)
				throw new ArgumentNullException(nameof(groupBy));

			string sql = ticket is null
				? ExtractSql<T>(groupBy, selectClause: selectClause)
				: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(groupBy, selectClause: selectClause));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return groupBy.Connection.Query<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on GroupBy
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="groupBy">GroupBy clause</param>
		/// <param name="selectClause">Select clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns></returns>
		public static string SelectAsSql<T>(this GroupBy groupBy, string selectClause, ITicket ticket = null)
		{
			if (groupBy is null)
				throw new ArgumentNullException(nameof(groupBy));

			return ticket is null
				? ExtractSql<T>(groupBy, selectClause: selectClause)
				: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(groupBy, selectClause: selectClause));
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
			if (orderBy is null)
				throw new ArgumentNullException(nameof(orderBy));

			string sql = ticket is null
					   ? ExtractSql<T>(orderBy, selectClause: selectClause)
					   : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(orderBy, selectClause: selectClause));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return orderBy.Connection.Query<T>(sql: sql, transaction: transaction);
		}

		/// <summary>
		/// Select on OrderBy
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="orderBy">OrderBy clause</param>
		/// <param name="ticket"></param>
		/// <returns>Returns <see cref="string"/></returns>
		public static string SelectAsSql<T>(this OrderBy orderBy, string selectClause = null, ITicket ticket = null)
		{
			if (orderBy is null)
				throw new ArgumentNullException(nameof(orderBy));

			return ticket is null
					   ? ExtractSql<T>(orderBy, selectClause: selectClause)
					   : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(orderBy, selectClause: selectClause));
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
			if (limit is null)
				throw new ArgumentNullException(nameof(limit));

			var sql = ticket is null ? ExtractSql<T>(limit) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(limit));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return SqlMapper.Query<T>(limit.Connection, sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on Limit
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="limit"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static string SelectAsSql<T>(this Limit limit, ITicket ticket = null)
		{
			if (limit is null)
				throw new ArgumentNullException(nameof(limit));

			return ticket is null ? ExtractSql<T>(limit) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(limit));
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
			if (fetchFirst is null)
				throw new ArgumentNullException(nameof(fetchFirst));

			var sql = ticket is null ? ExtractSql<T>(fetchFirst) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(fetchFirst));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return SqlMapper.Query<T>(fetchFirst.Connection, sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on FetchFirst
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="fetchFirst"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static string SelectAsSql<T>(this FetchFirst fetchFirst, ITicket ticket = null)
		{
			if (fetchFirst is null)
				throw new ArgumentNullException(nameof(fetchFirst));

			return ticket is null ? ExtractSql<T>(fetchFirst) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(fetchFirst));
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
			if (fetchNext is null)
				throw new ArgumentNullException(nameof(fetchNext));

			var sql = ticket is null ? ExtractSql<T>(fetchNext) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(fetchNext));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return SqlMapper.Query<T>(fetchNext.Connection, sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on FetchNext
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="fetchNext"></param>
		/// <param name="ticket"></param>
		/// <returns></returns>
		public static string SelectAsSql<T>(this FetchNext fetchNext, ITicket ticket = null)
		{
			if (fetchNext is null)
				throw new ArgumentNullException(nameof(fetchNext));

			return ticket is null ? ExtractSql<T>(fetchNext) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(fetchNext));
		}

		public static IEnumerable<T> Select<T>(this Having having, string clause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (having is null)
				throw new ArgumentNullException(nameof(having));

			var sql = ticket is null
					? ExtractSql<T>(having, selectClause: clause)
					: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(having, selectClause: clause));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return having.Connection.Query<T>(sql, null, transaction);
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

		#region async

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
			string sql = BuildSelectAllSql<T>();
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await connection.QueryAsync<T>(sql, transaction: transaction);
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
			var parameters = new Dictionary<string, object>();
			string sql = BuildSelectSql<T, TKey>(key, parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await connection.QuerySingleOrDefaultAsync<T>(sql, parameters, transaction);
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
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			var parameters = new Dictionary<string, object>();
			string sql = BuildSelectSql(expression, parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await connection.QueryAsync<T>(sql, parameters, transaction);
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
		public static async Task<IEnumerable<T>> SelectAsync<T>(this IDbConnection connection, object nameValuePairs,
			IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (nameValuePairs is null)
			{
				throw new ArgumentNullException(nameof(nameValuePairs));
			}

			var parameters = nameValuePairs.ToDictionary();
			string sql = BuildSelectSql<T>(parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await connection.QueryAsync<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Selects on From asynchronously 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="fromClause">From<T></param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this From fromClause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			string sql = ExtractSql<T>(fromClause);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await fromClause.Connection.QueryAsync<T>(sql, null, transaction);
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
			var sql = ExtractSql<T>(joinOn);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await joinOn.Connection.QueryAsync<T>(sql, transaction: transaction);
		}

		/// <summary>
		/// Selects on Where asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="conditionalClause">Where clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this Condition conditionalClause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var sql = ExtractSql<T>(conditionalClause);
			var parameters = conditionalClause.GetParameters();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await conditionalClause.Connection.QueryAsync<T>(sql, parameters, transaction);
		}

		/// <summary>
		/// Selects on GroupBy asynchronously 
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="groupByClause">GroupBy clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this GroupBy groupByClause, string clause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var sql = ExtractSql<T>(groupByClause);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await groupByClause.Connection.QueryAsync<T>(sql, null, transaction);
		}

		/// <summary>
		/// Selects on OrderBy asynchronously
		/// </summary>
		/// <typeparam name="T">The type representing the database table or view.</typeparam>
		/// <param name="orderByClause">OrderBy clause</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <see cref="IEnumerable{T}"/></returns>
		public static async Task<IEnumerable<T>> SelectAsync<T>(this OrderBy orderByClause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var sql = ExtractSql<T>(orderByClause);
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await orderByClause.Connection.QueryAsync<T>(sql, transaction: transaction);
		}

		#endregion

		#region private methods

		private static OrderBy CreateOrderBy(Clause clause, string orderByColumn, SortOrder? sortOrder)
		{
			var orderBy = new OrderBy(clause);
			orderBy.Add(orderByColumn, sortOrder);
			return orderBy;
		}

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
			string sql = SqlTemplates.Select.Replace("{{columns}}", GetColumnsCache(type).Keys.ToList().ToClause(), StringComparison.OrdinalIgnoreCase)
											.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase);
			sql += ";";
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
