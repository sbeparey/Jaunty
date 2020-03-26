// ﷽

using Dapper;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
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
			string sql = ticket is null ? BuildSelectAllSql<T>() : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectAllSql<T>());
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			string sql = ticket is null ? BuildSelectAllSql<T>() : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectAllSql<T>());
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await connection.QueryAsync<T>(sql, transaction: transaction).ConfigureAwait(false);
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
			var parameters = KeyToParameter<T, TKey>(key);
			string sql = ticket is null ? BuildSelectSql<T>(ClauseType.Select, parameters)  : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectSql<T>(ClauseType.Select, parameters) );
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return connection.QuerySingleOrDefault<T>(sql, parameters, transaction);
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
			var parameters = KeyToParameter<T, TKey>(key);
			string sql = ticket is null ? BuildSelectSql<T>(ClauseType.Select, parameters)  : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectSql<T>(ClauseType.Select, parameters) );
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await connection.QuerySingleOrDefaultAsync<T>(sql, parameters, transaction).ConfigureAwait(false);
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

			var parameters = ExpressionToParameters(expression);
			string sql = ticket is null ? BuildSelectSql(ClauseType.Select, expression)  : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectSql(ClauseType.Select, expression) );
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			var parameters = ExpressionToParameters(expression);
			string sql = ticket is null ? BuildSelectSql(ClauseType.Select, expression)  : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectSql(ClauseType.Select, expression) );
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await connection.QueryAsync<T>(sql, parameters, transaction).ConfigureAwait(false);
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
			string sql = ticket is null ? BuildSelectSql<T>(ClauseType.Select, parameters)  : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectSql<T>(ClauseType.Select, parameters) );
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			string sql = ticket is null ? BuildSelectSql<T>(ClauseType.Select, parameters)  : _queriesCache.GetOrAdd(ticket.Id, q => BuildSelectSql<T>(ClauseType.Select, parameters) );
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await connection.QueryAsync<T>(sql, parameters, transaction).ConfigureAwait(false);
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

			string sql = ticket is null ? ExtractSql<T>(ClauseType.Select, from) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, from));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (from is null)
				throw new ArgumentNullException(nameof(from));

			string sql = ticket is null ? ExtractSql<T>(ClauseType.Select, from) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, from));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (from is null)
				throw new ArgumentNullException(nameof(from));

			return ticket is null ? ExtractSql<T>(ClauseType.Select, from) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, from));
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

			string sql = ticket is null ? ExtractSql<T>(ClauseType.Select, joinOn) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, joinOn));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (joinOn is null)
				throw new ArgumentNullException(nameof(joinOn));

			string sql = ticket is null ? ExtractSql<T>(ClauseType.Select, joinOn) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, joinOn));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (joinOn is null)
				throw new ArgumentNullException(nameof(joinOn));

			return ticket is null ? ExtractSql<T>(ClauseType.Select, joinOn) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, joinOn));
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

			string sql = ticket is null ? ExtractSql<T>(ClauseType.Select, condition) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, condition));
			var parameters = condition.GetParameters();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (condition is null)
				throw new ArgumentNullException(nameof(condition));

			string sql = ticket is null ? ExtractSql<T>(ClauseType.Select, condition) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, condition));
			var parameters = condition.GetParameters();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (condition is null)
				throw new ArgumentNullException(nameof(condition));

			return ticket is null ? ExtractSql<T>(ClauseType.Select, condition) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, condition));
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
				? ExtractSql<T>(ClauseType.Select, groupBy, selectClause: selectClause)
				: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, groupBy, selectClause: selectClause));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (groupBy is null)
				throw new ArgumentNullException(nameof(groupBy));

			string sql = ticket is null
				? ExtractSql<T>(ClauseType.Select, groupBy, selectClause: selectClause)
				: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, groupBy, selectClause: selectClause));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (groupBy is null)
				throw new ArgumentNullException(nameof(groupBy));

			return ticket is null
				? ExtractSql<T>(ClauseType.Select, groupBy, selectClause: selectClause)
				: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, groupBy, selectClause: selectClause));
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
					   ? ExtractSql<T>(ClauseType.Select, orderBy, selectClause: selectClause)
					   : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, orderBy, selectClause: selectClause));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (orderBy is null)
				throw new ArgumentNullException(nameof(orderBy));

			string sql = ticket is null
					   ? ExtractSql<T>(ClauseType.Select, orderBy, selectClause: selectClause)
					   : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, orderBy, selectClause: selectClause));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (orderBy is null)
				throw new ArgumentNullException(nameof(orderBy));

			return ticket is null
					   ? ExtractSql<T>(ClauseType.Select, orderBy, selectClause: selectClause)
					   : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, orderBy, selectClause: selectClause));
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

			var sql = ticket is null ? ExtractSql<T>(ClauseType.Select, limit) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, limit));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (limit is null)
				throw new ArgumentNullException(nameof(limit));

			var sql = ticket is null ? ExtractSql<T>(ClauseType.Select, limit) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, limit));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (limit is null)
				throw new ArgumentNullException(nameof(limit));

			return ticket is null ? ExtractSql<T>(ClauseType.Select, limit) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, limit));
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

			var sql = ticket is null ? ExtractSql<T>(ClauseType.Select, fetchFirst) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, fetchFirst));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (fetchFirst is null)
				throw new ArgumentNullException(nameof(fetchFirst));

			var sql = ticket is null ? ExtractSql<T>(ClauseType.Select, fetchFirst) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, fetchFirst));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (fetchFirst is null)
				throw new ArgumentNullException(nameof(fetchFirst));

			return ticket is null ? ExtractSql<T>(ClauseType.Select, fetchFirst) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, fetchFirst));
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

			var sql = ticket is null ? ExtractSql<T>(ClauseType.Select, fetchNext) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, fetchNext));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (fetchNext is null)
				throw new ArgumentNullException(nameof(fetchNext));

			var sql = ticket is null ? ExtractSql<T>(ClauseType.Select, fetchNext) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, fetchNext));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
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
			if (fetchNext is null)
				throw new ArgumentNullException(nameof(fetchNext));

			return ticket is null ? ExtractSql<T>(ClauseType.Select, fetchNext) : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, fetchNext));
		}

		public static IEnumerable<T> Select<T>(this Having having, string clause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (having is null)
				throw new ArgumentNullException(nameof(having));

			var sql = ticket is null
					? ExtractSql<T>(ClauseType.Select, having, selectClause: clause)
					: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, having, selectClause: clause));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return having.Connection.Query<T>(sql, null, transaction);
		}

		public static async Task<IEnumerable<T>> SelectAsync<T>(this Having having, string clause, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (having is null)
				throw new ArgumentNullException(nameof(having));

			var sql = ticket is null
					? ExtractSql<T>(ClauseType.Select, having, selectClause: clause)
					: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Select, having, selectClause: clause));
			var eventArgs = new SqlEventArgs { Sql = sql };
			OnSelecting?.Invoke(ticket, eventArgs);
			return await having.Connection.QueryAsync<T>(sql, null, transaction).ConfigureAwait(false);
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

		private static string ExtractSql<T>(ClauseType clauseType, Clause clause, string alias = null, string selectClause = null)
		{
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

			string columns = selectClause ?? GetFormattedColumns(type, selectedAlias ?? (hasJoin ? GetTypeName(type) : null));
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
				case ClauseType.Insert:
					builder.Append("INSERT INTO ");
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
					case ClauseType.Update:
						builder.Append(sql);
						break;
					case ClauseType.Insert:
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

		private static string BuildSelectAllSql<T>()
		{
			Type type = GetType(typeof(T));
			string sql = SqlTemplates.Select.Replace("{{columns}}", GetColumnsCache(type).Keys.ToList().ToClause(), StringComparison.OrdinalIgnoreCase)
											.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase);
			sql += ";";
			return sql;
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

		private static string BuildSelectSql<T>(ClauseType clauseType, Expression<Func<T, bool>> expression)
		{
			Type type = GetType(typeof(T));
			var columns = GetColumnsCache(type).Keys.ToList();
			var where = new StringBuilder();
			expression.Body.WalkThrough((name, oper, _) =>
			{
				where.AppendIf(name != null, $"{name} {oper} @{name}");
				where.AppendIf(name is null, $" {oper} ");
			});

			switch (clauseType)
			{
				case ClauseType.Select:
					return SqlTemplates.SelectWhere.Replace("{{columns}}", columns.ToClause(), StringComparison.OrdinalIgnoreCase)
												   .Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase)
												   .Replace("{{where}}", where.ToString(), StringComparison.OrdinalIgnoreCase);
				case ClauseType.Delete:
					return SqlTemplates.DeleteWhere.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase)
												   .Replace("{{where}}", where.ToString(), StringComparison.OrdinalIgnoreCase);
				case ClauseType.Update:
					return SqlTemplates.UpdateWhere.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase)
												   .Replace("{{where}}", where.ToString(), StringComparison.OrdinalIgnoreCase);
				case ClauseType.Insert:
					throw new NotImplementedException();
				default:
					throw new NotImplementedException();
			}
		}

		private static string BuildSelectSql<T>(ClauseType clauseType, IDictionary<string, object> parameters)
		{
			Type type = GetType(typeof(T));
			var columns = GetColumnsCache(type).Keys.ToList();

			switch (clauseType)
			{
				case ClauseType.Select:
					return SqlTemplates.SelectWhere.Replace("{{columns}}", columns.ToClause(), StringComparison.OrdinalIgnoreCase)
												   .Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase)
												   .Replace("{{where}}", parameters.ToWhereClause(), StringComparison.OrdinalIgnoreCase);
				case ClauseType.Delete:
					return SqlTemplates.DeleteWhere.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase)
												   .Replace("{{where}}", parameters.ToWhereClause(), StringComparison.OrdinalIgnoreCase);
				case ClauseType.Update:
					return SqlTemplates.UpdateWhere.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase)
												   .Replace("{{where}}", parameters.ToWhereClause(), StringComparison.OrdinalIgnoreCase);
				case ClauseType.Insert:
					throw new NotImplementedException();
				default:
					throw new NotImplementedException();
			}
		}

		//private static string BuildSelectSql<T>()
		//{
		//	Type type = GetType(typeof(T));
		//	var keys = GetKeysCache(type).Keys.ToList();
		//	var columns = GetColumnsCache(type).Keys.ToList().ToClause();

		//	return SqlTemplates.SelectWhere.Replace("{{columns}}", columns, StringComparison.OrdinalIgnoreCase)
		//								   .Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase)
		//								   .Replace("{{where}}", keys.ToWhereClause(), StringComparison.OrdinalIgnoreCase);
		//}

		#endregion
	}
}
