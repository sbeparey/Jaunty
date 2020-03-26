// ﷽

using Dapper;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Jaunty
{
	public interface ISqlEventArgs
	{
		string Sql { get; }

		IDictionary<string, object> Parameters { get; }
	}

	public class SqlEventArgs : EventArgs, ISqlEventArgs
	{
		public string Sql { get; set; }

		public IDictionary<string, object> Parameters { get; set; }
	}

	public static partial class Jaunty
	{
		public static event EventHandler<SqlEventArgs> OnDeleting;

		/// <summary>
		/// Deletes an entity by the specified key.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The type of the primary key.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key">The value of the primary key.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <c>true</c> if only one row is deleted.</returns>
		public static bool Delete<T, TKey>(this IDbConnection connection, TKey key, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			IDictionary<string, object> parameter = KeyToParameter<T, TKey>(key);
			string sql = ticket is null
				? BuildSql<T>(ClauseType.Delete, parameter)
				: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql<T>(ClauseType.Delete, parameter));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameter };
			OnDeleting?.Invoke(ticket, eventArgs);
			int rowsAffected = connection.Execute(sql, parameter, transaction);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Deletes the specified key asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The type of the primary key.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key">The value of the primary key.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <c>true</c> if only one row is deleted.</returns>
		public static async Task<bool> DeleteAsync<T, TKey>(this IDbConnection connection, TKey key, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			IDictionary<string, object> parameter = KeyToParameter<T, TKey>(key);
			string sql = ticket is null
				? BuildSql<T>(ClauseType.Delete, parameter)
				: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql<T>(ClauseType.Delete, parameter));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameter };
			OnDeleting?.Invoke(ticket, eventArgs);
			int rowsAffected = await connection.ExecuteAsync(sql, parameter, transaction).ConfigureAwait(false);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Generates a SQL string for Delete by specified key.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The type of the primary key.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key">The value of the primary key.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <c>string</c></returns>
		public static string DeleteAsString<T, TKey>(this IDbConnection connection, TKey key, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			IDictionary<string, object> parameter = KeyToParameter<T, TKey>(key);
			return ticket is null
						? BuildSql<T>(ClauseType.Delete, parameter)
						: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql<T>(ClauseType.Delete, parameter));
		}

		/// <summary>
		/// Deletes an entity by the specified composite key.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey1">The type of the first key</typeparam>
		/// <typeparam name="TKey2">The type of the second key</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key1">The first key</param>
		/// <param name="key2">The second key</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <c>true</c> if only one row is deleted</returns>
		public static bool Delete<T, TKey1, TKey2>(this IDbConnection connection, TKey1 key1, TKey2 key2, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			IDictionary<string, object> parameters = KeysToParameters<T, TKey1, TKey2>(key1, key2);
			string sql = ticket is null
				? BuildSql<T>(ClauseType.Delete, parameters)
				: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql<T>(ClauseType.Delete, parameters));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(ticket, eventArgs);
			int rowsAffected = connection.Execute(sql, parameters, transaction);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Deletes an entity by the specified composite key asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey1">The type of the first key</typeparam>
		/// <typeparam name="TKey2">The type of the second key</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key1">The first key</param>
		/// <param name="key2">The second key</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <c>true</c> if only one row is deleted</returns>
		public static async Task<bool> DeleteAsync<T, TKey1, TKey2>(this IDbConnection connection, TKey1 key1, TKey2 key2, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			IDictionary<string, object> parameters = KeysToParameters<T, TKey1, TKey2>(key1, key2);
			string sql = ticket is null
				? BuildSql<T>(ClauseType.Delete, parameters)
				: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql<T>(ClauseType.Delete, parameters));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(ticket, eventArgs);
			int rowsAffected = await connection.ExecuteAsync(sql, parameters, transaction).ConfigureAwait(false);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Generates a SQL string for Deletes an entity by the specified composite key.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey1">The type of the first key</typeparam>
		/// <typeparam name="TKey2">The type of the second key</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key1">The first key</param>
		/// <param name="key2">The second key</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <c>true</c> if only one row is deleted</returns>
		public static string DeleteAsString<T, TKey1, TKey2>(this IDbConnection connection, TKey1 key1, TKey2 key2, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			IDictionary<string, object> parameters = KeysToParameters<T, TKey1, TKey2>(key1, key2);
			return ticket is null
						? BuildSql<T>(ClauseType.Delete, parameters)
						: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql<T>(ClauseType.Delete, parameters));
		}

		/// <summary>
		/// Deletes by where
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="condition">The conditional clause.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Return number of rows affected.</returns>
		public static int Delete<T>(this Condition condition, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (condition is null)
				throw new ArgumentNullException(nameof(condition));

			var parameters = condition.GetParameters();
			string sql = ticket is null
					? ExtractSql<T>(ClauseType.Delete, condition)
					: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Delete, condition));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(ticket, eventArgs);
			return condition.Connection.Execute(sql, parameters, transaction);
		}

		/// <summary>
		/// Deletes by where async
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="condition">The conditional clause.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Return number of rows affected.</returns>
		public static async Task<int> DeleteAsync<T>(this Condition condition, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (condition is null)
				throw new ArgumentNullException(nameof(condition));

			var parameters = condition.GetParameters();
			string sql = ticket is null
					? ExtractSql<T>(ClauseType.Delete, condition)
					: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Delete, condition));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(ticket, eventArgs);
			return await condition.Connection.ExecuteAsync(sql, parameters, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Generates a SQL string for Deletes by where
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="condition">The conditional clause.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Return number of rows affected.</returns>
		public static string DeleteAsString<T>(this Condition condition, ITicket ticket = null)
		{
			if (condition is null)
				throw new ArgumentNullException(nameof(condition));

			var parameters = condition.GetParameters();
			return ticket is null
				   ? ExtractSql<T>(ClauseType.Delete, condition)
				   : _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Delete, condition));
		}

		/// <summary>
		/// Deletes by expression
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="expression">The expression.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns number of rows affected.</returns>
		public static int Delete<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (expression is null)
				throw new ArgumentNullException(nameof(expression));

			var parameters = ExpressionToParameters(expression);
			string sql = ticket is null
					? BuildSql(ClauseType.Delete, expression)
					: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql(ClauseType.Delete, expression));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(ticket, eventArgs);
			return connection.Execute(sql, parameters, transaction);
		}

		/// <summary>
		/// Deletes by expression async
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="expression">The expression.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns number of rows affected.</returns>
		public static async Task<int> DeleteAsync<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (expression is null)
				throw new ArgumentNullException(nameof(expression));

			var parameters = ExpressionToParameters(expression);
			string sql = ticket is null
					? BuildSql(ClauseType.Delete, expression)
					: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql(ClauseType.Delete, expression));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(ticket, eventArgs);
			return await connection.ExecuteAsync(sql, parameters, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Generates a SQL string for Deletes by expression
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="expression">The expression.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns number of rows affected.</returns>
		public static string DeleteAsString<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			if (expression is null)
				throw new ArgumentNullException(nameof(expression));

			var parameters = ExpressionToParameters(expression);
			return ticket is null
					? BuildSql(ClauseType.Delete, expression)
					: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql(ClauseType.Delete, expression));
		}
	}
}
