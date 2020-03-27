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
			var parameter = KeyToParameter<T, TKey>(key);
			string sql = Delete<T>(connection, parameter, ticket);
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
			var parameter = KeyToParameter<T, TKey>(key);
			string sql = Delete<T>(connection, parameter, ticket);
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
			var parameter = KeyToParameter<T, TKey>(key);
			return Delete<T>(connection, parameter, ticket);
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
			var parameters = KeysToParameters<T, TKey1, TKey2>(key1, key2);
			string sql = Delete<T>(connection, parameters, ticket);
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
			var parameters = KeysToParameters<T, TKey1, TKey2>(key1, key2);
			string sql = Delete<T>(connection, parameters, ticket);
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
			var parameters = KeysToParameters<T, TKey1, TKey2>(key1, key2);
			return Delete<T>(connection, parameters, ticket);
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
			var parameters = ExpressionToParameters(expression);
			string sql = Delete<T>(connection, parameters, ticket);
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
			var parameters = ExpressionToParameters(expression);
			string sql = Delete<T>(connection, parameters, ticket);
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
			var parameters = ExpressionToParameters(expression);
			return Delete<T>(connection, parameters, ticket);
		}

		/// <summary>
		/// Deletes by anonymous object
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="nameValuePairs">An anonymous object</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns number of rows affected.</returns>
		public static int DeleteAnonymous<T>(this IDbConnection connection, object nameValuePairs, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameters = nameValuePairs.ToDictionary();
			string sql = Delete<T>(connection, parameters, ticket);
			return connection.Execute(sql, parameters, transaction);
		}

		/// <summary>
		/// Deletes by anonymous object async
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="nameValuePairs">An anonymous object</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns number of rows affected.</returns>
		public static async Task<int> DeleteAnonymousAsync<T>(this IDbConnection connection, object nameValuePairs, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameters = nameValuePairs.ToDictionary();
			string sql = Delete<T>(connection, parameters, ticket);
			return await connection.ExecuteAsync(sql, parameters, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Generates a SQL string for Deletes by anonymous object
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="nameValuePairs">An anonymous object</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <c>string</c></returns>
		public static string DeleteAnonymousAsString<T>(this IDbConnection connection, object nameValuePairs, ITicket ticket = null)
		{
			var parameters = nameValuePairs.ToDictionary();
			return Delete<T>(connection, parameters, ticket);
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
			var parameters = condition?.GetParameters();
			string sql = Delete<T>(condition, parameters, ticket);
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
			var parameters = condition?.GetParameters();
			string sql = Delete<T>(condition, parameters, ticket);
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
			var parameters = condition?.GetParameters();
			return Delete<T>(condition, parameters, ticket);
		}

		private static string Delete<T>(IDbConnection connection, IDictionary<string, object> parameters, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			string sql = ticket is null
				? BuildSql<T>(ClauseType.Delete, parameters)
				: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql<T>(ClauseType.Delete, parameters));

			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(ticket, eventArgs);
			return sql;
		}

		private static string Delete<T>(Clause clause, Dictionary<string, object> parameters, ITicket ticket = null)
		{
			if (clause is null)
				throw new ArgumentNullException(nameof(clause));

			string sql = ticket is null
								? ExtractSql<T>(ClauseType.Delete, clause)
								: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Delete, clause));
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(ticket, eventArgs);
			return sql;
		}
	}
}
