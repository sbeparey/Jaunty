using Dapper;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Jaunty
{
	public static partial class Jaunty
	{
		public static event EventHandler<SqlEventArgs> OnInserting;

		/// <summary>
		/// Inserts the specified entity.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <c>true</c> if only one row is inserted.</returns>
		/// <exception cref="ArgumentNullException">connection</exception>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static bool Insert<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			if (entity is null)
				throw new ArgumentNullException(nameof(entity));

			var parameters = ConvertToParameters(entity);
			string sql = Insert<T>(parameters, false, ticket, true);
			int rowsAffected = connection.Execute(sql, parameters, transaction);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Inserts the specified entity asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns <c>true</c> if only one row is inserted.</returns>
		/// <exception cref="ArgumentNullException">connection</exception>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static async Task<bool> InsertAsync<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			if (entity is null)
				throw new ArgumentNullException(nameof(entity));

			var parameters = ConvertToParameters(entity);
			string sql = Insert<T>(parameters, false, ticket, true);
			int rowsAffected = await connection.ExecuteAsync(sql, parameters, transaction).ConfigureAwait(false);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Generates a SQL string for Inserting an entity.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns string.</returns>
		/// <exception cref="ArgumentNullException">connection</exception>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static string InsertAsString<T>(this IDbConnection connection, T entity, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			if (entity is null)
				throw new ArgumentNullException(nameof(entity));

			var parameters = ConvertToParameters(entity);
			return Insert<T>(parameters, false, ticket);
		}

		/// <summary>
		/// Inserts the specified entity.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The type representing the primary key.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns the primary key of the entity once inserted successfully</returns>
		/// <exception cref="ArgumentNullException">connection</exception>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static TKey Insert<T, TKey>(this IDbConnection connection, T entity, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			if (entity is null)
				throw new ArgumentNullException(nameof(entity));

			var parameters = ConvertToParameters(entity);
			string sql = Insert<T>(parameters, true, ticket, true);
			connection.Execute(sql, parameters, transaction);
			return connection.QuerySingle<TKey>(sql, parameters, transaction);
		}

		/// <summary>
		/// Inserts the specified entity asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The type representing the primary key.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns the primary key of the entity once inserted successfully</returns>
		/// <exception cref="ArgumentNullException">connection</exception>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static async Task<TKey> InsertAsync<T, TKey>(this IDbConnection connection, T entity, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			if (entity is null)
				throw new ArgumentNullException(nameof(entity));

			var parameters = ConvertToParameters(entity);
			string sql = Insert<T>(parameters, true, ticket, true);
			connection.Execute(sql, parameters, transaction);
			return await connection.QuerySingleAsync<TKey>(sql, parameters, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Generates a SQL string for Inserting an entity.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns string</returns>
		/// <exception cref="ArgumentNullException">connection</exception>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static string InsertAsString<T, TKey>(this IDbConnection connection, T entity, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			if (entity is null)
				throw new ArgumentNullException(nameof(entity));

			var parameters = ConvertToParameters(entity);
			return Insert<T>(parameters, true, ticket);
		}

		public static Values Values<T>(this IDbConnection connection, T entity)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			return new Values(connection, entity);
		}

		public static int Insert<T>(this Values values, IDbTransaction transaction = null, ITicket ticket = null)
		{
			return InsertInto<T>(values, transaction, ticket);
		}

		public static int InsertInto<T>(this Values values, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameters = ConvertToParameters(values?.Entity);
			string sql = Insert<T>(values, parameters, ticket, true);
			return values.Connection.Execute(sql, parameters, transaction);
		}

		public static async Task<int> InsertAsync<T>(this Values values, IDbTransaction transaction = null, ITicket ticket = null)
		{
			return await InsertIntoAsync<T>(values, transaction, ticket).ConfigureAwait(false);
		}

		public static async Task<int> InsertIntoAsync<T>(this Values values, IDbTransaction transaction = null, ITicket ticket = null)
		{
			var parameters = ConvertToParameters(values?.Entity);
			string sql = Insert<T>(values, parameters, ticket, true);
			return await values.Connection.ExecuteAsync(sql, parameters, transaction).ConfigureAwait(false);
		}

		public static string InsertIntoAsString<T>(this Values values, ITicket ticket = null)
		{
			var parameters = ConvertToParameters(values?.Entity);
			return Insert<T>(values, parameters, ticket, true);
		}

		private static string Insert<T>(Values values, IDictionary<string, object> parameters, ITicket ticket = null, bool triggerEvent = false)
		{
			if (values is null)
				throw new ArgumentNullException(nameof(values));

			if (parameters is null)
				throw new ArgumentNullException(nameof(parameters));

			if (parameters.Count <= 0)
				throw new ArgumentOutOfRangeException(nameof(parameters));

			var sql = ticket is null
						? ExtractInsert<T>(values)
						: _queriesCache.GetOrAdd(ticket.Id, q => ExtractInsert<T>(values));

			if (!triggerEvent)
				return sql;
			
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(ticket, eventArgs);
			return sql;
		}

		private static string Insert<T>(IDictionary<string, object> parameters, bool returnScopeId, ITicket ticket = null, bool triggerEvent = false)
		{
			if (parameters is null)
				throw new ArgumentNullException(nameof(parameters));

			if (parameters.Count <= 0)
				throw new ArgumentOutOfRangeException(nameof(parameters));

			string sql = ticket is null
						? ExtractInsert<T>(parameters, returnScopeId)
						: _queriesCache.GetOrAdd(ticket.Id, q => ExtractInsert<T>(parameters, returnScopeId));

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnInserting?.Invoke(ticket, eventArgs);
			return sql;
		}

		private static string ExtractInsert<T>(Clause clause, bool returnId = false)
		{
			var type = GetType(typeof(T));
			string entity = GetTypeName(type);
			var nonKeyColumns = GetNonKeyColumnsCache(type);
			string columns = nonKeyColumns.ToClause();
			string values = nonKeyColumns.ToClause(prefix: "@");

			string sql = GenerateInsert(clause.ToSql(), entity, columns, values);
			sql += ";";

			if (returnId)
			{
				var keyColumns = new List<string>(GetKeysCache(type).Keys);
				string selectId = ExtractReturnId(keyColumns);
				sql += " " + selectId;
			}

			return sql;
		}

		private static string ExtractInsert<T>(IDictionary<string, object> parameters, bool returnId = false)
		{
			var type = GetType(typeof(T));
			string entity = GetTypeName(type);
			var keys = GetKeysCache(type);
			IList<string> keyColumns = new List<string>(keys.Keys);
			IList<string> parameterColumns = new List<string>(parameters.Keys);

			if (keyColumns.Count == 1 && !IsManual(keys.ElementAt(0).Value))
			{
				foreach (var key in keyColumns)
					parameterColumns.Remove(key);
			}

			string columns = parameterColumns.ToClause();
			string values = parameterColumns.ToClause(prefix: "@");
			string sql = GenerateInsert(SqlTemplates.Insert, entity, columns, values);
			sql += ";";

			if (returnId)
			{
				string selectId = ExtractReturnId(keyColumns);
				sql += " " + selectId;
			}

			return sql;
		}

		private static string GenerateInsert(string template, string entity, string columns, string values)
		{
			return template.Replace("{{table}}", entity, StringComparison.OrdinalIgnoreCase)
						   .Replace("{{columns}}", columns, StringComparison.OrdinalIgnoreCase)
						   .Replace("{{values}}", values, StringComparison.OrdinalIgnoreCase);
		}

		private static string ExtractReturnId(IList<string> keyColumns)
		{
			if (keyColumns.Count != 1)
				throw new InvalidOperationException("Entity must have exactly one key column for this to work");

			string selectId = SqlDialect switch
			{
				Dialect.Postgres => SqlTemplates.Postgres.InsertedPrimaryKey.Replace("{{id}}", keyColumns[0], StringComparison.OrdinalIgnoreCase),
				Dialect.SqlLite => SqlTemplates.Sqlite.InsertedPrimaryKey,
				Dialect.MySql => SqlTemplates.MySql.InsertedPrimaryKey,
				Dialect.SqlServer => SqlTemplates.SqlServer.InsertedPrimaryKey,
				_ => SqlTemplates.SqlServer.InsertedPrimaryKey
			};

			return selectId;
		}
	}
}