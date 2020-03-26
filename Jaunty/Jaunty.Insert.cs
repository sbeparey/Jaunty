using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Dapper;

namespace Jaunty
{
	public static partial class Jaunty
	{
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> insertQueryCache =
			new ConcurrentDictionary<RuntimeTypeHandle, string>();

		private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> insertSelectKeyQueryCache =
			new ConcurrentDictionary<RuntimeTypeHandle, string>();

		public static event EventHandler<SqlEventArgs> OnInserting;

		/// <summary>
		/// Inserts the specified entity.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <c>true</c> if only one row is inserted.</returns>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static bool Insert<T>(this IDbConnection connection, T entity, object token = null,
			IDbTransaction transaction = null)
		{
			if (entity is null)
			{
				throw new ArgumentNullException(nameof(entity));
			}

			string sql = BuildInsertSql<T>();
			var parameters = ConvertToParameters(entity);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnInserting?.Invoke(token, eventArgs);
			int rowsAffected = connection.Execute(sql, parameters, transaction);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Inserts the specified entity.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The type representing the primary key.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns the primary key of the entity once inserted successfully</returns>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static TKey Insert<T, TKey>(this IDbConnection connection, T entity, object token = null,
			IDbTransaction transaction = null)
		{
			if (entity is null)
			{
				throw new ArgumentNullException(nameof(entity));
			}

			string sql = BuildInsertSql<T>(true);
			var parameters = ConvertToParameters(entity);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnInserting?.Invoke(token, eventArgs);
			return connection.QuerySingle<TKey>(sql, parameters, transaction);
		}

		#region async

		/// <summary>
		/// Inserts the specified entity asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <c>true</c> if only one row is inserted.</returns>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static async Task<bool> InsertAsync<T>(this IDbConnection connection, T entity, object token = null,
			IDbTransaction transaction = null)
		{
			if (entity is null)
			{
				throw new ArgumentNullException(nameof(entity));
			}

			string sql = BuildInsertSql<T>();
			var parameters = ConvertToParameters(entity);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnInserting?.Invoke(token, eventArgs);
			int rowsAffected = await connection.ExecuteAsync(sql, parameters, transaction);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Inserts the specified entity asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The type representing the primary key.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns the primary key of the entity once inserted successfully</returns>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static async Task<TKey> InsertAsync<T, TKey>(this IDbConnection connection, T entity,
			object token = null, IDbTransaction transaction = null)
		{
			if (entity is null)
			{
				throw new ArgumentNullException(nameof(entity));
			}

			string sql = BuildInsertSql<T>(true);
			var parameters = ConvertToParameters(entity);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnInserting?.Invoke(token, eventArgs);
			return await connection.QuerySingleAsync<TKey>(sql, parameters, transaction);
		}

		#endregion

		private static string BuildInsertSql<T>(bool returnId = false)
		{
			Type type = GetType(typeof(T));

			if (!returnId && insertQueryCache.TryGetValue(type.TypeHandle, out string insertQuery))
			{
				return insertQuery;
			}

			if (returnId && insertSelectKeyQueryCache.TryGetValue(type.TypeHandle, out string insertSelectQuery))
			{
				return insertSelectQuery;
			}

			var keys = GetKeysCache(type);
			var columns = GetColumnsCache(type).Keys.ToList();

			if (keys.Count == 1 && AutoGenerated(keys))
			{
				columns.Reduce(keys.Keys.ToList());
			}

			return BuildInsertSql(type, columns, keys.ElementAt(0).Key, returnId);
		}

		private static bool AutoGenerated(IDictionary<string, PropertyInfo> keys)
		{
			var attributes = keys.ElementAt(0).Value.GetCustomAttributes(false);
			bool autoGenerated = true;

			foreach (object attribute in attributes)
			{
				if (attribute is KeyAttribute keyAttribute && keyAttribute.Manual ||
					attribute is DatabaseGeneratedAttribute generated &&
						generated.DatabaseGeneratedOption == DatabaseGeneratedOption.None)
				{
					autoGenerated = false;
					break;
				}
			}

			return autoGenerated;
		}

		private static string BuildInsertSql(Type type, IList<string> columns, string keyColumnName, bool returnId)
		{
			List<string> paramsList = columns.ForEach(x => ParameterFormatter?.Invoke(x) ?? $"@{x}");
			var sql = SqlTemplates.Insert.Trim().Replace("{{table}}", GetTypeName(type))
												.Replace("{{columns}}", columns.ToClause())
												.Replace("{{parameters}}", paramsList.ToClause());

			if (returnId)
			{
				string selectId = SqlDialect switch
				{
					Dialect.Postgres => SqlTemplates.Postgres.InsertedPrimaryKey.Replace("{{id}}", keyColumnName),
					Dialect.SqlLite => SqlTemplates.Sqlite.InsertedPrimaryKey,
					Dialect.MySql => SqlTemplates.MySql.InsertedPrimaryKey,
					Dialect.SqlServer => SqlTemplates.SqlServer.InsertedPrimaryKey,
					_ => SqlTemplates.SqlServer.InsertedPrimaryKey
				};

				sql = SqlDialect != Dialect.Postgres ? $"{sql}; {selectId}" : $"{sql} {selectId}";
			}
			else
			{
				sql += ";";
			}

			if (!returnId)
				insertQueryCache[type.TypeHandle] = sql;
			else
				insertSelectKeyQueryCache[type.TypeHandle] = sql;
			return sql;
		}
	}
}