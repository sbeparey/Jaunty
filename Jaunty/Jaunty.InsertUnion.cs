using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Dapper;

namespace Jaunty
{
	public static partial class Jaunty
	{
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> insertUnionQueryCache = new ConcurrentDictionary<RuntimeTypeHandle, string>();

		/// <summary>
		/// Inserts multiple entities using union insert.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entities">The entities to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns the number of rows inserted</returns>
		/// <exception cref="ArgumentNullException">entities</exception>
		public static int InsertUnion<T>(this IDbConnection connection, IEnumerable<T> entities, object token = null, IDbTransaction transaction = null)
		{
			if (entities is null)
			{
				throw new ArgumentNullException(nameof(entities));
			}

			var parameters = ConvertToParameters(entities);
			string sql = BuildInsertUnionSql(entities);
			var eventArgs = new SqlEventArgs {Sql = sql, Parameters = parameters};
			OnInserting?.Invoke(token, eventArgs);
			return connection.Execute(sql, parameters, transaction);
		}

		#region async

		/// <summary>
		/// Inserts multiple entities using union insert asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entities">The entities to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns the number of rows inserted</returns>
		/// <exception cref="ArgumentNullException">entities</exception>
		public static async Task<int> InsertUnionAsync<T>(this IDbConnection connection, IEnumerable<T> entities, object token = null, IDbTransaction transaction = null)
		{
			if (entities is null)
			{
				throw new ArgumentNullException(nameof(entities));
			}

			var parameters = ConvertToParameters(entities);
			string sql = BuildInsertUnionSql(entities);
			var eventArgs = new SqlEventArgs {Sql = sql, Parameters = parameters};
			OnInserting?.Invoke(token, eventArgs);
			return await connection.ExecuteAsync(sql, parameters, transaction);
		}

		#endregion

		private static string BuildInsertUnionSql<T>(IEnumerable<T> entities)
		{
			Type type = GetType(typeof(T));

			if (insertUnionQueryCache.TryGetValue(type.TypeHandle, out string query))
			{
				return query;
			}

			var keys = new List<string>(GetKeysCache(type).Keys);
			var columns = new List<string>(GetColumnsCache(type).Keys);
			columns.Reduce(keys);
			string sql = SqlTemplates.InsertByUnion.Trim().Replace("{{table}}", GetTableName(type))
														  .Replace("{{columns}}", columns.ToClause());

			// Good time to cache
			insertUnionQueryCache[type.TypeHandle] = sql;
			string selectUnion = BuildSelectUnion(columns, entities.Count());
			return sql.Replace("{{select}}", selectUnion);
		}

		private static string BuildSelectUnion(IEnumerable<string> columns, int count)
		{
			var sql = new StringBuilder();

			for (int i = 0; i < count; i++)
			{
				string values = columns.ToList().ToClause(prefix: "@", suffix: i.ToString());
				sql.AppendIf(i > 0, "UNION ALL ");
				sql.Append($"SELECT {values} \n");
			}

			return sql.ToString();
		}
	}
}
