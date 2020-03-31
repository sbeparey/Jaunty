using Dapper;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns the number of rows inserted</returns>
		/// <exception cref="ArgumentNullException">entities</exception>
		public static int InsertUnion<T>(this IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			var parameters = ConvertToParameters(entities);
			string sql = InsertUnion(entities, parameters, ticket, true);
			return connection.Execute(sql, parameters, transaction);
		}

		/// <summary>
		/// Inserts multiple entities using union insert asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entities">The entities to insert.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns the number of rows inserted</returns>
		/// <exception cref="ArgumentNullException">entities</exception>
		public static async Task<int> InsertUnionAsync<T>(this IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			var parameters = ConvertToParameters(entities);
			string sql = InsertUnion(entities, parameters, ticket, true);
			return await connection.ExecuteAsync(sql, parameters, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Generates SQL for Inserts multiple entities using union insert.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entities">The entities to insert.</param>
		/// <param name="ticket">An ITicket to uniquely id the query.</param>
		/// <returns>Returns string</returns>
		/// <exception cref="ArgumentNullException">entities</exception>
		public static string InsertUnion<T>(this IDbConnection connection, IEnumerable<T> entities, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			var parameters = ConvertToParameters(entities);
			return InsertUnion(entities, parameters, ticket);
		}

		private static string InsertUnion<T>(IEnumerable<T> entities, IDictionary<string, object> parameters, ITicket ticket = null, bool triggerEvent = false)
		{
			if (entities is null)
				throw new ArgumentNullException(nameof(entities));

			if (parameters is null)
				throw new ArgumentNullException(nameof(parameters));

			if (parameters.Count <= 0)
				throw new ArgumentOutOfRangeException(nameof(parameters));

			string sql = ticket is null
						? BuildInsertUnionSql(entities)
						: _queriesCache.GetOrAdd(ticket.Id, q => BuildInsertUnionSql(entities));

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnInserting?.Invoke(ticket, eventArgs);
			return sql;
		}

		private static string BuildInsertUnionSql<T>(IEnumerable<T> entities)
		{
			Type type = GetType(typeof(T));
			var keys = new List<string>(GetKeysCache(type).Keys);
			var columns = new List<string>(GetColumnsCache(type).Keys);
			columns.Reduce(keys);

			string sql = SqlTemplates.InsertByUnion.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase)
												   .Replace("{{columns}}", columns.ToClause(), StringComparison.OrdinalIgnoreCase);

			string selectUnion = BuildSelectUnion(columns, entities.Count());
			return sql.Replace("{{select}}", selectUnion, StringComparison.OrdinalIgnoreCase);
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
