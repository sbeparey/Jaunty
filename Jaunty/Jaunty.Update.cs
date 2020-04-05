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
		public static Action<ISqlEventArgs> OnUpdating;

		/// <summary>
		/// Updates the specified entity.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to update.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <c>true</c> if only one row is updated.</returns>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static bool Update<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			if (entity is null)
				throw new ArgumentNullException(nameof(entity));

			var parameters = ConvertToParameters(entity);
			string sql = Update<T>(parameters, ticket, true);
			int rowsAffected = connection.Execute(sql, entity, transaction);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Updates the specified entity asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="entity">The entity to update.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <c>true</c> if only one row is updated.</returns>
		/// <exception cref="ArgumentNullException">entity</exception>
		public static async Task<bool> UpdateAsync<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null, ITicket ticket = null)
		{
			if (connection is null)
				throw new ArgumentNullException(nameof(connection));

			if (entity is null)
				throw new ArgumentNullException(nameof(entity));

			var parameters = ConvertToParameters(entity);
			string sql = Update<T>(parameters, ticket, true);
			int rowsAffected = await connection.ExecuteAsync(sql, entity, transaction).ConfigureAwait(false);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Updates the specified entity.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="condition"></param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns number of rows affected.</returns>
		public static int Update<T>(this Condition condition, IDbTransaction transaction = null, ITicket ticket = null)
		{
			IDictionary<string, object> parameters = condition?.GetParameters();
			string sql = Update<T>(condition, parameters, ticket, true);
			return condition.Connection.Execute(sql, parameters, transaction);
		}

		/// <summary>
		/// Updates the specified entity asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="conditionalClause"></param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns number of rows affected.</returns>
		public static async Task<int> UpdateAsync<T>(this Condition condition, IDbTransaction transaction = null, ITicket ticket = null)
		{
			IDictionary<string, object> parameters = condition?.GetParameters();
			string sql = Update<T>(condition, parameters, ticket, true);
			return await condition.Connection.ExecuteAsync(sql, parameters, transaction).ConfigureAwait(false);
		}

		/// <summary>
		/// Sets the column.
		/// </summary>
		/// <typeparam name="T">The type of value for the column.</typeparam>
		/// <param name="connection">The connection query on.</param>
		/// <param name="column">The column.</param>
		/// <param name="value">The value.</param>
		/// <returns>A SetClause</returns>
		public static SetClause Set<T>(this IDbConnection connection, string column, T value)
		{
			var set = new SetClause(connection);
			set.Add(column, value);
			return set;
		}

		/// <summary>
		/// Sets the column.
		/// </summary>
		/// <typeparam name="T">The type of value for the column.</typeparam>
		/// <param name="set">The SetClause.</param>
		/// <param name="column">The column.</param>
		/// <param name="value">The value.</param>
		/// <returns>A SetClause</returns>
		public static SetClause Set<T>(this SetClause set, string column, T value)
		{
			if (set is null)
				throw new ArgumentNullException(nameof(set));

			set.Add(column, value);
			return set;
		}

		/// <summary>
		/// Sets the column.
		/// </summary>
		/// <typeparam name="T">The type of value for the column.</typeparam>
		/// <param name="connection">The connection query on.</param>
		/// <param name="expression">The linq expression.</param>
		/// <returns>A SetClause</returns>
		public static SetClause Set<T>(this IDbConnection connection, Expression<Func<T, bool>> expression)
		{
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			var set = new SetClause(connection);
			expression.Body.WalkThrough((name, _, value) => set.Add(name, value));
			return set;
		}

		/// <summary>
		/// Sets the column.
		/// </summary>
		/// <typeparam name="T">The type of value for the column.</typeparam>
		/// <param name="set"></param>
		/// <param name="expression">The linq expression.</param>
		/// <returns>A SetClause</returns>
		public static SetClause Set<T>(this SetClause set, Expression<Func<T, bool>> expression)
		{
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			expression.Body.WalkThrough((name, _, value) => set.Add(name, value));
			return set;
		}

		private static string Update<T>(IDictionary<string, object> parameters, ITicket ticket = null, bool triggerEvent = false)
		{
			if (parameters is null)
				throw new ArgumentNullException(nameof(parameters));

			if (parameters.Count <= 0)
				throw new ArgumentOutOfRangeException(nameof(parameters));

			string sql = ticket is null
						? BuildSql<T>(ClauseType.Update, parameters)
						: _queriesCache.GetOrAdd(ticket.Id, q => BuildSql<T>(ClauseType.Update, parameters));

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnUpdating?.Invoke(eventArgs);
			return sql;
		}

		private static string Update<T>(Clause clause, IDictionary<string, object> parameters, ITicket ticket = null, bool triggerEvent = false)
		{
			if (clause is null)
				throw new ArgumentNullException(nameof(clause));

			if (parameters is null)
				throw new ArgumentNullException(nameof(parameters));

			string sql = ticket is null
						? ExtractSql<T>(ClauseType.Update, clause)
						: _queriesCache.GetOrAdd(ticket.Id, q => ExtractSql<T>(ClauseType.Update, clause));

			if (!triggerEvent)
				return sql;

			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnUpdating?.Invoke(eventArgs);
			return sql;
		}
	}
}
