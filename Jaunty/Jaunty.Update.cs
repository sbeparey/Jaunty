using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Linq;

using Dapper;

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
		public static bool Update<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null)
		{
			if (entity is null)
			{
				throw new ArgumentNullException(nameof(entity));
			}

			string sql = BuildUpdateSql<T>();
			OnUpdating?.Invoke(new SqlEventArgs { Sql = sql });
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
		public static async Task<bool> UpdateAsync<T>(this IDbConnection connection, T entity, IDbTransaction transaction = null)
		{
			if (entity is null)
			{
				throw new ArgumentNullException(nameof(entity));
			}

			string sql = BuildUpdateSql<T>();
			OnUpdating?.Invoke(new SqlEventArgs { Sql = sql });
			int rowsAffected = await connection.ExecuteAsync(sql, entity, transaction);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Updates the specified entity.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="conditionalClause"></param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns number of rows affected.</returns>
		public static int Update<T>(this Condition conditionalClause, IDbTransaction transaction = null)
		{
			IDictionary<string, object> parameters = conditionalClause.GetParameters();
			string sql = BuildUpdateSql<T>(conditionalClause);
			OnUpdating?.Invoke(new SqlEventArgs { Sql = sql, Parameters = parameters });
			return conditionalClause.Connection.Execute(sql, parameters, transaction);
		}

		/// <summary>
		/// Updates the specified entity asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="conditionalClause"></param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns number of rows affected.</returns>
		public static async Task<int> UpdateAsync<T>(this Condition conditionalClause, IDbTransaction transaction = null)
		{
			IDictionary<string, object> parameters = conditionalClause.GetParameters();
			string sql = BuildUpdateSql<T>(conditionalClause);
			OnUpdating?.Invoke(new SqlEventArgs { Sql = sql });
			return await conditionalClause.Connection.ExecuteAsync(sql, parameters, transaction);
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
			var setClause = new SetClause(connection);
			setClause.Add(column, value);
			return setClause;
		}

		/// <summary>
		/// Sets the column.
		/// </summary>
		/// <typeparam name="T">The type of value for the column.</typeparam>
		/// <param name="setClause">The SetClause.</param>
		/// <param name="column">The column.</param>
		/// <param name="value">The value.</param>
		/// <returns>A SetClause</returns>
		public static SetClause Set<T>(this SetClause setClause, string column, T value)
		{
			setClause.Add(column, value);
			return setClause;
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

			var setClause = new SetClause(connection);
			expression.Body.WalkThrough((name, _, value) => setClause.Add(name, value));
			return setClause;
		}

		/// <summary>
		/// Sets the column.
		/// </summary>
		/// <typeparam name="T">The type of value for the column.</typeparam>
		/// <param name="setClause"></param>
		/// <param name="expression">The linq expression.</param>
		/// <returns>A SetClause</returns>
		public static SetClause Set<T>(this SetClause setClause, Expression<Func<T, bool>> expression)
		{
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			expression.Body.WalkThrough((name, _, value) => setClause.Add(name, value));
			return setClause;
		}

		private static string BuildUpdateSql<T>(Condition conditionClause = null)
		{
			Type type = GetType(typeof(T));
			var columnsList = conditionClause is null ? GetColumnsCache(type).Keys.ToList() : null;
			var keyColumnsList = conditionClause is null ? GetKeysCache(type).Keys.ToList() : null;
			columnsList?.Reduce(keyColumnsList);
			string setClause = conditionClause is null ? columnsList.ToSetClause() : conditionClause.GetSetClause();
			string whereClause = conditionClause is null ? keyColumnsList.ToWhereClause() : conditionClause.ToSql();
			return SqlTemplates.UpdateWhere.Replace("{{table}}", GetTypeName(type), StringComparison.OrdinalIgnoreCase)
										   .Replace("{{columns}}", setClause, StringComparison.OrdinalIgnoreCase)
										   .Replace("{{where}}", whereClause, StringComparison.OrdinalIgnoreCase);
		}
	}
}
