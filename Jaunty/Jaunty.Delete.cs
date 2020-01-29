using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using Dapper;

namespace Jaunty
{
	public interface ISqlEventArgs
	{
		string Sql { get; set; }

		IDictionary<string, object> Parameters { get; set; }
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
		/// <returns>Returns <c>true</c> if only one row is deleted.</returns>
		public static bool Delete<T, TKey>(this IDbConnection connection, TKey key, object token = null, IDbTransaction transaction = null)
		{
			IDictionary<string, object> parameter = GetParameter<T, TKey>(key);
			string sql = BuildDeleteSql<T>();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameter };
			OnDeleting?.Invoke(token, eventArgs);
			int rowsAffected = connection.Execute(sql, parameter, transaction);
			return rowsAffected == 1;
		}

		// Todo: test
		public static bool Delete<T, TKey1, TKey2>(this IDbConnection connection, TKey1 key1, TKey2 key2, object token = null, IDbTransaction transaction = null)
		{
			IDictionary<string, object> parameter = GetParameters<T, TKey1, TKey2>(key1, key2);
			string sql = BuildDeleteSql<T>();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameter };
			OnDeleting?.Invoke(token, eventArgs);
			int rowsAffected = connection.Execute(sql, parameter, transaction);
			return rowsAffected == 1;
		}

		//public static bool Delete<T>(this IDbConnection connection, T entity, object token = null,
		//	IDbTransaction transaction = null)
		//{
		//	IDictionary<string, object> parameter = GetParameter<T>();
		//}

		/// <summary>
		/// Deletes by where
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="conditionalClause">The conditional clause.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Return number of rows affected.</returns>
		public static int Delete<T>(this ConditionalClause conditionalClause, object token = null, IDbTransaction transaction = null)
		{
			var parameters = conditionalClause.GetParameters();
			string sql = BuildDeleteSql<T>(conditionalClause);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(token, eventArgs);
			return conditionalClause.Connection.Execute(sql, parameters, transaction);
		}

		/// <summary>
		/// Deletes by expression
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="expression">The expression.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns number of rows affected.</returns>
		public static int Delete<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, object token = null, IDbTransaction transaction = null)
		{
			var parameters = new Dictionary<string, object>();
			string sql = BuildDeleteSql(expression, parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(token, eventArgs);
			return connection.Execute(sql, parameters, transaction);
		}

		#region async

		/// <summary>
		/// Deletes the specified key asynchronously.
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <typeparam name="TKey">The type of the primary key.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="key">The value of the primary key.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns <c>true</c> if only one row is deleted.</returns>
		public static async Task<bool> DeleteAsync<T, TKey>(this IDbConnection connection, TKey key, object token = null, IDbTransaction transaction = null)
		{
			var parameters = new Dictionary<string, object>();
			string sql = BuildDeleteSql<T>();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(token, eventArgs);
			int rowsAffected = await connection.ExecuteAsync(sql, key, transaction);
			return rowsAffected == 1;
		}

		public static async Task<bool> DeleteAsync<T, TKey1, TKey2>(this IDbConnection connection, TKey1 key1, TKey2 key2, object token = null, IDbTransaction transaction = null)
		{
			IDictionary<string, object> parameter = GetParameters<T, TKey1, TKey2>(key1, key2);
			string sql = BuildDeleteSql<T>();
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameter };
			OnDeleting?.Invoke(token, eventArgs);
			int rowsAffected = await connection.ExecuteAsync(sql, parameter, transaction);
			return rowsAffected == 1;
		}

		/// <summary>
		/// Deletes by where async
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="conditionalClause">The conditional clause.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Return number of rows affected.</returns>
		public static async Task<int> DeleteAsync<T>(this ConditionalClause conditionalClause, object token = null, IDbTransaction transaction = null)
		{
			var parameters = conditionalClause.GetParameters();
			string sql = BuildDeleteSql<T>(conditionalClause);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(token, eventArgs);
			return await conditionalClause.Connection.ExecuteAsync(sql, parameters, transaction);
		}

		/// <summary>
		/// Deletes by expression async
		/// </summary>
		/// <typeparam name="T">The type representing the database table.</typeparam>
		/// <param name="connection">The connection to query on.</param>
		/// <param name="expression">The expression.</param>
		/// <param name="transaction">The transaction (optional).</param>
		/// <returns>Returns number of rows affected.</returns>
		public static async Task<int> DeleteAsync<T>(this IDbConnection connection, Expression<Func<T, bool>> expression, object token = null, IDbTransaction transaction = null)
		{
			var parameters = new Dictionary<string, object>();
			string sql = BuildDeleteSql(expression, parameters);
			var eventArgs = new SqlEventArgs { Sql = sql, Parameters = parameters };
			OnDeleting?.Invoke(token, eventArgs);
			return await connection.ExecuteAsync(sql, parameters, transaction);
		}

		#endregion

		private static IDictionary<string, object> GetParameter<T, TKey>(TKey key)
		{
			Type type = GetType(typeof(T));
			IDictionary<string, PropertyInfo> keys = GetKeysCache(type);

			if (keys.Count > 1)
			{
				throw new ArgumentException("This entity has more than one key columns. Cannot use this method");
			}

			return new Dictionary<string, object> { { keys.ElementAt(0).Key, key } };
		}

		private static IDictionary<string, object> GetParameters<T, TKey1, TKey2>(TKey1 key1, TKey2 key2)
		{
			Type type = GetType(typeof(T));
			IDictionary<string, PropertyInfo> keys = GetKeysCache(type);

			if (keys.Count != 2)
			{
				throw new ArgumentException("This entity does not have two key columns. Cannot use this method");
			}

			return new Dictionary<string, object> { { keys.ElementAt(0).Key, key1 }, { keys.ElementAt(1).Key, key2 } };
		}

		private static IDictionary<string, object> GetParameters<T>(T entity)
		{
			Type type = GetType(typeof(T));
			IDictionary<string, PropertyInfo> keys = GetColumnsCache(type);

			if (keys.Count > 0)
			{

			}

			throw new NotImplementedException();
		}

		private static string BuildDeleteSql<T>()
		{
			Type type = GetType(typeof(T));
			var keyColumnsList = GetColumnsCache(type).Keys.ToList();
			return SqlTemplates.DeleteWhere.Trim().Replace("{{table}}", GetTableName(type))
													  .Replace("{{where}}", keyColumnsList.ToWhereClause());
		}

		private static string BuildDeleteSql<T>(IDictionary<string, object> parameters)
		{
			Type type = GetType(typeof(T));
			string tableName = GetTableName(type);
			return SqlTemplates.DeleteWhere.Trim().Replace("{{table}}", tableName)
													  .Replace("{{where}}", parameters.ToWhereClause());
		}

		private static string BuildDeleteSql<T>(ConditionalClause conditionalClause = null)
		{
			Type type = GetType(typeof(T));
			string tableName = GetTableName(type);
			string whereClause = conditionalClause is null
				? GetKeysCache(type).Keys.ToList().ToWhereClause()
				: conditionalClause.ToWhereClause();
			return SqlTemplates.DeleteWhere.Trim().Replace("{{table}}", tableName)
													  .Replace("{{where}}", whereClause);
		}

		public static string BuildDeleteSql<T>(Expression<Func<T, bool>> expression, IDictionary<string, object> parameters)
		{
			Type type = GetType(typeof(T));
			var whereClause = new StringBuilder();
			expression.Body.WalkThrough((n, o, v) => ExtractClause(n, o, v, whereClause, parameters.Add));
			return SqlTemplates.DeleteWhere.Trim().Replace("{{table}}", GetTableName(type))
													  .Replace("{{where}}", whereClause.ToString());
		}
	}
}
