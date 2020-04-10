using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Jaunty
{
	public static partial class Jaunty
	{
		public static Dialect SqlDialect { get; set; }

		public delegate string TableNameMapperDelegate(Type type);
		public static TableNameMapperDelegate TableNameMapper;

		public delegate string TableNameFormatterDelegate(string name);
		public static TableNameFormatterDelegate TableNameFormatter;

		public delegate string ColumnNameFormatterDelegate(string name);
		public static ColumnNameFormatterDelegate ColumnNameFormatter;

		public delegate string ParameterFormatterDelegate(string name);
		public static ParameterFormatterDelegate ParameterFormatter;

		public delegate string PluralizeDelegate(string name);
		public static PluralizeDelegate Pluralize;

		public enum ClauseType
		{
			Insert = 1,
			Select = 2,
			Update = 3,
			Delete = 4
		}

		public enum Dialect
		{
			Empty = 0,
			SqlServer = 1,
			Postgres = 2,
			SqlLite = 3,
			MySql = 4
		}

		public enum SortOrder
		{
			Ascending,
			Descending
		}

		internal static string ToSqlString(this ComparisonOperator oper)
		{
			switch (oper)
			{
				case ComparisonOperator.EqualTo:
					return "=";
				case ComparisonOperator.GreaterThan:
					return ">";
				case ComparisonOperator.LessThan:
					return "<";
				case ComparisonOperator.GreaterThanOrEqualTo:
					return ">=";
				case ComparisonOperator.LessThanOrEqualTo:
					return "<=";
				case ComparisonOperator.NotEqualTo:
					return "<>";
				default:
					return "";
			}
		}

		internal static ComparisonOperator ToComparisonOperator(this string o)
		{
			switch (o)
			{
				case "=":
					return ComparisonOperator.EqualTo;
				case ">":
					return ComparisonOperator.GreaterThan;
				case "<":
					return ComparisonOperator.LessThan;
				case ">=":
					return ComparisonOperator.GreaterThanOrEqualTo;
				case "<=":
					return ComparisonOperator.LessThanOrEqualTo;
				case "<>":
				case "!=":
					return ComparisonOperator.NotEqualTo;
				default:
					return ComparisonOperator.Empty;
			}
		}

		internal static string ToSqlString(this Separator separator)
		{
			switch (separator)
			{
				case Separator.And:
					return "AND";
				case Separator.Or:
					return "OR";
				default:
					throw new Exception("Not a valid separator");
			}
		}

		internal static Separator ToSeparator(this string s)
		{
			switch (s)
			{
				case "AND":
					return Separator.And;
				case "OR":
					return Separator.Or;
				default:
					return Separator.Empty;
			}
		}

		internal static string ToSqlString(this JoinType joinType)
		{
			switch (joinType)
			{
				case JoinType.InnerJoin:
					return "INNER JOIN";
				case JoinType.LeftOuterJoin:
					return "LEFT OUTER JOIN";
				case JoinType.RightOuterJoin:
					return "RIGHT OUTER JOIN";
			}

			return null;
		}

		[Flags]
		internal enum ComparisonOperator
		{
			Empty = 0,
			EqualTo = 1,
			GreaterThan = 2,
			LessThan = 4,
			GreaterThanOrEqualTo = GreaterThan | EqualTo,
			LessThanOrEqualTo = LessThan | EqualTo,
			NotEqualTo = 8
		}

		internal enum LogicalOperator
		{
			All,
			And,
			Any,
			Between,
			Exists,
			In,
			Like,
			Not,
			Or,
			Some
		}

		internal enum Separator
		{
			Empty = 0,
			And = 1,
			Or = 2,
			Not = 3
		}

		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> _propertiesCache =
			new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();

		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IDictionary<string, PropertyInfo>> _keysCache =
			new ConcurrentDictionary<RuntimeTypeHandle, IDictionary<string, PropertyInfo>>();

		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IDictionary<string, PropertyInfo>> _columnsCache =
			new ConcurrentDictionary<RuntimeTypeHandle, IDictionary<string, PropertyInfo>>();

		private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> _tableNamesCache =
			new ConcurrentDictionary<RuntimeTypeHandle, string>();

		private static readonly ConcurrentDictionary<string, string> _queriesCache = new ConcurrentDictionary<string, string>();

		internal static Type GetType(Type type)
		{
			if (type.IsArray)
			{
				type = type.GetElementType();
			}
			else if (type.IsGenericType)
			{
				TypeInfo typeInfo = type.GetTypeInfo();

				if (typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>))
				{
					type = type.GetGenericArguments()[0];
				}

				for (var i = 0; i < typeInfo.ImplementedInterfaces.Count(); i++)
				{
					Type implementedInterface = typeInfo.ImplementedInterfaces.ElementAt(i);

					if (implementedInterface.IsGenericType
						&& implementedInterface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
					{
						type = type.GetGenericArguments()[0];
					}
				}
			}

			return type;
		}

		internal static string GetTypeName(Type type)
		{
			if (_tableNamesCache.TryGetValue(type.TypeHandle, out string tableNamesCache))
			{
				return tableNamesCache;
			}

			if (TableNameMapper != null)
			{
				string name = TableNameMapper(type);
				_tableNamesCache[type.TypeHandle] = name;
				return name;
			}

			string tableSchema = type.GetCustomAttribute<TableAttribute>(false)?.Schema ??
								 type.GetCustomAttribute<
									 System.ComponentModel.DataAnnotations.Schema.TableAttribute>(false)?.Schema;

			tableSchema = EscapeSqlKeywords(tableSchema);
			string tableName = type.GetCustomAttribute<TableAttribute>(false)?.Name ??
									   type.GetCustomAttribute<
										   System.ComponentModel.DataAnnotations.Schema.TableAttribute>(false)?.Name;

			if (string.IsNullOrWhiteSpace(tableName))
			{
				tableName = type.Name;
				tableName = Pluralize?.Invoke(tableName) ?? tableName;
				tableName = TableNameFormatter?.Invoke(tableName) ?? tableName;
			}

			tableName = EscapeSqlKeywords(tableName);
			tableName = !string.IsNullOrWhiteSpace(tableSchema) ? $"{tableSchema}.{tableName}" : tableName;

			_tableNamesCache[type.TypeHandle] = tableName;
			return tableName;
		}

		internal static List<PropertyInfo> GetPropertiesCache(Type type)
		{
			if (_propertiesCache.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> propertiesCache))
			{
				return propertiesCache.ToList();
			}

			PropertyInfo[] instanceProperties = type.GetProperties(BindingFlags.Public
														| BindingFlags.NonPublic
														| BindingFlags.Instance);
			var properties = new List<PropertyInfo>();

			foreach (PropertyInfo property in instanceProperties)
			{
				if (property is { CanRead: true, CanWrite: true })
				{
					if (property.IsNotIgnored())
						properties.Add(property);
				}
			}

			_propertiesCache[type.TypeHandle] = properties;
			return properties;
		}

		private static bool IsNotIgnored(this PropertyInfo property)
		{
			return !property.IsIgnored();
		}

		private static bool IsIgnored(this PropertyInfo property)
		{
			foreach (object attribute in property.GetCustomAttributes(false))
			{
				if (attribute is IgnoreAttribute || attribute is NotMappedAttribute)
				{
					return true;
				}
			}

			return false;
		}

		private static IDictionary<string, PropertyInfo> GetKeysCache(Type type)
		{
			if (_keysCache.TryGetValue(type.TypeHandle, out IDictionary<string, PropertyInfo> keysCache))
			{
				return keysCache;
			}

			List<PropertyInfo> properties = GetPropertiesCache(type);
			var keys = new Dictionary<string, PropertyInfo>(); ;

			foreach (PropertyInfo property in properties)
			{
				if (property.IsKey())
					keys.Add(property.Name, property);
			}

			if (keys.Count == 0)
			{
				foreach (PropertyInfo property in properties)
				{
					string columnName = GetColumnName(property);

					if (HasIdInColumnName(type, columnName))
					{
						keys.Add(columnName, property);
					}
				}
			}

			_keysCache[type.TypeHandle] = keys;
			return keys;
		}

		private static bool IsKey(this PropertyInfo property)
		{
			var attributes = property.GetCustomAttributes(false);

			foreach (var attribute in attributes)
			{
				switch (attribute)
				{
					case KeyAttribute _:
					case System.ComponentModel.DataAnnotations.KeyAttribute _:
						return true;
				}
			}

			return false;
		}

		private static bool HasIdInColumnName(Type type, string columnName)
		{
			return columnName.Equals("id", StringComparison.CurrentCultureIgnoreCase)
				   || columnName.Equals(type.Name + "id", StringComparison.CurrentCultureIgnoreCase)
				   || columnName.Equals(type.Name + "_" + "id", StringComparison.CurrentCultureIgnoreCase);
		}

		internal static IDictionary<string, PropertyInfo> GetColumnsCache(Type type)
		{
			if (_columnsCache.TryGetValue(type.TypeHandle, out IDictionary<string, PropertyInfo> columnsCache))
			{
				return columnsCache;
			}

			var columns = new Dictionary<string, PropertyInfo>();
			List<PropertyInfo> propertyInfos = GetPropertiesCache(type);

			foreach (PropertyInfo property in propertyInfos)
			{
				string columnName = GetColumnName(property);
				columns.Add(columnName, property);
			}

			_columnsCache[type.TypeHandle] = columns;
			return columns;
		}

		private static IList<string> GetNonKeyColumnsCache(Type type)
		{
			var allColumns = GetColumnsCache(type);
			var keys = GetKeysCache(type).Keys.ToList();
			var nonKeyColumns = allColumns.Keys.ToList().Clone();

			foreach (var key in keys)
				nonKeyColumns.Remove(key);

			return nonKeyColumns;
		}

		private static string GetColumnName(PropertyInfo property)
		{
			string columnName = GetColumnNameFromAttribute(property);

			if (string.IsNullOrWhiteSpace(columnName))
			{
				columnName = property.Name;
				columnName = ColumnNameFormatter?.Invoke(columnName) ?? columnName;
			}

			columnName = EscapeSqlKeywords(columnName);
			return columnName;
		}

		private static string GetColumnNameFromAttribute(PropertyInfo property)
		{
			var attributes = property.GetCustomAttributes(false);

			foreach (var attribute in attributes)
			{
				switch (attribute)
				{
					case ColumnAttribute columnAttribute:
						return columnAttribute.Name;
					case System.ComponentModel.DataAnnotations.Schema.ColumnAttribute columnAttribute2:
						return columnAttribute2.Name;
				}
			}

			return null;
		}

		private static string EscapeSqlKeywords(string word)
		{
			if (string.IsNullOrWhiteSpace(word)) return word;

			return !SqlKeywords.All.Contains(word, StringComparison.CurrentCultureIgnoreCase)
				&& !word.Contains(" ", StringComparison.CurrentCultureIgnoreCase)
					? word
					: (SqlDialect switch
					{
						Dialect.MySql => SqlTemplates.MySql.Identifier.Replace("{{name}}", word, StringComparison.CurrentCultureIgnoreCase),
						Dialect.Postgres => SqlTemplates.Postgres.Identifier.Replace("{{name}}", word, StringComparison.CurrentCultureIgnoreCase),
						Dialect.SqlLite => SqlTemplates.Sqlite.Identifier.Replace("{{name}}", word, StringComparison.CurrentCultureIgnoreCase),
						Dialect.SqlServer => SqlTemplates.SqlServer.Identifier.Replace("{{name}}", word, StringComparison.CurrentCultureIgnoreCase),
						_ => SqlTemplates.SqlServer.Identifier.Replace("{{name}}", word, StringComparison.CurrentCultureIgnoreCase)
					});
		}

		internal static void ExtractClause(string name, string oper, object value,
			StringBuilder clause, Action<string, object> whileExtracting = null)
		{
			string parameterName = null;

			if (!string.IsNullOrEmpty(name))
			{
				name = ColumnNameFormatter?.Invoke(name) ?? name;
				parameterName = ParameterFormatter?.Invoke(name) ?? $"@{name}";
			}

			clause.Append($"{name} {oper} {parameterName}");

			if (value != null)
			{
				whileExtracting?.Invoke(name, value);
			}
		}

		private static IDictionary<string, object> ConvertToParameters<T>(IEnumerable<T> entities)
		{
			Type type = GetType(typeof(T));
			List<PropertyInfo> properties = GetPropertiesCache(type);
			IDictionary<string, PropertyInfo> columnNames = GetColumnsCache(type);
			var parameters = new Dictionary<string, object>();
			var list = entities.ToList();

			for (var i = 0; i < list.Count; i++)
			{
				foreach (KeyValuePair<string, PropertyInfo> column in columnNames)
				{
					parameters.Add(column.Key + i, column.Value.GetValue(list[i]));
				}
			}

			return parameters;
		}

		private static IDictionary<string, object> ConvertToParameters<T>(T entity)
		{
			Type type = GetType(entity.GetType());
			IDictionary<string, PropertyInfo> columnNames = GetColumnsCache(type);
			var parameters = new Dictionary<string, object>();

			foreach (KeyValuePair<string, PropertyInfo> column in columnNames)
			{
				parameters.Add(column.Key, column.Value.GetValue(entity));
			}

			return parameters;
		}
	}
}