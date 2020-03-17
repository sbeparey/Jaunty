using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Jaunty
{
	public static class Extensions
	{
		public static StringBuilder AppendIf(this StringBuilder builder, bool condition, string text)
		{
			return condition ? builder?.Append(text) : builder;
		}

		internal static StringBuilder AppendIfPresent(this StringBuilder builder, string candidate, string text)
		{
			return builder.AppendIf(!string.IsNullOrWhiteSpace(candidate), text);
		}

		internal static StringBuilder Prepend(this StringBuilder builder, string text)
		{
			return builder.Insert(0, text);
		}

		internal static StringBuilder PrependIf(this StringBuilder builder, bool condition, string text)
		{
			return condition ? builder?.Prepend(text) : builder;
		}

		internal static StringBuilder PrependBefore(this StringBuilder builder, string before, string text)
		{
			string temp = builder.ToString();
			return builder.Insert(temp.IndexOf(before, StringComparison.CurrentCultureIgnoreCase), text);
		}

		internal static StringBuilder InsertBefore(this StringBuilder builder, string before, string text)
		{
			string temp = builder.ToString();
			int index = temp.IndexOf(before, StringComparison.CurrentCultureIgnoreCase);
			return builder.Insert(index, text);
		}

		internal static Dictionary<TKey, TValue> AddIf<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, bool condition, TKey key, TValue value)
		{
			if (condition)
				dictionary.Add(key, value);
			return dictionary;
		}

		internal static bool IsNullOrEmpty(this string text)
		{
			return string.IsNullOrEmpty(text);
		}

		internal static bool IsNullOrWhiteSpace(this string text)
		{
			return string.IsNullOrWhiteSpace(text);
		}

		internal static bool NotNull(this string text)
		{
			return text != null;
		}

		internal static bool NotNullOrEmpty(this string text)
		{
			return !string.IsNullOrEmpty(text);
		}

		internal static bool NotNullOrWhiteSpace(this string text)
		{
			return !string.IsNullOrWhiteSpace(text);
		}

		internal static string InsertBefore(this string originalText, string candidate, string toInsertText)
		{
			if (originalText.IsNullOrWhiteSpace())
				throw new ArgumentNullException(nameof(originalText));

			if (candidate.IsNullOrWhiteSpace())
				throw new ArgumentNullException(nameof(candidate));

			int index = originalText.IndexOf(candidate, StringComparison.CurrentCulture);

			if (index == -1)
				throw new ArgumentOutOfRangeException(nameof(candidate));

			return originalText.Insert(index, toInsertText);
		}

		internal static string InsertAfter(this string originalText, string candidate, string toInsertText)
		{
			if (originalText.IsNullOrWhiteSpace())
				throw new ArgumentNullException(nameof(originalText));

			if (candidate.IsNullOrWhiteSpace())
				throw new ArgumentNullException(nameof(candidate));

			int index = originalText.IndexOf(candidate, StringComparison.CurrentCulture);

			if (index == -1)
				throw new ArgumentOutOfRangeException(nameof(candidate));

			return originalText.Insert(index + candidate.Length, toInsertText);
		}

		internal static List<string> Prepend(this IList<string> list, string prefix)
		{
			var modifiedList = new List<string>();

			for (var i = 0; i < list.Count; i++) modifiedList.Add(prefix + list[i]);

			return modifiedList;
		}

		internal static bool IsNullOrDefault(this object o)
		{
			switch (o)
			{
				case null:
				case long l when l == 0:
				case ulong ul when ul == 0:
				case int i when i == 0:
				case uint ui when ui == 0:
				case short s when s == 0:
				case ushort us when us == 0:
				case byte b when b == 0:
				case sbyte sb when sb == 0:
				case double d when d == 0d:
				case float f when f == 0f:
				case decimal de when de == 0m:
					return true;
				default:
					return false;
			}
		}

		internal static bool IsAnonymousType(this Type type)
		{
			var hasCompilerGeneratedAttribute =
				type.GetCustomAttributes(typeof(CompilerGeneratedAttribute), false).Any();
			var nameContainsAnonymousType = type.FullName != null && type.FullName.Contains("AnonymousType");
			return hasCompilerGeneratedAttribute && nameContainsAnonymousType;
		}

		internal static void Reduce(this IList<string> list, IList<string> items)
		{
			for (var i = 0; i < items.Count; i++) list.Remove(items[i]);
		}

		internal static List<T> ForEach<T>(this IList<T> list, Func<T, T> callback)
		{
			var modifiedList = new List<T>();

			for (var i = 0; i < list.Count; i++)
				if (!(callback is null))
					modifiedList.Add(callback.Invoke(list[i]));

			return modifiedList.Count == 0 ? null : modifiedList;
		}

		internal static Dictionary<string, object> ToDictionary(this object nameValuePairs,
			Action<int, int, string, object> whileAdding = null)
		{
			if (nameValuePairs is null) throw new ArgumentNullException(nameof(nameValuePairs));

			var type = nameValuePairs.GetType();

			if (!type.IsAnonymousType())
				throw new ArgumentException($"{nameof(nameValuePairs)} must be an anonymous type.");

			var properties = type.GetProperties();
			var dictionary = new Dictionary<string, object>();
			string name;
			object value;

			for (var i = 0; i < properties.Length; i++)
			{
				name = properties[i].Name;
				value = properties[i].GetValue(nameValuePairs);
				whileAdding?.Invoke(i, properties.Length, name, value);
				dictionary.Add(name, value);
			}

			return dictionary.Count > 0 ? dictionary : null;
		}


		internal static string ToSetClause(this IDictionary<string, object> dictionary)
		{
			return ToSetClause(new List<string>(dictionary.Keys));
		}

		internal static string ToWhereClause(this IDictionary<string, object> dictionary,
			IList<Jaunty.ComparisonOperator> operators = null)
		{
			return ToWhereClause(new List<string>(dictionary.Keys), operators);
		}

		internal static string ToWhereClause(this IDictionary<string, string> dictionary,
			IList<Jaunty.ComparisonOperator> operators = null)
		{
			return ToWhereClause(new List<string>(dictionary.Values), operators);
		}

		internal static string ToClause(this IDictionary<string, string> columns)
		{
			return ToClause(new List<string>(columns.Values));
		}

		//internal static string ToClause(this IList<string> columnsList)
		//{
		//	return columnsList.ToClause(", ");
		//}

		internal static string ToClause(this IList<string> list, string delimiter = ", ", string prefix = null, string suffix = null)
		{
			var clause = new StringBuilder();

			for (var i = 0; i < list.Count; i++)
			{
				clause.Append($"{prefix}{list[i]}{suffix}");

				if (i < list.Count - 1) clause.Append(delimiter);
			}

			return clause.ToString();
		}

		internal static string ToSetClause(this IList<string> columnsList)
		{
			return columnsList.ToWhereClause(", ");
		}

		internal static string ToWhereClause(this IList<string> columnsList,
			IList<Jaunty.ComparisonOperator> operators = null)
		{
			return columnsList.ToWhereClause(" AND ", operators);
		}

		internal static string ToWhereClause(this IList<string> columnsList, string delimiter,
			IList<Jaunty.ComparisonOperator> operators = null)
		{
			var clause = new StringBuilder();

			if (operators != null && operators.Count != columnsList.Count)
				throw new ArgumentException(
					$"{nameof(columnsList)} must have the same number of elements as {nameof(operators)}");

			for (var i = 0; i < columnsList.Count; i++)
			{
				var parameterName = Jaunty.ParameterFormatter?.Invoke(columnsList[i]) ?? $"@{columnsList[i]}";
				columnsList[i] = columnsList[i].Replace("#", ".", StringComparison.CurrentCultureIgnoreCase);
				columnsList[i] = columnsList[i].Replace("$", "", StringComparison.CurrentCultureIgnoreCase);

				if (operators != null)
					clause.Append($"{columnsList[i]} {operators[i].ToSqlString()} {parameterName}");
				else
					clause.Append($"{columnsList[i]} = {parameterName}");

				if (i < columnsList.Count - 1) clause.Append(delimiter);
			}

			return clause.ToString();
		}
	}
}