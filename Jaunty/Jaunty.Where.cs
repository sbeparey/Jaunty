using System;
using System.Data;
using System.Linq.Expressions;

namespace Jaunty
{
	public static partial class Jaunty
	{
		public static PartialConditionalClause Where(this TableClause tableClause, string column)
		{
			var conditionalClause = new ConditionalClause(tableClause);
			var partial = new PartialConditionalClause(conditionalClause);
			partial.AddColumn(column);
			return partial;
		}

		public static ConditionalClause Where<T>(this TableClause table, string column, T value)
		{
			return GetCondition(table, column, ComparisonOperator.EqualTo, value);
		}

		public static ConditionalClause Where<T>(this JoinOnClause join, string column, T value)
		{
			return GetCondition(join, column, ComparisonOperator.EqualTo, value);
		}

		public static ConditionalClause Where<T>(this JoinOnClause joinOnClause, Expression<Func<T, bool>> expression, IDbTransaction transaction = null)
		{
			throw new NotImplementedException();
		}

		public static ConditionalClause Where<T>(this ConditionalClause conditionClause, Expression<Func<T, bool>> expression, IDbTransaction transaction = null)
		{
			throw new NotImplementedException();
		}

		public static ConditionalClause Where<T>(this SetClause setClause, string column, T value)
		{
			var condition = new ConditionalClause(setClause);
			condition.Add(column, ComparisonOperator.EqualTo, value);
			return condition;
		}

		public static ConditionalClause AndWhere<T>(this ConditionalClause condition, string column, T value)
		{
			condition.Add(Separator.And);
			condition.Add(column, ComparisonOperator.EqualTo, value);
			return condition;
		}

		public static ConditionalClause OrWhere<T>(this ConditionalClause condition, string column, T value)
		{
			condition.Add(Separator.Or);
			condition.Add(column, ComparisonOperator.EqualTo, value);
			return condition;
		}

		public static ConditionalClause NotWhere<T>(this ConditionalClause condition, string column, T value)
		{
			condition.Add(Separator.Not);
			condition.Add(column, ComparisonOperator.EqualTo, value);
			return condition;
		}

		public static PartialConditionalClause Where(this SetClause setClause, string column)
		{
			var condition = new ConditionalClause(setClause);
			var partial = new PartialConditionalClause(condition);
			partial.AddColumn(column);
			return partial;
		}

		public static PartialConditionalClause AndWhere(this ConditionalClause conditionalClause, string column)
		{
			conditionalClause.Add(Separator.And);
			var partial = new PartialConditionalClause(conditionalClause);
			partial.AddColumn(column);
			return partial;
		}

		public static PartialConditionalClause OrWhere(this ConditionalClause conditionalClause, string column)
		{
			conditionalClause.Add(Separator.Or);
			var partial = new PartialConditionalClause(conditionalClause);
			partial.AddColumn(column);
			return partial;
		}

		public static PartialConditionalClause NotWhere(this ConditionalClause conditionalClause, string column)
		{
			conditionalClause.Add(Separator.Not);
			var partial = new PartialConditionalClause(conditionalClause);
			partial.AddColumn(column);
			return partial;
		}
		
		public static ConditionalClause Where<T>(this SetClause setClause, Expression<Func<T, bool>> expression)
		{
			if (expression is null)
			{
				throw new ArgumentNullException(nameof(expression));
			}

			var condition = new ConditionalClause(setClause);
			condition.Add(expression);
			return condition;
		}

		//public static PartialConditionalClause Where<T>(this SetClause setClause, Expression<Func<T, object>> expression)
		//{
		//	var condition = new ConditionalClause(setClause);
		//	var partial = new PartialConditionalClause(condition);
		//	var body = (MemberExpression)expression.Body;
		//	partial.AddColumn(body.Member.Name);
		//	return partial;
		//}

		//public static PartialConditionalClause Where<T, P>(this ConditionalClause conditionalClause, Expression<Func<T, P>> expression)
		//{
		//	var conditional = new ConditionalClause(conditionalClause.PreviousClause);
		//	var partial = new PartialConditionalClause(conditional);
		//	var body = (MemberExpression)expression.Body;
		//	partial.AddColumn(body.Member.Name);
		//	return partial;
		//}

		public static ConditionalClause EqualTo<T>(this PartialConditionalClause partialClause, T value)
		{
			var fullClause = (ConditionalClause)partialClause.PreviousClause;
			fullClause.Add(partialClause.Column, ComparisonOperator.EqualTo, value);
			return fullClause;
		}

		public static ConditionalClause GreaterThan<T>(this PartialConditionalClause partialClause, T value)
		{
			var fullClause = (ConditionalClause)partialClause.PreviousClause;
			fullClause.Add(partialClause.Column, ComparisonOperator.GreaterThan, value);
			return fullClause;
		}

		public static ConditionalClause LessThan<T>(this PartialConditionalClause partialClause, T value)
		{
			var fullClause = (ConditionalClause)partialClause.PreviousClause;
			fullClause.Add(partialClause.Column, ComparisonOperator.LessThan, value);
			return fullClause;
		}

		public static ConditionalClause GreaterThanOrEqualTo<T>(this PartialConditionalClause partialClause, T value)
		{
			var fullClause = (ConditionalClause)partialClause.PreviousClause;
			fullClause.Add(partialClause.Column, ComparisonOperator.GreaterThanOrEqualTo, value);
			return fullClause;
		}

		public static ConditionalClause LessThanOrEqualTo<T>(this PartialConditionalClause partialClause, T value)
		{
			var fullClause = (ConditionalClause)partialClause.PreviousClause;
			fullClause.Add(partialClause.Column, ComparisonOperator.LessThanOrEqualTo, value);
			return fullClause;
		}

		public static ConditionalClause NotEqualTo<T>(this PartialConditionalClause partialClause, T value)
		{
			var fullClause = (ConditionalClause)partialClause.PreviousClause;
			fullClause.Add(partialClause.Column, ComparisonOperator.NotEqualTo, value);
			return fullClause;
		}

		public static ConditionalClause In<T>(this PartialConditionalClause partialClause, params T[] ins)
		{
			throw new NotImplementedException();
		}

		public static ConditionalClause NotIn<T>(this PartialConditionalClause partialClause, params T[] ins)
		{
			throw new NotImplementedException();
		}

		public static ConditionalClause Between<T1, T2>(this PartialConditionalClause partialClause, T1 value1, T2 value2)
		{
			throw new NotImplementedException();
		}

		public static ConditionalClause Like(this PartialConditionalClause partialClause, string like)
		{
			throw new NotImplementedException();
		}

		public static ConditionalClause NotLike(this PartialConditionalClause partialClause, string notLike)
		{
			throw new NotImplementedException();
		}
		
		private static ConditionalClause GetCondition<T>(Clause clause, string name, ComparisonOperator oper, T value)
		{
			var condition = new ConditionalClause(clause);
			condition.Add(name, oper, value);
			return condition;
		}
	}
}
