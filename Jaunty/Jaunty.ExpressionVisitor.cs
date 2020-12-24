using System;
using System.Linq.Expressions;

namespace Jaunty
{
	public static partial class Jaunty
	{
		public static void WalkThrough(this Expression expression, Action<string, string, object> callback)
		{
			var binaryExpression = expression as BinaryExpression;
			string name;
			object value;

			switch (expression.NodeType)
			{
				case ExpressionType.Lambda:
					WalkThrough(((LambdaExpression)expression).Body, callback);
					break;
				case ExpressionType.Not:
					var unaryExpression = expression as UnaryExpression;
					name = unaryExpression?.Operand is MemberExpression unaryMember
										? unaryMember.Member.Name
										: unaryExpression?.Operand.ToString();
					value = false;
					callback?.Invoke(name, expression.NodeType.ToSqlOperator(), value);
					break;
				case ExpressionType.GreaterThan:
				case ExpressionType.GreaterThanOrEqual:
				case ExpressionType.LessThan:
				case ExpressionType.LessThanOrEqual:
				case ExpressionType.NotEqual:
				case ExpressionType.Equal:
                    name = (binaryExpression?.Left) switch
                    {
                        UnaryExpression leftUnary when leftUnary.Operand is MemberExpression leftUnaryMember => leftUnaryMember.Member.Name,
                        MemberExpression member => member.Member.Name,
                        _ => throw new Exception($"Unable to parse the left side of the {nameof(binaryExpression)}"),
                    };
                    value = binaryExpression.Right switch
                    {
                        UnaryExpression rightUnary when rightUnary.Operand is ConstantExpression rightUnaryConstant => rightUnaryConstant.Value,
                        ConstantExpression constant => constant.Value,
                        _ => Expression.Lambda(binaryExpression.Right).Compile().DynamicInvoke(),
                    };
                    callback?.Invoke(name, expression.NodeType.ToSqlOperator(), value);
					break;
				case ExpressionType.OrElse:
				case ExpressionType.AndAlso:
					WalkThrough(binaryExpression?.Left, callback);
					callback?.Invoke(null, expression.NodeType.ToSqlOperator(), null);
					WalkThrough(binaryExpression?.Right, callback);
					break;
				case ExpressionType.Call:
					var methodCallExpression = expression as MethodCallExpression;
					var methodMemberExpression = methodCallExpression?.Object as MemberExpression;
					name = methodMemberExpression?.Member.Name;
					switch (methodCallExpression?.Method.Name)
					{
						case "Equals":
							value = methodCallExpression.Arguments[0].ToString().Trim('"');
							callback?.Invoke(name, expression.NodeType.ToSqlOperator(), value);
							break;
						case "Contains":
							value = methodCallExpression.Arguments[0].ToString().Trim('"');
							value = $"%{value}%";
							callback?.Invoke(name, "LIKE", value);
							break;
						default:
							throw new NotImplementedException();
					}
					break;
				case ExpressionType.MemberAccess:
					var memberExpression = expression as MemberExpression;
					name = memberExpression?.Member.Name;
					value = true;
					callback?.Invoke(name, expression.NodeType.ToSqlOperator(), value);
					break;
				default:
					throw new NotImplementedException();
			}
		}

		private static string ToSqlOperator(this ExpressionType nodeType)
		{
			switch (nodeType)
			{
				case ExpressionType.AndAlso:
					return "AND";
				case ExpressionType.OrElse:
					return "OR";
				case ExpressionType.Equal:
					return "=";
				case ExpressionType.GreaterThan:
					return ">";
				case ExpressionType.GreaterThanOrEqual:
					return ">=";
				case ExpressionType.LessThan:
					return "<";
				case ExpressionType.LessThanOrEqual:
					return "<=";
				case ExpressionType.NotEqual:
					return "<>";
				default:
					return "=";
			}
		}
	}
}
