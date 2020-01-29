using System;

namespace Jaunty
{
	[AttributeUsage(AttributeTargets.Class)]
	public class TableAttribute : Attribute
	{
		public TableAttribute(string name)
			: this(null, name)
		{ }

		public TableAttribute(string schema, string name)
		{
			Schema = schema;
			Name = name;
		}

		public string Schema { get; }

		public string Name { get; }
	}

	[AttributeUsage(AttributeTargets.Property)]
	public class ColumnAttribute : Attribute
	{
		public ColumnAttribute(string name)
		{
			Name = name;
		}

		public string Name { get; }
	}

	[AttributeUsage(AttributeTargets.Property)]
	public class KeyAttribute : Attribute
	{
		public bool Manual { get; set; }
	}

	[AttributeUsage(AttributeTargets.Property)]
	public class IgnoreAttribute : Attribute
	{ }
}
