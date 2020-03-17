namespace Jaunty
{
	internal static class SqlTemplates
	{
		internal const string Select = "SELECT {{columns}} FROM {{table}}";
		internal const string SelectDistinct = "SELECT DISTINCT {{columns}} FROM {{table}}";
		internal const string SelectTop = "SELECT {{top}} {{columns}} FROM {{table}}";
		internal const string SelectDistinctTop = "SELECT DISTINCT {{top}} {{columns}} FROM {{table}}";
		internal const string SelectWhere = "SELECT {{columns}} FROM {{table}} WHERE {{where}};";
		//internal const string Select = "SELECT {{columns}} FROM {{table}} GROUP BY {{groupby}} HAVING {{having}} ORDER BY {{orderby}}";

		internal const string Insert = "INSERT INTO {{table}} ({{columns}}) VALUES ({{parameters}})";
		internal const string InsertByUnion = "INSERT INTO {{table}} ({{columns}}) \n" +
											   "{{select}}";

		internal const string UpdateWhere = "UPDATE {{table}} SET {{columns}} WHERE {{where}}; ";

		//internal const string DeleteAll = "DELETE FROM {{table}}";
		internal const string DeleteWhere = "DELETE FROM {{table}} WHERE {{where}}; ";

		internal static class SqlServer
		{
			internal const string CheckConstraint = "ALTER TABLE {{table}} {{toggle}} CONSTRAINT ALL";
			internal const string InsertedPrimaryKey = "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";
			internal const string Identifier = "\"{{name}}\"";
		}

		internal static class Postgres
		{
			internal const string InsertedPrimaryKey = "RETURNING {{id}};";
			internal const string Identifier = "\"{{name}}\"";
		}

		internal static class MySql
		{
			internal const string InsertedPrimaryKey = "SELECT LAST_INSERT_ID();";
			internal const string Identifier = "`{{name}}`";
		}

		internal static class Sqlite
		{
			internal const string InsertedPrimaryKey = "SELECT LAST_INSERT_ROWID();";
			internal const string Identifier = "\"{{name}}\"";
		}
	}
}
