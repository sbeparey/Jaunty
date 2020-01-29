using System;
using System.Data;
using System.IO;

using Dapper;
using MySql.Data.MySqlClient;

namespace Jaunty.Tests.Mysql.IntegrationTests
{
	public class Northwind : IDisposable
	{
		public Northwind()
		{
			CreateDatabase();
			StageDatabase();
		}

		private void CreateDatabase()
		{
			string currentTime = DateTime.Now.ToString("yyyyMMddHHmmss");
			Name = $"northwind_{currentTime}";
			string connectionString = $"server=localhost;port=3306;database=mysql;userid=dotnet;password=csharp-rocks!";
			Connection = new MySqlConnection(connectionString);
			Connection.Execute($"CREATE DATABASE {Name};");
			connectionString = $"server=localhost;port=3306;database={Name};userid=dotnet;password=csharp-rocks!";
			Connection.ConnectionString = connectionString;
		}

		public IDbConnection Connection { get; private set; }
		public string Name { get; private set; }

		public void StageDatabase()
		{
			string northwindDir = Path.Combine(Directory.GetCurrentDirectory(), @"..\..\..\..\..\..\", @"databases\src\Northwind\Mysql");

			string tablesDir = Path.Combine(northwindDir, "Tables");
			ExecuteSql(tablesDir);

			string dataDir = Path.Combine(northwindDir, "Data");
			ExecuteSql(dataDir);

			string constraintsDir = Path.Combine(northwindDir, "Constraints");
			ExecuteSql(constraintsDir);

			string sprocsDir = Path.Combine(northwindDir, "Stored Procedures");
			ExecuteSql(sprocsDir);

			string viewsDir = Path.Combine(northwindDir, "Views");
			ExecuteSql(viewsDir);
		}

		private void ExecuteSql(string directory)
		{
			string[] files = Directory.GetFiles(directory, "*.sql");

			foreach (string file in files)
			{
				string sql = File.ReadAllText(file);
				if (!string.IsNullOrWhiteSpace(sql))
					Connection.Execute(sql);
			}
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				DestroyDatabase();
			}
		}

		private void DestroyDatabase()
		{
			Connection.Open();
			Connection.Execute($"DROP DATABASE {Name};");

			if (Connection?.State != ConnectionState.Closed)
			{
				Connection.Close();
				Connection.Dispose();
			}
		}
	}
}
