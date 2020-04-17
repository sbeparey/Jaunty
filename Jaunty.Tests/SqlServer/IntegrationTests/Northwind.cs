using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;

using Dapper;

namespace Jaunty.Tests.SqlServer.IntegrationTests
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
			Name = $"Northwind_{currentTime}";
			connectionString = $"server=.;database=;trusted_connection=true;";
			Connection = new SqlConnection(connectionString);
			Connection.Execute($"CREATE DATABASE {Name};");
			connectionString = $"server=.;database={Name};trusted_connection=true;";
			Connection.ConnectionString = connectionString;
		}

		public IDbConnection Connection { get; private set; }
		public string Name { get; private set; }
		internal string connectionString { get; private set; }

		public void StageDatabase()
		{
			string northwindDir = Path.Combine(Directory.GetCurrentDirectory(), @"..\..\..\..\..\", @"databases\src\Northwind\SqlServer");

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
			if (string.IsNullOrEmpty(Connection.ConnectionString))
				Connection.ConnectionString = connectionString;

			Connection.Open();
			Connection.Execute("msdb..sp_delete_database_backuphistory", new { database_name = Name }, commandType: CommandType.StoredProcedure);
			Connection.Execute($"ALTER DATABASE {Name} SET OFFLINE WITH ROLLBACK IMMEDIATE;");
			Connection.Execute($"DROP DATABASE {Name};");

			if (Connection?.State != ConnectionState.Closed)
			{
				Connection.Close();
				Connection.Dispose();
			}
		}
	}
}
