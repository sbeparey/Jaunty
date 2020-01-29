using System;
using System.Data;
using System.Data.SQLite;
using System.IO;

using Dapper;

namespace Jaunty.Tests.Sqlite.IntegrationTests
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
			string connectionString = $"data source={Name}.sqlite;version=3;";
			Connection = new SQLiteConnection(connectionString);
			Connection.ConnectionString = connectionString;
		}

		public IDbConnection Connection { get; private set; }
		public string Name { get; private set; }

		public void StageDatabase()
		{
			string northwindDir = Path.Combine(Directory.GetCurrentDirectory(), @"..\..\..\..\..\..\", @"databases\src\Northwind\Sqlite");

			string tablesDir = Path.Combine(northwindDir, "Tables");
			ExecuteSql(tablesDir);

			string dataDir = Path.Combine(northwindDir, "Data");
			ExecuteSql(dataDir);

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
