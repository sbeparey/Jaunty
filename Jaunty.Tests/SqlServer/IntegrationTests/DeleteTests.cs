using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Entities;
using Xunit;

namespace Speedy.Tests.IntegrationTests.SqlServer
{
	public class DeleteTests
	{
		private readonly IDbConnection connection;

		public DeleteTests()
		{
			var connectionString = "server=.;database=Northwind;trusted_connection=true;";
			connection = new SqlConnection(connectionString);
			var pluralizer = new Pluralizer();
			//Speedy.Pluralize = pluralizer.Pluralize;
			Speedy.SqlDialect = Speedy.Dialects.SqlServer;
			
			Speedy.TableNameMapper += type =>
			{
				if (type == typeof(CustomerCustomerDemo))
					return "CustomerCustomerDemo";

				return pluralizer.Pluralize(type.Name);
			};
		}

		[Fact]
		public void DeleteByKey_ShouldThrowSqlException()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Speedy.OnDeleting += (sender, args) =>
			{
				if (((Guid) sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			Assert.Throws<SqlException>(() => connection.Delete<Product, int>(77, guid));

			Assert.Equal("DELETE FROM Products WHERE ProductId = @ProductId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductId", parameters.ElementAt(0).Key);
			Assert.Equal(77, parameters.ElementAt(0).Value);
		}

		[Fact]
		public void DeleteByKey()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Speedy.OnDeleting += (sender, args) =>
			{
				if (((Guid) sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			bool success = connection.Delete<CustomerDemographic, string>("Potential", token: guid);

			Assert.Equal("DELETE FROM CustomerDemographics WHERE CustomerTypeId = @CustomerTypeId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("CustomerTypeId", parameters.ElementAt(0).Key);
			Assert.Equal("Potential", parameters.ElementAt(0).Value);
		}

		[Fact]
		public void DeleteByCompositeKey()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Speedy.OnDeleting += (sender, args) =>
			{
				if (((Guid) sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			bool success = connection.Delete<CustomerCustomerDemo, string, string>("ALFKI", "Loyal", token: guid);

			Assert.Equal("DELETE FROM CustomerCustomerDemo WHERE CustomerId = @CustomerId AND CustomerTypeId = @CustomerTypeId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("CustomerId", parameters.ElementAt(0).Key);
			Assert.Equal("ALFKI", parameters.ElementAt(0).Value);
			Assert.Equal("CustomerTypeId", parameters.ElementAt(1).Key);
			Assert.Equal("Loyal", parameters.ElementAt(1).Value);

			Assert.True(success);
		}

		[Fact]
		public void DeleteByLambdaExpression()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Speedy.OnDeleting += (sender, args) =>
			{
				if (((Guid) sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			var productName = "The Mamba Juice";
			var rowsAffected = connection.Delete<Product>(p => p.ProductName == productName, guid);

			Assert.Equal("DELETE FROM Products WHERE ProductName = @ProductName;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal(productName, parameters.ElementAt(0).Value);
			Assert.True(rowsAffected == 0);
		}

		[Fact]
		public void DeleteUsingMultipleWhereClause()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Speedy.OnDeleting += (sender, args) =>
			{
				if (((Guid) sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			var rowsAffected = connection.From<Product>()
				.Where("ProductName", "abc")
				.AndWhere("Discontinued", true)
				.Delete<Product>(guid);

			Assert.Equal("DELETE FROM Products WHERE ProductName = @ProductName AND Discontinued = @Discontinued;",
				sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("abc", parameters.ElementAt(0).Value);
			Assert.Equal("Discontinued", parameters.ElementAt(1).Key);
			Assert.Equal(true, parameters.ElementAt(1).Value);
			Assert.Equal(0, rowsAffected);
		}

		[Fact]
		public void DeleteUsingWhereClause()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Speedy.OnDeleting += (sender, args) =>
			{
				if (((Guid) sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			var rowsAffected = connection.From<Product>()
				.Where("ProductName", "abc")
				.Delete<Product>(guid);

			Assert.Equal("DELETE FROM Products WHERE ProductName = @ProductName;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("abc", parameters.ElementAt(0).Value);
		}
	}
}