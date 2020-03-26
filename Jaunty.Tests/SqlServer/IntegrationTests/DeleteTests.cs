using Jaunty.Tests.Entities;

using Pluralize.NET;

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

using Xunit;
using Xunit.Abstractions;

namespace Jaunty.Tests.SqlServer.IntegrationTests
{
	[Collection("sql server tests")]
	public class DeleteTests : IClassFixture<Northwind>
	{
		private readonly ITestOutputHelper output;
		private readonly Northwind northwind;
		IPluralize pluralize = new Pluralizer();

		public DeleteTests(ITestOutputHelper output, Northwind northwind)
		{
			this.output = output;
			Jaunty.SqlDialect = Jaunty.Dialect.SqlServer;
			this.northwind = northwind;
			Jaunty.TableNameMapper += GetEntityName;
		}

		private string GetEntityName(Type type)
		{
			if (type == typeof(CustomerCustomerDemo))
				return "CustomerCustomerDemo";
			if (type == typeof(OrderDetail))
				return "\"Order Details\"";
			if (type == typeof(Region))
				return "Region";
			return pluralize.Pluralize(type.Name);
		}

		[Fact]
		public void delete_a_single_item_using_Delete_throws_sql_exception()
		{
			var ticket = new Ticket("delete a product by id");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnDeleting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			Assert.Throws<SqlException>(() => northwind.Connection.Delete<Product, int>(77, ticket: ticket));

			Assert.Equal("DELETE FROM Products WHERE ProductId = @ProductId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductId", parameters.ElementAt(0).Key);
			Assert.Equal(77, parameters.ElementAt(0).Value);
		}

		//[Fact]
		//public void delete_a_single_item_using_Delete_returns_true()
		//{
		//	var ticket = new Ticket("delete a product by id");
		//	string sql = null;
		//	IDictionary<string, object> parameters = null;

		//	Jaunty.OnDeleting += (sender, args) =>
		//	{
		//		sql = args.Sql;
		//		parameters = args.Parameters;
		//	};

		//	bool success = northwind.Connection.Delete<Product, int>(1, ticket: ticket);

		//	Assert.Equal("DELETE FROM Products WHERE ProductId = @ProductId;", sql);
		//	Assert.NotEmpty(parameters);
		//	Assert.Equal("ProductId", parameters.ElementAt(0).Key);
		//	Assert.Equal(1, parameters.ElementAt(0).Value);

		//	Assert.True(success);
		//}

		[Fact]
		public void delete_a_single_item_using_Delete_string_as_key_returns_true()
		{
			var ticket = new Ticket("delete a customer demographic by customer type id");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnDeleting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			bool success = northwind.Connection.Delete<CustomerDemographic, string>("Potential", ticket: ticket);

			Assert.Equal("DELETE FROM CustomerDemographics WHERE CustomerTypeId = @CustomerTypeId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("CustomerTypeId", parameters.ElementAt(0).Key);
			Assert.Equal("Potential", parameters.ElementAt(0).Value);

			//Assert.True(success);
		}

		[Fact]
		public void delete_an_item_by_multiple_foreign_keys_using_Delete_returns_true()
		{
			var ticket = new Ticket("delete a customer customer demo by customer id and customer type id");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnDeleting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			bool success = northwind.Connection.Delete<CustomerCustomerDemo, string, string>("ALFKI", "Loyal", ticket: ticket);

			Assert.Equal("DELETE FROM CustomerCustomerDemo WHERE CustomerId = @CustomerId AND CustomerTypeId = @CustomerTypeId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("CustomerId", parameters.ElementAt(0).Key);
			Assert.Equal("ALFKI", parameters.ElementAt(0).Value);
			Assert.Equal("CustomerTypeId", parameters.ElementAt(1).Key);
			Assert.Equal("Loyal", parameters.ElementAt(1).Value);

			//Assert.True(success);
		}

		[Fact]
		public void delete_using_lamda_Delete_returns_true()
		{
			var ticket = new Ticket("delete a customer by lambda expression");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnDeleting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var productName = "The Mamba Juice";
			var rowsAffected = northwind.Connection.Delete<Product>(p => p.ProductName == productName, ticket: ticket);

			Assert.Equal("DELETE FROM Products WHERE ProductName = @ProductName;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal(productName, parameters.ElementAt(0).Value);
			Assert.True(rowsAffected == 0);
		}

		[Fact]
		public void delete_using_Anonymous_Delete_returns_true()
		{
			var ticket = new Ticket("delete order details by id");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnDeleting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var rowsAffected = northwind.Connection.DeleteAnonymous<OrderDetail>(new { OrderId = 10248 }, ticket: ticket);
			
			Assert.Equal("DELETE FROM \"Order Details\" WHERE OrderId = @OrderId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("OrderId", parameters.ElementAt(0).Key);
			Assert.Equal(10248, parameters.ElementAt(0).Value);
			Assert.True(rowsAffected > 1);
		}

		[Fact]
		public void delete_using_fluent_Delete_returns_true()
		{
			var ticket = new Ticket("delete products by fluent select");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnDeleting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var rowsAffected = northwind.Connection.From<Product>()
												   .Where("ProductName", "abc")
												   .Delete<Product>(ticket: ticket);

			Assert.Equal("DELETE FROM Products WHERE ProductName = @ProductName;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("abc", parameters.ElementAt(0).Value);
		}

		[Fact]
		public void delete_using_fluent_Delete_with_multiple_where_returns_true()
		{
			var ticket = new Ticket("delete products by fluent select with two where clause");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnDeleting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var rowsAffected = northwind.Connection.From<Product>()
												   .Where("ProductName", "abc")
												   .AndWhere("Discontinued", true)
												   .Delete<Product>(ticket: ticket);

			Assert.Equal("DELETE FROM Products WHERE ProductName = @ProductName AND Discontinued = @Discontinued;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("abc", parameters.ElementAt(0).Value);
			Assert.Equal("Discontinued", parameters.ElementAt(1).Key);
			Assert.Equal(true, parameters.ElementAt(1).Value);
			Assert.Equal(0, rowsAffected);
		}
	}
}