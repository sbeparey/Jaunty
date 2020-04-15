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
	public class UpdateTests : IClassFixture<Northwind>
	{
		private readonly ITestOutputHelper output;
		private readonly Northwind northwind;
		IPluralize pluralize = new Pluralizer();

		public UpdateTests(ITestOutputHelper output, Northwind northwind)
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
		public void update_using_Update_should_return_true()
		{
			var ticket = new Ticket("update all rows in product");
			Product product = northwind.Connection.Get<Product, int>(1);
			product.ProductName = "Coffee";

			string sql = null;
			Jaunty.OnUpdating += e => sql = e.Sql;

			bool success = northwind.Connection.Update(product, ticket: ticket);

			Assert.Equal("UPDATE Products " +
						"SET ProductName = @ProductName, " +
							"SupplierId = @SupplierId, " +
							"CategoryId = @CategoryId, " +
							"QuantityPerUnit = @QuantityPerUnit, " +
							"UnitPrice = @UnitPrice, " +
							"UnitsInStock = @UnitsInStock, " +
							"UnitsOnOrder = @UnitsOnOrder, " +
							"ReorderLevel = @ReorderLevel, " +
							"Discontinued = @Discontinued " +
						"WHERE ProductId = @ProductId;", sql);
			Assert.True(success);
		}

		[Fact]
		public void update_using_fluent_Update_should_return_number_of_rows_affected_in_int()
		{
			var ticket = new Ticket("update product name by product id");
			string sql = null;
			IDictionary<string, object> parameters = null;
			Jaunty.OnUpdating += e =>
			{
				sql = e.Sql;
				parameters = e.Parameters;
			};

			int rowsAffected = northwind.Connection
										.Set(Products.ProductName, "Chai latte")
										.Where(Products.ProductId, 1)
										.Update<Product>(ticket: ticket);

			Assert.Equal("UPDATE Products SET ProductName = @ProductName WHERE ProductId = @ProductId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("Chai latte", parameters.ElementAt(0).Value);
			Assert.Equal("ProductId", parameters.ElementAt(1).Key);
			Assert.Equal(1, parameters.ElementAt(1).Value);
			Assert.True(rowsAffected == 1);
		}

		[Fact]
		public void update_a_non_nullable_column_to_null_using_fluent_Update_should_throw_SqlException()
		{
			var ticket = new Ticket("update product name by product id");

			Assert.Throws<SqlException>(() => northwind.Connection
													   .Set("ProductName", (string)null)
													   .Where("ProductId", 1)
													   .Update<Product>(ticket: ticket));
		}

		[Fact]
		public void update_multiple_columns_with_multiple_where_using_fluent_Update_should_return_number_of_rows_affected_in_int()
		{
			var ticket = new Ticket("update product name and unit price by product name and category id");
			string sql = null;
			IDictionary<string, object> parameters = null;
			Jaunty.OnUpdating += e =>
			{
				sql = e.Sql;
				parameters = e.Parameters;
			};

			int rowsAffected = northwind.Connection
										.Set("ProductName", "Chang 2")
										.Set("UnitPrice", 24m)
										.Where("ProductName", "Chang")
										.AndWhere("CategoryId", 2)
										.Update<Product>(ticket: ticket);

			Assert.Equal("UPDATE Products " +
						 "SET ProductName = @ProductName, " +
							"UnitPrice = @UnitPrice " +
						 "WHERE ProductName = @ProductName$ AND " +
							"CategoryId = @CategoryId;", sql);

			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("Chang 2", parameters.ElementAt(0).Value);
			Assert.Equal("UnitPrice", parameters.ElementAt(1).Key);
			Assert.Equal(24m, parameters.ElementAt(1).Value);
			Assert.Equal("ProductName$", parameters.ElementAt(2).Key);
			Assert.Equal("Chang", parameters.ElementAt(2).Value);
			Assert.Equal("CategoryId", parameters.ElementAt(3).Key);
			Assert.Equal(2, parameters.ElementAt(3).Value);
			Assert.True(rowsAffected == 0);
		}

		[Fact]
		public void update_column_by_EqualTo_using_fluent_Update_should_return_number_of_rows_affected_in_int()
		{
			var ticket = new Ticket("update unit price by unit price");
			string sql = null;
			IDictionary<string, object> parameters = null;
			Jaunty.OnUpdating += e =>
			{
				sql = e.Sql;
				parameters = e.Parameters;
			};

			int rowsAffected = northwind.Connection
										.Set("UnitPrice", 24.01m)
										.Where("UnitPrice").EqualTo(24m)
										.Update<Product>(ticket: ticket);

			Assert.Equal("UPDATE Products SET UnitPrice = @UnitPrice WHERE UnitPrice = @UnitPrice$;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("UnitPrice", parameters.ElementAt(0).Key);
			Assert.Equal(24.01m, parameters.ElementAt(0).Value);
			Assert.Equal("UnitPrice$", parameters.ElementAt(1).Key);
			Assert.Equal(24m, parameters.ElementAt(1).Value);
			Assert.True(rowsAffected == 1);
		}

		[Fact]
		public void update_mutliple_columns_by_EqualTo_and_GreaterThan_using_fluent_Update_should_return_number_of_rows_affected_in_int()
		{
			var ticket = new Ticket("update unit price by unit price and units in stock");
			string sql = null;
			IDictionary<string, object> parameters = null;
			Jaunty.OnUpdating += e =>
			{
				sql = e.Sql;
				parameters = e.Parameters;
			};

			int rowsAffected = northwind.Connection
										.Set("UnitPrice", 11.50m)
										.Where("UnitPrice").EqualTo(12m)
										.AndWhere("UnitsInStock").GreaterThan(12)
										.Update<Product>(ticket: ticket);

			Assert.Equal("UPDATE Products " +
						"SET UnitPrice = @UnitPrice " +
						"WHERE UnitPrice = @UnitPrice$ AND " +
						"UnitsInStock > @UnitsInStock;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("UnitPrice", parameters.ElementAt(0).Key);
			Assert.Equal(11.50m, parameters.ElementAt(0).Value);
			Assert.Equal("UnitPrice$", parameters.ElementAt(1).Key);
			Assert.Equal(12m, parameters.ElementAt(1).Value);
			Assert.Equal("UnitsInStock", parameters.ElementAt(2).Key);
			Assert.Equal(12, parameters.ElementAt(2).Value);
			Assert.True(rowsAffected == 1);
		}

		[Fact]
		public void update_mutliple_columns_with_and_and_or_clause_using_fluent_Update_should_return_number_of_rows_affected_in_int()
		{
			var ticket = new Ticket("update unit price by unit price and units in stock and units on order");
			string sql = null;
			IDictionary<string, object> parameters = null;
			Jaunty.OnUpdating += e =>
			{
				sql = e.Sql;
				parameters = e.Parameters;
			};

			int rowsAffected = northwind.Connection
										.Set("UnitPrice", 24.01m)
										.Where("UnitPrice").EqualTo(12m)
										.AndWhere("UnitsInStock").EqualTo(2)
										.OrWhere("UnitsOnOrder").EqualTo(3)
										.Update<Product>(ticket: ticket);

			Assert.Equal("UPDATE Products " +
						"SET UnitPrice = @UnitPrice " +
						"WHERE UnitPrice = @UnitPrice$ AND " +
						"UnitsInStock = @UnitsInStock OR " +
						"UnitsOnOrder = @UnitsOnOrder;", sql);

			Assert.NotEmpty(parameters);
			Assert.Equal("UnitPrice", parameters.ElementAt(0).Key);
			Assert.Equal(24.01m, parameters.ElementAt(0).Value);
			Assert.Equal("UnitPrice$", parameters.ElementAt(1).Key);
			Assert.Equal(12m, parameters.ElementAt(1).Value);
			Assert.Equal("UnitsInStock", parameters.ElementAt(2).Key);
			Assert.Equal(2, parameters.ElementAt(2).Value);
			Assert.Equal("UnitsOnOrder", parameters.ElementAt(3).Key);
			Assert.Equal(3, parameters.ElementAt(3).Value);
			Assert.True(rowsAffected == 0);
		}

		[Fact]
		public void update_with_lambda_using_fluent_Update_should_return_number_of_rows_affected_in_int()
		{
			var ticket = new Ticket("update product name by product id");
			string sql = null;
			IDictionary<string, object> parameters = null;
			Jaunty.OnUpdating += e =>
			{
				sql = e.Sql;
				parameters = e.Parameters;
			};

			int rowsAffected = northwind.Connection
										.Set<Product>(x => x.ProductName == "Chai")
										.Where<Product>(x => x.ProductId == 1)
										.Update<Product>(ticket: ticket);

			Assert.Equal("UPDATE Products SET ProductName = @ProductName WHERE ProductId = @ProductId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("Chai", parameters.ElementAt(0).Value);
			Assert.Equal("ProductId", parameters.ElementAt(1).Key);
			Assert.Equal(1, parameters.ElementAt(1).Value);
			Assert.Equal(1, rowsAffected);
		}

		[Fact]
		public void update_with_multiple_lambda_using_fluent_Update_should_return_number_of_rows_affected_in_int()
		{
			var ticket = new Ticket("update product name by product id and category id");
			string sql = null;
			IDictionary<string, object> parameters = null;
			Jaunty.OnUpdating += e =>
			{
				sql = e.Sql;
				parameters = e.Parameters;
			};

			int rowsAffected = northwind.Connection
										.Set<Product>(x => x.ProductName == "Chai")
										.Where<Product>(x => x.ProductId != 10 && x.CategoryId == 20)
										.Update<Product>(ticket: ticket);

			Assert.Equal("UPDATE Products " +
						"SET ProductName = @ProductName " +
						"WHERE ProductId <> @ProductId AND " +
						"CategoryId = @CategoryId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("Chai", parameters.ElementAt(0).Value);
			Assert.Equal("ProductId", parameters.ElementAt(1).Key);
			Assert.Equal(10, parameters.ElementAt(1).Value);
			Assert.Equal("CategoryId", parameters.ElementAt(2).Key);
			Assert.Equal(20, parameters.ElementAt(2).Value);
			Assert.Equal(0, rowsAffected);
		}
	}
}
