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
		public void UpdateEntity()
		{
			Product product = northwind.Connection.Get<Product, int>(1);
			product.ProductName = "Coffee";

			string sql = null;
			Jaunty.OnUpdating += e => sql = e.Sql;

			bool success = northwind.Connection.Update(product);

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
		public void UpdateEntityBySetAndWhere()
		{
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
								.Update<Product>();

			Assert.Equal("UPDATE Products SET ProductName = @ProductName WHERE ProductId = @ProductId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("Chai latte", parameters.ElementAt(0).Value);
			Assert.Equal("ProductId", parameters.ElementAt(1).Key);
			Assert.Equal(1, parameters.ElementAt(1).Value);
			Assert.True(rowsAffected == 1);
		}

		[Fact]
		public void UpdateEntityColumnToNull()
		{
			Assert.Throws<SqlException>(() => northwind.Connection
														.Set("ProductName", (string)null)
														.Where("ProductId", 1)
														.Update<Product>());
		}

		[Fact]
		public void UpdateEntityByDoubleSetAndWhere()
		{
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
								.Update<Product>();

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
		public void UpdateEntityUsingFluentEqualTo()
		{
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
								.Update<Product>();

			Assert.Equal("UPDATE Products SET UnitPrice = @UnitPrice WHERE UnitPrice = @UnitPrice$;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("UnitPrice", parameters.ElementAt(0).Key);
			Assert.Equal(24.01m, parameters.ElementAt(0).Value);
			Assert.Equal("UnitPrice$", parameters.ElementAt(1).Key);
			Assert.Equal(24m, parameters.ElementAt(1).Value);
			Assert.True(rowsAffected == 1);
		}

		[Fact]
		public void UpdateEntityUsingFluentEqualToAndGreaterThan()
		{
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
								.AndWhere("UnitsInStock").GreaterThan(12)
								.Update<Product>();

			Assert.Equal("UPDATE Products " +
								 "SET UnitPrice = @UnitPrice " +
								 "WHERE UnitPrice = @UnitPrice$ AND " +
								       "UnitsInStock > @UnitsInStock;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("UnitPrice", parameters.ElementAt(0).Key);
			Assert.Equal(24.01m, parameters.ElementAt(0).Value);
			Assert.Equal("UnitPrice$", parameters.ElementAt(1).Key);
			Assert.Equal(24m, parameters.ElementAt(1).Value);
			Assert.Equal("UnitsInStock", parameters.ElementAt(2).Key);
			Assert.Equal(12, parameters.ElementAt(2).Value);
			Assert.True(rowsAffected == 0);
		}

		[Fact]
		public void UpdateEntityUsingAndAndOr()
		{
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
				.Update<Product>();

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
		public void UpdateEntityBySetAndWhereLambda()
		{
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
								.Update<Product>();
			Assert.Equal("UPDATE Products SET ProductName = @ProductName WHERE ProductId = @ProductId;", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("ProductName", parameters.ElementAt(0).Key);
			Assert.Equal("Chai", parameters.ElementAt(0).Value);
			Assert.Equal("ProductId", parameters.ElementAt(1).Key);
			Assert.Equal(1, parameters.ElementAt(1).Value);
			Assert.Equal(1, rowsAffected);
		}

		[Fact]
		public void UpdateEntityBySetAndMultipleWhereLambda()
		{
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
								.Update<Product>();
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
