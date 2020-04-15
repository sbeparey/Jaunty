using Jaunty.Tests.Entities;

using Pluralize.NET;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Jaunty.Tests.SqlServer.IntegrationTests
{
	[Collection("sql server tests")]
	public class InsertAsyncTests : IClassFixture<Northwind>
	{
		private readonly ITestOutputHelper output;
		private readonly Northwind northwind;
		IPluralize pluralize = new Pluralizer();

		public InsertAsyncTests(ITestOutputHelper output, Northwind northwind)
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
		public async Task insert_using_Insert_should_return_true()
		{
			var ticket = new Ticket("insert a product");
			var product = new Product
			{
				ProductName = "Latte",
				CategoryId = 1,
				SupplierId = 2,
				Discontinued = false,
				UnitPrice = 20.00m,
				QuantityPerUnit = "500 ml box",
				ReorderLevel = 10,
				UnitsInStock = 20,
				UnitsOnOrder = 0
			};

			string sql = null;
			Jaunty.OnInserting += (sender, args) =>
			{
				sql = args.Sql;
			};

			bool success = await northwind.Connection.InsertAsync(product, ticket: ticket);

			Assert.Equal("INSERT INTO Products (ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
													   "UnitsOnOrder, ReorderLevel, Discontinued) " +
						 "VALUES (@ProductName, @SupplierId, @CategoryId, @QuantityPerUnit, @UnitPrice, @UnitsInStock, @UnitsOnOrder, " +
													   "@ReorderLevel, @Discontinued);", sql);
			Assert.True(success);
		}

		[Fact]
		public async Task insert_using_Insert_should_return_the_primary_key()
		{
			var ticket = new Ticket("insert a product return the primary key");
			var product = new Product
			{
				ProductName = "Chai Latte",
				CategoryId = 1,
				SupplierId = 2,
				Discontinued = false,
				UnitPrice = 20.00m,
				QuantityPerUnit = "500 ml box",
				ReorderLevel = 10,
				UnitsInStock = 20,
				UnitsOnOrder = 0
			};

			string sql = null;
			Jaunty.OnInserting += (sender, args) =>
			{
				sql = args.Sql;
			};

			int productId = await northwind.Connection.InsertAsync<Product, int>(product, ticket: ticket);

			Assert.Equal("INSERT INTO Products (ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
													   "UnitsOnOrder, ReorderLevel, Discontinued) " +
						 "VALUES (@ProductName, @SupplierId, @CategoryId, @QuantityPerUnit, @UnitPrice, @UnitsInStock, @UnitsOnOrder, " +
													   "@ReorderLevel, @Discontinued); " +
						 "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);", sql);
			Assert.True(productId > 77);
		}

		[Fact]
		public async Task insert_where_key_is_string_using_Insert_should_return_true()
		{
			var ticket = new Ticket("insert customer demographics");
			var customerDemographic = new CustomerDemographic
			{
				CustomerTypeId = "Potential",
				CustomerDescription = "Potential customers"
			};

			string sql = null;
			IDictionary<string, object> parameters = null;
			Jaunty.OnInserting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			northwind.Connection.Delete<CustomerDemographic, string>(customerDemographic.CustomerTypeId);

			bool success = await northwind.Connection.InsertAsync(customerDemographic, ticket: ticket);

			Assert.Equal("INSERT INTO CustomerDemographics (CustomerTypeId, CustomerDescription) " +
						 "VALUES (@CustomerTypeId, @CustomerDescription);", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("CustomerTypeId", parameters.ElementAt(0).Key);
			Assert.Equal("Potential", parameters.ElementAt(0).Value);
			Assert.Equal("CustomerDescription", parameters.ElementAt(1).Key);
			Assert.Equal("Potential customers", parameters.ElementAt(1).Value);

			Assert.True(success);
		}

		[Fact]
		public async Task insert_using_fluent_Insert_should_return_true()
		{
			var ticket = new Ticket("insert a product");

			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnInserting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var product = new Product
			{
				ProductName = "Best Ground Coffee",
				CategoryId = 1,
				SupplierId = 2,
				Discontinued = false,
				UnitPrice = 20.00m,
				QuantityPerUnit = "500 ml box",
				ReorderLevel = 10,
				UnitsInStock = 20,
				UnitsOnOrder = 0
			};

			bool success = await northwind.Connection.Values(product)
											   .InsertAsync<Product>();

			Assert.Equal("INSERT INTO Products (ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, " +
				"ReorderLevel, Discontinued) VALUES (@ProductName, @SupplierId, @CategoryId, @QuantityPerUnit, @UnitPrice, @UnitsInStock, " +
				"@UnitsOnOrder, @ReorderLevel, @Discontinued);", sql);
			Assert.NotEmpty(parameters);
			Assert.True(success);
		}

		[Fact]
		public async Task insert_using_fluent_Insert_should_return_the_primary_key()
		{
			var ticket = new Ticket("insert a product return the primary key");

			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnInserting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var product = new Product
			{
				ProductName = "Best Ground Coffee",
				CategoryId = 1,
				SupplierId = 2,
				Discontinued = false,
				UnitPrice = 20.00m,
				QuantityPerUnit = "500 ml box",
				ReorderLevel = 10,
				UnitsInStock = 20,
				UnitsOnOrder = 0
			};

			int productId = await northwind.Connection.Values(product)
												.InsertAsync<Product, int>();

			Assert.Equal("INSERT INTO Products (ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, " +
				"ReorderLevel, Discontinued) VALUES (@ProductName, @SupplierId, @CategoryId, @QuantityPerUnit, @UnitPrice, @UnitsInStock, " +
				"@UnitsOnOrder, @ReorderLevel, @Discontinued); SELECT CAST(SCOPE_IDENTITY() AS BIGINT);", sql);
			Assert.NotEmpty(parameters);
			Assert.True(productId > 77);
		}

		[Fact]
		public async Task insert_using_union()
		{
			var product = new Product
			{
				ProductName = "Latte",
				CategoryId = 1,
				SupplierId = 2,
				Discontinued = false,
				UnitPrice = 20.00m,
				QuantityPerUnit = "500 ml box",
				ReorderLevel = 10,
				UnitsInStock = 20,
				UnitsOnOrder = 0
			};

			var product2 = new Product
			{
				ProductName = "Latte",
				CategoryId = 1,
				SupplierId = 2,
				Discontinued = false,
				UnitPrice = 20.00m,
				QuantityPerUnit = "500 ml box",
				ReorderLevel = 10,
				UnitsInStock = 20,
				UnitsOnOrder = 0
			};

			var product3 = new Product
			{
				ProductName = "Latte",
				CategoryId = 1,
				SupplierId = 2,
				Discontinued = false,
				UnitPrice = 20.00m,
				QuantityPerUnit = "500 ml box",
				ReorderLevel = 10,
				UnitsInStock = 20,
				UnitsOnOrder = 0
			};

			var ticket = new Ticket("");
			var products = new List<Product>
			{
				product,
				product2,
				product3
			};

			string sql = null;
			Jaunty.OnInserting += (sender, args) =>
			{
				sql = args.Sql;
			};

			int rowsAffected = await northwind.Connection.InsertUnionAsync(products, null, ticket);

			Assert.Equal("INSERT INTO Products (ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
													   "UnitsOnOrder, ReorderLevel, Discontinued) \n" +
								 "SELECT @ProductName0, @SupplierId0, @CategoryId0, @QuantityPerUnit0, @UnitPrice0, @UnitsInStock0, @UnitsOnOrder0, " +
													   "@ReorderLevel0, @Discontinued0 \n" +
								 "UNION ALL SELECT @ProductName1, @SupplierId1, @CategoryId1, @QuantityPerUnit1, @UnitPrice1, @UnitsInStock1, " +
													   "@UnitsOnOrder1, @ReorderLevel1, @Discontinued1 \n" +
								 "UNION ALL SELECT @ProductName2, @SupplierId2, @CategoryId2, @QuantityPerUnit2, @UnitPrice2, @UnitsInStock2, " +
													   "@UnitsOnOrder2, @ReorderLevel2, @Discontinued2 \n", sql);
			Assert.Equal(3, rowsAffected);
		}
	}
}
