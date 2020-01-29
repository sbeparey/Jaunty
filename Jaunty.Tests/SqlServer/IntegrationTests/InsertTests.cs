using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Entities;
using Xunit;

namespace Speedy.Tests.IntegrationTests.SqlServer
{
	public class InsertTests
	{
		private readonly IDbConnection connection;

		public InsertTests()
		{
			var connectionString = "server=.;database=Northwind;trusted_connection=true;";
			connection = new SqlConnection(connectionString);
			var pluralizer = new Pluralizer();
			Speedy.Pluralize = pluralizer.Pluralize;
		}

		[Fact]
		public void Insert()
		{
			var guid = Guid.NewGuid();
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
			Speedy.OnInserting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			bool success = connection.Insert(product, token: guid);

			Assert.Equal("INSERT INTO Products (ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
													   "UnitsOnOrder, ReorderLevel, Discontinued) " +
						 "VALUES (@ProductName, @SupplierId, @CategoryId, @QuantityPerUnit, @UnitPrice, @UnitsInStock, @UnitsOnOrder, " +
													   "@ReorderLevel, @Discontinued);", sql);
			Assert.True(success);
		}

		[Fact]
		public void InsertAndReturnThePrimaryKey()
		{
			var guid = Guid.NewGuid();
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
			Speedy.OnInserting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			int productId = connection.Insert<Product, int>(product, token: guid);

			Assert.Equal("INSERT INTO Products (ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
													   "UnitsOnOrder, ReorderLevel, Discontinued) " +
						 "VALUES (@ProductName, @SupplierId, @CategoryId, @QuantityPerUnit, @UnitPrice, @UnitsInStock, @UnitsOnOrder, " +
													   "@ReorderLevel, @Discontinued); " +
						 "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);", sql);
			Assert.True(productId > 1);
		}

		[Fact]
		public void InsertUsingStringKey()
		{
			var guid = Guid.NewGuid();
			var customerDemographic = new CustomerDemographic
			{
				CustomerTypeId = "Potential",
				CustomerDesc = "Potential customers"
			};

			string sql = null;
			IDictionary<string, object> parameters = null;
			Speedy.OnInserting += (sender, args) =>
			{
				if (((Guid) sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			connection.Delete<CustomerDemographic, string>(customerDemographic.CustomerTypeId);

			bool success = connection.Insert(customerDemographic, token: guid);

			Assert.Equal("INSERT INTO CustomerDemographics (CustomerTypeId, CustomerDesc) " +
			             "VALUES (@CustomerTypeId, @CustomerDesc);", sql);
			Assert.NotEmpty(parameters);
			Assert.Equal("CustomerTypeId", parameters.ElementAt(0).Key);
			Assert.Equal("Potential", parameters.ElementAt(0).Value);
			Assert.Equal("CustomerDesc", parameters.ElementAt(1).Key);
			Assert.Equal("Potential customers", parameters.ElementAt(1).Value);

			Assert.True(success);
		}

		[Fact]
		public void InsertUsingUnion()
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

			var guid = Guid.NewGuid();
			var products = new List<Product>
			{
				product,
				product2,
				product3
			};

			string sql = null;
			Speedy.OnInserting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			int rowsAffected = connection.InsertUnion(products, token: guid);

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
