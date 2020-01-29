using System;
using System.Collections.Generic;
using System.Linq;

using Jaunty.Tests.Entities;

using Pluralize.NET;

using Xunit;

namespace Jaunty.Tests.Postgres.IntegrationTests
{
	public class SelectTests : IClassFixture<Northwind>
	{
		private readonly Northwind northwind;

		public SelectTests(Northwind northwind)
		{
			this.northwind = northwind;
			IPluralize pluralize = new Pluralizer();
			Jaunty.TableNameMapper += type => pluralize.Pluralize(type.Name);
		}

		[Fact]
		public void GetAll_Products_ReturnsAllProducts()
		{
			var guid = Guid.NewGuid();
			string sql = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			var products = northwind.Connection.GetAll<Product>(token: guid).ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products;", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public void GetAll_OrderDetails_ReturnsAllOrderDetails()
		{
			var guid = Guid.NewGuid();
			string sql = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			var orderDetails = northwind.Connection.GetAll<OrderDetail>(token: guid).ToList();

			Assert.Equal("SELECT OrderId, ProductId, UnitPrice, Quantity, Discount " +
						 "FROM \"Order Details\";", sql);

			Assert.NotEmpty(orderDetails);
			Assert.True(orderDetails[0].ProductId > 0);
		}

		[Fact]
		public void Get_ProductByPrimaryKey_ReturnsAProduct()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			Product product = northwind.Connection.Get<Product, int>(1, token: guid);

			Assert.Equal(1, product.ProductId);

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "WHERE ProductId = @ProductId;", sql);
			Assert.Equal("ProductId", parameters.ElementAt(0).Key);
			Assert.Equal(1, parameters.ElementAt(0).Value);

			Assert.NotNull(product);
			Assert.True(product.ProductId > 0);
		}

		[Fact]
		public void SelectByAnonymousObject_Products_ReturnsAListOfProducts()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			var products = northwind.Connection.Select<Product>(new { CategoryId = 1, SupplierId = 1 }, token: guid).ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "WHERE CategoryId = @CategoryId AND SupplierId = @SupplierId;", sql);
			Assert.Equal("CategoryId", parameters.ElementAt(0).Key);
			Assert.Equal(1, parameters.ElementAt(0).Value);
			Assert.Equal("SupplierId", parameters.ElementAt(1).Key);
			Assert.Equal(1, parameters.ElementAt(1).Value);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public void SelectByLambda_Products_ReturnsAListOfProducts()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			var products = northwind.Connection.Query<Product>(x => x.CategoryId == 1 && x.SupplierId == 1, token: guid).ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "WHERE CategoryId = @CategoryId AND SupplierId = @SupplierId;", sql);
			Assert.Equal("CategoryId", parameters.ElementAt(0).Key);
			Assert.Equal(1, parameters.ElementAt(0).Value);
			Assert.Equal("SupplierId", parameters.ElementAt(1).Key);
			Assert.Equal(1, parameters.ElementAt(1).Value);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAllProducts()
		{
			var guid = Guid.NewGuid();
			string sql = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			var products = northwind.Connection.From<Product>()
											   .Select<Product>(token: guid).ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products ", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAllProductsOrderedByProductId()
		{
			var guid = Guid.NewGuid();
			string sql = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			var products = northwind.Connection.From<Product>()
											   .OrderBy("ProductId")
											   .Select<Product>(token: guid)
											   .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "ORDER BY ProductId ", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAllProductsOrderedByProductIdDescending()
		{
			var guid = Guid.NewGuid();
			string sql = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			var products = northwind.Connection.From<Product>()
											   .OrderBy("ProductId", Jaunty.SortOrder.Descending)
											   .Select<Product>(token: guid)
											   .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "ORDER BY ProductId DESC ", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAllProductsOrderedByProductIdAndProductName()
		{
			var guid = Guid.NewGuid();
			string sql = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			var products = northwind.Connection.From<Product>()
											   .OrderBy("ProductId")
											   .OrderBy("ProductName")
											   .Select<Product>(token: guid)
											   .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "ORDER BY ProductId, ProductName ", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAllProductsOrderedByProductIdAndProductNameDescending()
		{
			var guid = Guid.NewGuid();
			string sql = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			var products = northwind.Connection.From<Product>()
											   .OrderBy("ProductId")
											   .OrderBy("ProductName", Jaunty.SortOrder.Descending)
											   .Select<Product>(token: guid)
											   .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "ORDER BY ProductId, ProductName DESC ", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsProductNameAndCountGroupedByProductName()
		{
			var guid = Guid.NewGuid();
			string sql = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			var products = northwind.Connection.From<Product>()
											   .GroupBy("ProductName")
											   .Select<Product>("ProductName, count(*)", token: guid)
											   .ToList();

			Assert.Equal("SELECT ProductName, count(*) " +
						 "FROM Products " +
						 "GROUP BY ProductName ", sql);

			Assert.NotEmpty(products);
			Assert.True(!string.IsNullOrEmpty(products[0].ProductName));
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAProductWhereProductIdIs12()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			Product product = northwind.Connection.From<Product>()
												  .Where("ProductId", 12)
												  .Select<Product>(token: guid)
												  .SingleOrDefault();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "WHERE ProductId = @ProductId ", sql);
			Assert.Equal("ProductId", parameters.ElementAt(0).Key);
			Assert.Equal(12, parameters.ElementAt(0).Value);

			Assert.NotNull(product);
			Assert.True(product.ProductId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAListOfProductsWhereProductIdIs1AndCategoryIdIs1()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			var products = northwind.Connection.From<Product>()
											   .Where("SupplierId", 1)
											   .AndWhere("CategoryId", 1)
											   .Select<Product>(token: guid)
											   .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "WHERE SupplierId = @SupplierId AND CategoryId = @CategoryId ", sql);
			Assert.Equal("SupplierId", parameters.ElementAt(0).Key);
			Assert.Equal(1, parameters.ElementAt(0).Value);
			Assert.Equal("CategoryId", parameters.ElementAt(1).Key);
			Assert.Equal(1, parameters.ElementAt(1).Value);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAListOfCategoriesInnerJoinedToProducts()
		{
			var guid = Guid.NewGuid();
			string sql = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
				}
			};

			var categories = northwind.Connection.From<Product>("p")
												 .InnerJoin<Category>("c")
												 .On("p.CategoryId", "c.CategoryId")
												 .Select<Category>(token: guid)
												 .ToList();

			Assert.Equal("SELECT c.CategoryId, c.CategoryName, c.Description, c.Picture " +
						 "FROM Products p " +
						 "INNER JOIN Categories c " +
						 "ON p.CategoryId = c.CategoryId ", sql);

			Assert.NotEmpty(categories);
			Assert.True(categories[0].CategoryId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAListOfCategoriesInnerJoinedToProductsWhereCategoryNameIsProduce()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			var categories = northwind.Connection.From<Product>()
												 .InnerJoin<Category>()
												 .On("Products.CategoryId", "Categories.CategoryId")
												 .Where("CategoryName", "Produce")
												 .Select<Category>(token: guid)
												 .ToList();

			Assert.Equal("SELECT Categories.CategoryId, Categories.CategoryName, Categories.Description, Categories.Picture " +
						 "FROM Products " +
						 "INNER JOIN Categories " +
						 "ON Products.CategoryId = Categories.CategoryId " +
						 "WHERE CategoryName = @CategoryName ", sql);
			Assert.Equal("CategoryName", parameters.ElementAt(0).Key);
			Assert.Equal("Produce", parameters.ElementAt(0).Value);

			Assert.NotEmpty(categories);
			Assert.True(categories[0].CategoryId > 0);
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAListOfOrdersInnerJoinedToOrderDetailsInnerJoinedToProductsWhereProductIdIs1()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			var orders = northwind.Connection.From<Product>("p")
											 .InnerJoin<OrderDetail>("od")
											 .On("od.ProductId", "p.ProductId")
											 .InnerJoin<Order>("o")
											 .On("o.OrderId", "od.OrderId")
											 .Where("p.ProductId", 1)
											 .Select<Order>(token: guid)
											 .ToList();

			Assert.Equal("SELECT o.OrderId, o.CustomerId, o.EmployeeId, o.OrderDate, o.RequiredDate, o.ShippedDate, " +
							"o.ShipVia, o.Freight, o.ShipName, o.ShipAddress, o.ShipCity, o.ShipRegion, o.ShipPostalCode, o.ShipCountry " +
						 "FROM Products p " +
						 "INNER JOIN \"Order Details\" od " +
						 "ON od.ProductId = p.ProductId " +
						 "INNER JOIN Orders o " +
						 "ON o.OrderId = od.OrderId " +
						 "WHERE p.ProductId = @p__ProductId ", sql);
			Assert.Equal("p__ProductId", parameters.ElementAt(0).Key);
			Assert.Equal(1, parameters.ElementAt(0).Value);

			Assert.NotEmpty(orders);
			Assert.True(!string.IsNullOrEmpty(orders[0].OrderId));
		}

		[Fact]
		public void SelectByFluent_Products_ReturnsAListOfEmployeesInnerJoinedToOrdersInnerJoinedToOrderDetailsInnerJoinedToProductsWhereEmployeeIdIs1()
		{
			var guid = Guid.NewGuid();
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				if (((Guid)sender).Equals(guid))
				{
					sql = args.Sql;
					parameters = args.Parameters;
				}
			};

			var employees = northwind.Connection.From<Product>("p")
												.InnerJoin<OrderDetail>("od")
												.On("p.ProductId", "od.ProductId")
												.InnerJoin<Order>("o")
												.On("o.OrderId", "od.OrderId")
												.InnerJoin<Employee>("e")
												.On("e.EmployeeId", "o.EmployeeId")
												.Where("e.EmployeeId", 1)
												.Select<Employee>(token: guid)
												.ToList();

			Assert.Equal("SELECT e.EmployeeId, e.LastName, e.FirstName, e.Title, e.TitleOfCourtesy, e.Address, e.City, e.Region, " +
							"e.PostalCode, e.Country, e.HomePhone, e.Extension, e.Photo, e.Notes, e.ReportsTo, e.PhotoPath " +
						 "FROM Products p " +
						 "INNER JOIN \"Order Details\" od " +
						 "ON p.ProductId = od.ProductId " +
						 "INNER JOIN Orders o " +
						 "ON o.OrderId = od.OrderId " +
						 "INNER JOIN Employees e " +
						 "ON e.EmployeeId = o.EmployeeId " +
						 "WHERE e.EmployeeId = @e__EmployeeId ", sql);
			Assert.Equal("e__EmployeeId", parameters.ElementAt(0).Key);
			Assert.Equal(1, parameters.ElementAt(0).Value);

			Assert.NotEmpty(employees);
			Assert.True(!string.IsNullOrEmpty(employees[0].EmployeeId));
		}
	}
}
