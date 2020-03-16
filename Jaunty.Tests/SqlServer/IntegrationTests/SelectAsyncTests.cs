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
	public class SelectAsyncTests : IClassFixture<Northwind>
	{
		private readonly ITestOutputHelper output;
		private readonly Northwind northwind;
		IPluralize pluralize = new Pluralizer();

		public SelectAsyncTests(ITestOutputHelper output, Northwind northwind)
		{
			this.output = output;
			Jaunty.SqlDialect = Jaunty.Dialects.SqlServer;
			this.northwind = northwind;
			Jaunty.TableNameMapper += GetEntityName;
		}

		private string GetEntityName(Type type)
		{
			if (type == typeof(OrderDetail))
				return "\"Order Details\"";
			if (type == typeof(Region))
				return "Region";
			return pluralize.Pluralize(type.Name);
		}

		[Fact]
		public async Task get_all_using_GetAll_returns_a_collection_of_products()
		{
			var ticket = new Ticket("get all products using GetAll<T>");
			string sql = null;
			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var products = (await northwind.Connection.GetAllAsync<Product>(ticket: ticket)).ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products;", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public async Task get_all_using_GetAll_returns_a_collection_of_order_details()
		{
			var ticket = new Ticket("get all order details using GetAll<T>");
			string sql = null;
			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var orderDetails = (await northwind.Connection.GetAllAsync<OrderDetail>(ticket: ticket)).ToList();

			Assert.Equal("SELECT OrderId, ProductId, UnitPrice, Quantity, Discount " +
						 "FROM \"Order Details\";", sql);

			Assert.NotEmpty(orderDetails);
			Assert.True(orderDetails[0].ProductId > 0);
		}

		[Fact]
		public async Task get_by_primary_key_using_Get_returns_a_product()
		{
			var ticket = new Ticket("get a product via primary key using Get");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			Product product = await northwind.Connection.GetAsync<Product, int>(1, ticket: ticket);

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
		public async Task get_using_anonymous_object_Query_returns_a_collection_of_products()
		{
			var ticket = new Ticket("select product by anonymous object query");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var products = (await northwind.Connection.QueryAnonymousAsync<Product>(new { CategoryId = 1, SupplierId = 1 }, ticket: ticket)).ToList();

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
		public async Task get_using_lambda_Query_returns_a_collection_of_products()
		{
			var ticket = new Ticket("select product by lambda query");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var products = (await northwind.Connection.QueryAsync<Product>(x => x.CategoryId == 1 && x.SupplierId == 1, ticket: ticket)).ToList();

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
		public async Task get_using_fluent_Select_returns_a_collection_of_products()
		{
			var ticket = new Ticket("fluent select all products");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var products = (await northwind.Connection.From<Product>()
													  .SelectAsync<Product>(ticket: ticket))
													  .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products;", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public async Task get_top_15_using_fluent_Select_returns_a_collection_of_15_products()
		{
			var ticket = new Ticket("fluent select top 15 products");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var products = (await northwind.Connection.Top(15)
													  .From<Product>()
													  .SelectAsync<Product>(ticket: ticket))
													  .ToList();

			Assert.Equal("SELECT TOP 15 ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products;", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		//[Fact]
		//public async Task SelectWhereWithLimitByFluent_Products_ReturnsAllProducts()
		//{
		//	var ticket = new Ticket("");
		//	string sql = null;

		//	Jaunty.OnSelecting += (sender, args) =>
		//	{
		//		if (sender == ticket)
		//		{
		//			sql = args.Sql;
		//		}
		//	};

		//	var products = (await northwind.Connection.From<Product>()
		//									   .Where("CategoryId", 2)
		//									   .Limit(5)
		//									   .Select<Product>(ticket: ticket).ToList();

		//	Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
		//					"UnitsOnOrder, ReorderLevel, Discontinued " +
		//				 "FROM Products " +
		//				 "WHERE CategoryId = @CategoryId " +
		//				 "LIMIT 5;", sql);

		//	Assert.NotEmpty(products);
		//	Assert.True(products[0].ProductId > 0);
		//}

		[Fact]
		public async Task get_top_10_using_fluent_Select_returns_a_collection_of_first_10_products()
		{
			var ticket = new Ticket("fluent select first 10 products ordered by product name using offset");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var products = (await northwind.Connection.From<Product>()
													  .OrderBy("ProductName")
													  .Offset(0)
													  .FetchNext(10)
													  .SelectAsync<Product>(ticket: ticket))
													  .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "ORDER BY ProductName " +
						 "OFFSET 0 ROWS " +
						 "FETCH NEXT 10 ROWS ONLY;", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
			Assert.True(products.Count() == 10);
		}

		[Fact]
		public async Task get_ordered_items_using_fluent_Select_returns_a_collection_of_products_ordered_by_ProductId()
		{
			var ticket = new Ticket("fluent select all products ordered by product id");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var products = (await northwind.Connection.From<Product>()
													  .OrderBy("ProductId")
													  .SelectAsync<Product>(ticket: ticket))
													  .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "ORDER BY ProductId;", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public async Task get_ordered_items_using_fluent_Select_returns_a_collection_of_products_ordered_by_ProductId_descending()
		{
			var ticket = new Ticket("fluent select all products ordered by product id descending");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var products = (await northwind.Connection.From<Product>()
													  .OrderBy("ProductId", Jaunty.SortOrder.Descending)
													  .SelectAsync<Product>(ticket: ticket))
													  .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "ORDER BY ProductId DESC;", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public async Task get_multiple_ordered_items_using_fluent_Select_returns_a_collection_of_products_ordered_by_ProductId_and_ProductName()
		{
			var ticket = new Ticket("fluent select products ordered by product id then product name");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var products = (await northwind.Connection.From<Product>()
													  .OrderBy("ProductId")
													  .OrderBy("ProductName")
													  .SelectAsync<Product>(ticket: ticket))
													  .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "ORDER BY ProductId, ProductName;", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public async Task get_multiple_ordered_items_using_fluent_Select_returns_a_collection_of_products_ordered_by_ProductId_and_ProductName_descending()
		{
			var ticket = new Ticket("fluent select products ordered by product id then product name descending");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var products = (await northwind.Connection.From<Product>()
													  .OrderBy("ProductId")
													  .OrderBy("ProductName", Jaunty.SortOrder.Descending)
													  .SelectAsync<Product>(ticket: ticket))
													  .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "ORDER BY ProductId, ProductName DESC;", sql);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public async Task get_count_using_fluent_Select_returns_a_collection_of_ProductName_and_count_tuple()
		{
			var ticket = new Ticket("fluent select products' name count from products grouped by product name");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var products = (await northwind.Connection.From<Product>()
													  .GroupBy("ProductName")
													  .SelectAsync<Product>("ProductName, count(*)", ticket: ticket))
													  .ToList();

			Assert.Equal("SELECT ProductName, count(*) " +
						 "FROM Products " +
						 "GROUP BY ProductName;", sql);

			Assert.NotEmpty(products);
			Assert.True(!string.IsNullOrEmpty(products[0].ProductName));
		}

		[Fact]
		public async Task get_a_single_item_using_fluent_Select_returns_a_product()
		{
			var ticket = new Ticket("fluent select products where product id is 12");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			Product product = (await northwind.Connection.From<Product>()
														 .Where("ProductId", 12)
														 .SelectAsync<Product>(ticket: ticket))
														 .SingleOrDefault();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "WHERE ProductId = @ProductId;", sql);
			Assert.Equal("ProductId", parameters.ElementAt(0).Key);
			Assert.Equal(12, parameters.ElementAt(0).Value);

			Assert.NotNull(product);
			Assert.True(product.ProductId > 0);
		}

		[Fact]
		public async Task get_by_where_using_Select_returns_a_collection_of_products()
		{
			var ticket = new Ticket("fluent select products where supplier id is 1 and category id is 1");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var products = (await northwind.Connection.From<Product>()
													  .Where("SupplierId", 1)
													  .AndWhere("CategoryId", 1)
													  .SelectAsync<Product>(ticket: ticket))
													  .ToList();

			Assert.Equal("SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, " +
							"UnitsOnOrder, ReorderLevel, Discontinued " +
						 "FROM Products " +
						 "WHERE SupplierId = @SupplierId AND CategoryId = @CategoryId;", sql);
			Assert.Equal("SupplierId", parameters.ElementAt(0).Key);
			Assert.Equal(1, parameters.ElementAt(0).Value);
			Assert.Equal("CategoryId", parameters.ElementAt(1).Key);
			Assert.Equal(1, parameters.ElementAt(1).Value);

			Assert.NotEmpty(products);
			Assert.True(products[0].ProductId > 0);
		}

		[Fact]
		public async Task get_with_an_inner_join_using_fluent_Select_returns_a_collection_of_categories()
		{
			var ticket = new Ticket("fluent select categories from products inner joined to categories");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var categories = (await northwind.Connection.From<Product>("p")
														.InnerJoin<Category>("c")
														.On("p.CategoryId", "c.CategoryId")
														.SelectAsync<Category>(ticket: ticket))
														.ToList();

			Assert.Equal("SELECT c.CategoryId, c.CategoryName, c.Description, c.Picture " +
						 "FROM Products p " +
						 "INNER JOIN Categories c " +
						 "ON p.CategoryId = c.CategoryId;", sql);

			Assert.NotEmpty(categories);
			Assert.True(categories[0].CategoryId > 0);
		}

		[Fact]
		public async Task get_with_an_inner_join_plus_where_clause_using_fluent_Select_returns_a_collection_of_categories()
		{
			var ticket = new Ticket("fluent select categories from products inner joined to categories where category name is produce");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var categories = (await northwind.Connection.From<Product>()
														.InnerJoin<Category>()
														.On("Products.CategoryId", "Categories.CategoryId")
														.Where("CategoryName", "Produce")
														.SelectAsync<Category>(ticket: ticket))
														.ToList();

			Assert.Equal("SELECT Categories.CategoryId, Categories.CategoryName, Categories.Description, Categories.Picture " +
						 "FROM Products " +
						 "INNER JOIN Categories " +
						 "ON Products.CategoryId = Categories.CategoryId " +
						 "WHERE CategoryName = @CategoryName;", sql);
			Assert.Equal("CategoryName", parameters.ElementAt(0).Key);
			Assert.Equal("Produce", parameters.ElementAt(0).Value);

			Assert.NotEmpty(categories);
			Assert.True(categories[0].CategoryId > 0);
		}

		[Fact]
		public async Task get_with_two_inner_joins_and_where_clause_using_fluent_Select_returns_a_collection_of_orders()
		{
			var ticket = new Ticket("fluent select orders from products inner joined to order details inner joined to order where product id is 1");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var orders = (await northwind.Connection.From<Product>("p")
													.InnerJoin<OrderDetail>("od")
													.On("od.ProductId", "p.ProductId")
													.InnerJoin<Order>("o")
													.On("o.OrderId", "od.OrderId")
													.Where("p.ProductId", 1)
													.SelectAsync<Order>(ticket: ticket))
													.ToList();

			Assert.Equal("SELECT o.OrderId, o.CustomerId, o.EmployeeId, o.OrderDate, o.RequiredDate, o.ShippedDate, " +
							"o.ShipVia, o.Freight, o.ShipName, o.ShipAddress, o.ShipCity, o.ShipRegion, o.ShipPostalCode, o.ShipCountry " +
						 "FROM Products p " +
						 "INNER JOIN \"Order Details\" od " +
						 "ON od.ProductId = p.ProductId " +
						 "INNER JOIN Orders o " +
						 "ON o.OrderId = od.OrderId " +
						 "WHERE p.ProductId = @p__ProductId;", sql);
			Assert.Equal("p__ProductId", parameters.ElementAt(0).Key);
			Assert.Equal(1, parameters.ElementAt(0).Value);

			Assert.NotEmpty(orders);
			Assert.True(!string.IsNullOrEmpty(orders[0].OrderId));
		}

		[Fact]
		public async Task get_with_three_inner_joins_and_where_clause_using_fluent_Select_returns_a_collection_of_employees()
		{
			var ticket = new Ticket("fluent select employees from products inner joined to order details inner joined to order inner joined to employees where employee id is 1");
			string sql = null;
			IDictionary<string, object> parameters = null;

			Jaunty.OnSelecting += (sender, args) =>
			{
				sql = args.Sql;
				parameters = args.Parameters;
			};

			var employees = (await northwind.Connection.From<Product>("p")
													   .InnerJoin<OrderDetail>("od")
													   .On("p.ProductId", "od.ProductId")
													   .InnerJoin<Order>("o")
													   .On("o.OrderId", "od.OrderId")
													   .InnerJoin<Employee>("e")
													   .On("e.EmployeeId", "o.EmployeeId")
													   .Where("e.EmployeeId", 1)
													   .SelectAsync<Employee>(ticket: ticket))
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
						 "WHERE e.EmployeeId = @e__EmployeeId;", sql);
			Assert.Equal("e__EmployeeId", parameters.ElementAt(0).Key);
			Assert.Equal(1, parameters.ElementAt(0).Value);

			Assert.NotEmpty(employees);
			Assert.True(!string.IsNullOrEmpty(employees[0].EmployeeId));
		}

		[Fact]
		public async Task get_order_details_employing_having_clause_using_fluent_Select_returns_a_dictionary_of_order_ids_and_unit_prices()
		{
			var ticket = new Ticket("fluent select a dictionary of order ids and unit prices from order details using having");
			string sql = null;

			Jaunty.OnSelecting += (senders, args) => sql = args.Sql;

			var dictionary = (await northwind.Connection.From<OrderDetail>()
														.GroupBy("OrderId")
														.Having("SUM(UnitPrice) > 10")
														.OrderBy("OrderId")
														.SelectAsync<dynamic>("OrderId, SUM(UnitPrice) AS Total"))
														.ToDictionary(row => (int)row.OrderId, row => (decimal)row.Total);

			Assert.Equal("SELECT OrderId, SUM(UnitPrice) AS Total " +
						 "FROM \"Order Details\" " +
						 "GROUP BY OrderId " +
						 "HAVING SUM(UnitPrice) > 10 " +
						 "ORDER BY OrderId;", sql);

			Assert.NotEmpty(dictionary);
			Assert.True(dictionary.Count > 0);
		}

		[Fact]
		public async Task get_with_distinct_using_fluent_Select_returns_a_collection_of_region()
		{
			var ticket = new Ticket("fluent distinct select all regions");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var regions = (await northwind.Connection.Distinct()
													 .From<Region>()
													 .SelectAsync<Region>(ticket: ticket));

			Assert.Equal("SELECT DISTINCT RegionId, Description FROM Region;", sql);
			Assert.NotEmpty(regions);
		}

		[Fact]
		public async Task get_with_distinct_and_top_using_fluent_Select_returns_a_collection_of_customers()
		{
			var ticket = new Ticket("fluent select distinct top 20 customers");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var regions = (await northwind.Connection.Distinct()
													 .Top(20)
													 .From<Customer>()
													 .SelectAsync<Customer>(ticket: ticket));

			Assert.Equal("SELECT DISTINCT TOP 20 CustomerId, CompanyName, ContactName, ContactTitle, Address, City, Region, " +
							"PostalCode, Country, Phone, Fax " +
						 "FROM Customers;", sql);
			Assert.NotEmpty(regions);
		}

		[Fact]
		public async Task get_many_to_many_using_fluent_Select_returns_a_lookup_collection_of_employee_territories()
		{
			var ticket = new Ticket("fluent select employee territories");
			string sql = null;

			Jaunty.OnSelecting += (sender, args) => sql = args.Sql;

			var lookup = (await northwind.Connection.From<EmployeeTerritory>()
													.SelectAsync<EmployeeTerritory>(ticket: ticket))
													.ToLookup(x => x.EmployeeId, x => x.TerritoryId);

			Assert.Equal("SELECT EmployeeId, TerritoryId FROM EmployeeTerritories;", sql);
			Assert.NotEmpty(lookup);
		}
	}
}
