namespace Jaunty.Tests.Entities
{
	//[Table("Order Details")]
	public class OrderDetail
	{
		public int OrderId { get; set; }

		public int ProductId { get; set; }

		public decimal UnitPrice { get; set; }

		public short Quantity { get; set; }

		public float Discount { get; set; }
	}
}
