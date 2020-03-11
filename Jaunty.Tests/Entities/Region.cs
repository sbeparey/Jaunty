namespace Jaunty.Tests.Entities
{
	public class Region
	{
		public string RegionId { get; set; }

		[Column("Description")]
		public string Name { get; set; }
	}
}