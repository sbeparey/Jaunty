namespace Jaunty.Tests.Entities
{
	public class Region
	{
		public string RegionId { get; set; }

		[Column("RegionDescription")]
		public string Name { get; set; }
	}
}