namespace Jaunty.Tests.Entities
{
	public class CustomerDemographic
	{
		[Key(Manual = true)]
		public string CustomerTypeId { get; set; }

		public string CustomerDescription { get; set; }
	}
}
