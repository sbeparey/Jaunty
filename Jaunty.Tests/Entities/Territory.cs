namespace Jaunty.Tests.Entities
{
    public class Territory
    {
        public string TerritoryId { get; set; }

        [Column("TerritoryDescription")]
        public string Name { get; set; }

        public int RegionId { get; set; }
    }
}