using System;

namespace Jaunty
{
	public interface ITicket
	{
		string Id { get; }
	}

	public class Ticket : ITicket
	{
		public Ticket(string id)
		{
			Id = id;
		}
		
		public string Id { get; }

		public override int GetHashCode()
		{
			return Id.GetHashCode(StringComparison.CurrentCultureIgnoreCase);
		}

		public override bool Equals(object obj)
		{
			return Id.Equals(obj);
		}
	}
}
