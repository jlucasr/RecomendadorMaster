public class Event
{
	static final String timestampStrings[] = 
		{"2007-09-23 10:10:10.0", "2017-10-03 10:10:10.0", "2015-10-10 10:10:10.0", "2016-09-23 10:10:10.0", "2020-12-23 10:10:10.0"};
	
	private String userId;
	private String timestamp;
	private String artId;
	private String artName;
	private String trackName;
	private String row;
	
	public String getRow()
	{
		return row;
	}
	
	public Event(int usersNumber, int artistsNumber, int userForced, int artistForced)
	{
		Utils u = new Utils(usersNumber, artistsNumber);
		
		if (userForced == 0)
		{
			userId = u.getRandomUser();
		}
		else
		{
			userId = u.getUser(userForced);
		}
		
		timestamp = u.getRandomTimestamp();
		
		if (artistForced == 0)
		{
			artId = u.getRandomArtist();
		}
		else
		{
			artId = u.getArtist(artistForced);
		}
		
		artName = artId + "artist";
		
		trackName = u.getRandomContent(16) + "track";

		row = userId + "\t" + timestamp + "\t" + artId + "\t" + artName + "\t" + trackName;
		
		System.out.println("EVENT: " + row);
	}
}
