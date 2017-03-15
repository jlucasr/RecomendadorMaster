import java.io.PrintWriter;

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
	
	public Event(PrintWriter writer, int usersNumber, int artistsNumber)
	{
		Utils u = new Utils(usersNumber, artistsNumber);
		
		userId = u.getRandomUser();
		timestamp = u.getRandomTimestamp();
		artId = u.getRandomArtist();
		artName = u.getRandomContent(8) + "artist";
		trackName = u.getRandomContent(16) + "track";

		row = userId + "\t" + timestamp + "\t" + artId + "\t" + artName + "\t" + trackName;
		
		System.out.println("EVENT: " + row);
		
		writer.println(row);
	}
}
