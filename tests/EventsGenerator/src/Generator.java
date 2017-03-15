import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class Generator
{	
	static final String pathFileUsers = "/home/cloudera/tmptest/userid-profile1.tsv";
	static final String pathFileEvents = "/home/cloudera/tmptest/userid-timestamp-artid-artname-traid-traname.tsv";
	static final int usersNumber = 10;
	static final int artistsNumber = 40;
	static final int artistForced = 0;
	static final int userForced = 0;
	
	static final int newUsers = 200;
	static final int newEvents = 500;
	
	public static void main(String[] args)
	{
		PrintWriter writerUsers = null, writerEvents = null;
		try
		{
			writerEvents = new PrintWriter(pathFileEvents);
			writerUsers = new PrintWriter(pathFileUsers);
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		
		for (int i = 0; i < newUsers; i++)
		{
			User u = new User(usersNumber + i, usersNumber, artistsNumber);
			writerUsers.println(u.getRow());			
		}
		
		writerUsers.close();
		
		for (int i = 0; i < newEvents; i++)
		{
			Event e = new Event(usersNumber, artistsNumber, userForced, artistForced);
			writerEvents.println(e.getRow());
		}
		
		writerEvents.close();
	}
}
