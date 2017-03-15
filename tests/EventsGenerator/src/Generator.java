import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class Generator
{	
	static final String pathFileUsers = "C://tmp/userid-profile1.tsv";
	static final String pathFileEvents = "C://tmp/userid-timestamp-artid-artname-traid-traname.tsv";
	static final int usersNumber = 10;
	static final int artistsNumber = 40;
	
	static final int newUsers = 15;
	static final int newEvents = 50;
	
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
			new User(writerUsers, usersNumber + i, usersNumber, artistsNumber);
		}
		
		writerUsers.close();
		
		for (int i = 0; i < newEvents; i++)
		{
			new Event(writerEvents, usersNumber, artistsNumber);
		}
		
		writerEvents.close();
	}
}
