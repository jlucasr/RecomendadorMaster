import java.security.SecureRandom;
import java.sql.Timestamp;

public class Utils
{
	static final int maxAge = 100;
	static final String timestampStrings[] = 
		{"2007-09-23 10:10:10.0", "2017-10-03 10:10:10.0", "2015-10-10 10:10:10.0", "2016-09-23 10:10:10.0", "2020-12-23 10:10:10.0"};
	
	private int usersNumber;
	private int artistsNumber;	
	
	public String getRandomInt(int limit)
	{
		SecureRandom random = new SecureRandom();
		byte[] seed = random.generateSeed(8);
		random = new SecureRandom(seed);

		int output = random.nextInt(limit);
		
		return String.valueOf(output);
	}
	
	public int getRandomIntNet(int limit)
	{
		SecureRandom random = new SecureRandom();
		byte[] seed = random.generateSeed(8);
		random = new SecureRandom(seed);

		return random.nextInt(limit);
	}
	
	public boolean isField()
	{
		int random = getRandomIntNet(10);
		
		if (random < 9)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public boolean isEmptyRow()
	{
		int random = getRandomIntNet(50);
		
		if (random < 49)
		{
			return false;
		}
		else
		{
			return true;
		}
	}
	
	public String getRandomAge()
	{	
		return getRandomInt(maxAge);
	}
	
	public String getRandomGender()
	{
		SecureRandom random = new SecureRandom();
		byte[] seed = random.generateSeed(8);
		random = new SecureRandom(seed);
		
		int output = random.nextInt(2);

		if (output == 1)
		{
			return "m";
		}
		else
		{
			return "f";
		}
	}
	
	public String getRandomContent(int outputLength)
	{
		SecureRandom random = new SecureRandom();
		byte[] seed = random.generateSeed(8);
		random = new SecureRandom(seed);
		
		long output = random.nextLong();
		String convertedString = String.valueOf(output);
		
		String padding = "";
		int sLength = convertedString.length(), paddingLength = 0;
		
		if (convertedString.length() > outputLength)
		{
			convertedString = convertedString.substring(sLength - outputLength, sLength);
		}
		else if (convertedString.length() < outputLength)
		{
			paddingLength = outputLength - sLength;
			for (int i = 0; i < paddingLength; i++)
			{
				padding = "0" + padding;
			}
			convertedString += padding;
		}
		
		return convertedString;
	}
	
	public String getRandomArtId()
	{
		return getRandomContent(8) + "-" + getRandomContent(4) + "-" + getRandomContent(4) + "-" + getRandomContent(4) + "-" + getRandomContent(12);
	}
	
	public String getRandomUser()
	{
		String s = getRandomInt(usersNumber);
		int paddingLength = 6 - s.length();
		
		String padding = "";
		for (int i = 0; i < paddingLength; i++)
		{
			padding += "0";
		}
		
		return "user_" + padding + s;
	}
	
	public String getUser(int userId)
	{
		String s =  String.valueOf(userId);
		int paddingLength = 6 - s.length();
		
		String padding = "";
		for (int i = 0; i < paddingLength; i++)
		{
			padding += "0";
		}
		
		return "user_" + padding + s;
	}
	
	public String getRandomArtist()
	{
		String s = getRandomInt(artistsNumber);
		int paddingLength = 6 - s.length();
		
		String padding = "";
		for (int i = 0; i < paddingLength; i++)
		{
			padding += "0";
		}
		
		return padding + s;
	}
	
	public String getRandomTimestamp()
	{
		SecureRandom random = new SecureRandom();
		byte[] seed = random.generateSeed(8);
		random = new SecureRandom(seed);
		
		int index = random.nextInt(timestampStrings.length);
		
		Timestamp output= Timestamp.valueOf(timestampStrings[index]);

		return output.toString();
	}
	
	public Utils(int usersNumber, int artistsNumber)
	{
		this.usersNumber = usersNumber;
		this.artistsNumber = artistsNumber;
	}
}
