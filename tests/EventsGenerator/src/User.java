import java.io.PrintWriter;

public class User
{	
	static final String timestampStrings[] = { "2007-09-23 10:10:10.0", "2017-10-03 10:10:10.0",
			"2015-10-10 10:10:10.0", "2016-09-23 10:10:10.0", "2020-12-23 10:10:10.0" };

	private String userId;
	private String gender;
	private String age;
	private String country;
	private String dateRegister;
	private String row;	

	public User(PrintWriter writer, int newUserId, int usersNumber, int artistsNumber)
	{
		Utils u = new Utils(usersNumber, artistsNumber);

		userId = u.getUser(newUserId);
		gender = u.getRandomGender();
		age = u.getRandomAge();
		country = u.getRandomContent(5) + "country";
		dateRegister = u.getRandomTimestamp();

		row = userId + "\t" + gender + "\t" + age + "\t" + country + "\t" + dateRegister;

		System.out.println("USER: " + row);

		writer.println(row);
	}
}
