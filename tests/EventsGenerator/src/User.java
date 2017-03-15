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
	
	public String getRow()
	{
		return row;
	}

	public User(int newUserId, int usersNumber, int artistsNumber)
	{
		Utils u = new Utils(usersNumber, artistsNumber);
		
		boolean isEmpty = u.isEmptyRow();

		userId = u.getUser(newUserId);
		if (!isEmpty)
		{
			gender = u.getRandomGender();
			age = u.getRandomAge();
			country = u.getRandomContent(5) + "country";
			dateRegister = u.getRandomTimestamp();
		}

		row = userId + "\t";
		if ((u.isField()) && (!isEmpty))
		{
			row = row + gender;
		}
		row = row + "\t";
		
		if ((u.isField()) && (!isEmpty))
		{
			row = row + age;
		}
		row = row + "\t";
		
		if ((u.isField()) && (!isEmpty))
		{
			row = row + country;
		}
		row = row + "\t";
		
		if ((u.isField()) && (!isEmpty))
		{
			row = row + dateRegister;
		}
		row = row + "\t";

		System.out.println("USER: " + row);
	}
}
