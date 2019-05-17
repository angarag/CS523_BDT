package bdt.mars.project.v1;

public class Names {

	final static String[] names = { "James", "John", "Robert", "Michael",
			"William", "David", "Richard", "Joseph", "Thomas", "Charles",
			"Christopher", "Daniel", "Matthew", "Anthony", "Donald", "Mark",
			"Paul", "Steven", "Andrew", "Kenneth", "Joshua", "George", "Kevin",
			"Brian", "Edward", "Ronald", "Timothy", "Jason", "Jeffrey", "Ryan",
			"Jacob", "Gary", "Nicholas", "Eric", "Stephen", "Jonathan",
			"Larry", "Justin", "Scott", "Brandon", "Frank", "Benjamin",
			"Gregory", "Samuel", "Raymond", "Patrick", "Alexander", "Jack",
			"Dennis", "Jerry", "Tyler", "Aaron", "Jose", "Henry", "Douglas",
			"Adam", "Peter", "Nathan", "Zachary", "Walter", "Kyle", "Harold",
			"Carl", "Jeremy", "Keith", "Roger", "Gerald", "Ethan", "Arthur",
			"Terry", "Christian", "Sean", "Lawrence", "Austin", "Joe", "Noah",
			"Jesse", "Albert", "Bryan", "Billy", "Bruce", "Willie", "Jordan",
			"Dylan", "Alan", "Ralph", "Gabriel", "Roy", "Juan", "Wayne",
			"Eugene", "Logan", "Randy", "Louis", "Russell", "Vincent",
			"Philip", "Bobby", "Johnny", "Bradley" };

	public static String giveMeName(double i) {
		return names[(int) i];
	}

	public static int giveMeSize() {
		return names.length;
	}
}
