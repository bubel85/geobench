
package utilities;

import java.util.Random;

public class RandomGenerator {
	static private RandomGenerator instance = null;

	static public RandomGenerator getInstance() {
		if (instance == null) instance = new RandomGenerator();
		return instance;
	}

	private RandomGenerator() {

	}

	public String getRandomString(int length) {

		StringBuffer randomString = new StringBuffer();
		String randomSource = "aaaaabbbcccdddddeeeeefffffgggghhhhiijjjkkllllmmmnnnnnnnoooopqrsttttuuuuuvvwwxyyzz     ";
		Random r = new Random();
		for (int i=0; i<length; i++){
			randomString.append(randomSource.charAt(r.nextInt(randomSource.length())));
		}
		return randomString.toString();
	}

	public int getRandomNum(int length) {

		StringBuffer randomNum = new StringBuffer();
		String randomNumSource = "1234567890";
		Random r = new Random();
		for (int i=0; i<length; i++){
			randomNum.append(randomNumSource.charAt(r.nextInt(10)));
		}
		
		return Integer.parseInt(randomNum.toString());
	}
	
	
	
	public String getRandom(int max){
		 Random r = new Random();
		 int randomInt = r.nextInt(max);
		 Random p = new Random();
		 int sign = p.nextInt(10);
		 if ((sign% 2) == 0)
		 	randomInt=-randomInt;
		return Integer.toString(randomInt);
	}
	
	public String getRandomP(int max){
		 Random r = new Random();
		 int randomInt = r.nextInt(max);
		 if (randomInt == 0)
			 randomInt = 1;
		 return Integer.toString(randomInt);
	}
	
	public String getRandomN(int max){
		 Random r = new Random();
		 int randomInt = r.nextInt(max);
		 if (randomInt == 0)
			 randomInt = 1;
		 randomInt=-randomInt;
		 return Integer.toString(randomInt);
	}
	

}