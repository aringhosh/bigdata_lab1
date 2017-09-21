package asignment1;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
//		String line = "pagecounts-20160802-050000.gz";
////		String[] o = line.split("-");
//		String filename = line.substring(11, 22);
//		System.out.println(filename);
		
		
		ObjectMapper json_mapper = new ObjectMapper();
		 
		String input_string = "{\"archived\": true, \"author\": \"[deleted]\", \"author_flair_css_class\": null, \"author_flair_text\": null, "
				+ "\"body\": \"[deleted]\", \"controversiality\": 0, \"created_utc\": \"1229580549\", \"distinguished\": null, \"downs\": 0, "
				+ "\"edited\": false, \"gilded\": 0, \"id\": \"3b\", \"link_id\": \"t3_7k40r\", \"name\": \"t1_3b\", \"parent_id\": \"t1_c06vu5i\", "
				+ "\"retrieved_on\": 1428217154, \"score\": 1, \"score_hidden\": false, \"subreddit\": \"canada\", \"subreddit_id\": \"t5_2qh68\", "
				+ "\"ups\": 1}";
		JsonNode data = null;
		try {
			data = json_mapper.readValue(input_string, JsonNode.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(data.get("subreddit").textValue());
		System.out.println(data.get("score").longValue());
		
		double average = 0;
		double score_total = 5.362778;
		int _comments = 8;
		average = (score_total / _comments);
		System.out.println(average);
	}

}
