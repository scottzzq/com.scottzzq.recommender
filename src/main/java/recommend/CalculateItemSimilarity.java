package recommend;

import itemSimilarity.Step1;
import itemSimilarity.Step2;
import itemSimilarity.Step3;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;


public class CalculateItemSimilarity {
	public static final String SIMILARITYPATH ="/home/hdp_teu_dia/resultdata/zhaizhiqiang";
	public static Map<String, String> path = new HashMap<String, String>();

	public static void init() {
		// step 1
		path.put("SimilarityStep1Input", SIMILARITYPATH + "/InputData");
		path.put("SimilarityStep1Output", SIMILARITYPATH + "/ItemSimilarityStep1");
		// step 2
		path.put("SimilarityStep2Input", path.get("SimilarityStep1Output"));
		path.put("SimilarityStep2Output", SIMILARITYPATH + "/ItemSimilarityStep2");
		// step 3
		path.put("SimilarityStep3Input", path.get("SimilarityStep2Output"));
		path.put("SimilarityStep3Output", SIMILARITYPATH+ "/ItemSimilarityStep3");
	}

	public static void main(String[] args) throws Exception {
		init();
		Configuration conf = new Configuration();
		@SuppressWarnings("unused")
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// step 1
		String[] args1 = { "-input", path.get("SimilarityStep1Input"),
				"-output", path.get("SimilarityStep1Output") };
		ToolRunner.run(conf, new Step1(), args1);
		// step 2
		String[] args2 = { "-input", path.get("SimilarityStep2Input"),
				"-output", path.get("SimilarityStep2Output") };
		ToolRunner.run(conf, new Step2(), args2);
		// step 3
		String[] args3 = { "-input", path.get("SimilarityStep3Input"),
				"-output", path.get("SimilarityStep3Output") };
		ToolRunner.run(conf, new Step3(), args3);
	}
}
