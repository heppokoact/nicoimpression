/*
 * タイトル：単語カウントMap処理
 * 説明　　：単語カウントのMap処理を実行する。
 *
 * 著作権　：Copyright(c) 2014 Information System Engineering Co., Ltd. All Rights Reserved.
 * 会社名　：株式会社情報システム工学
 *
 * 変更履歴：2014.02.11 Tuesday
 * 　　　　：新規登録
 *
 */
package jp.co.ise_group.bigdata.nicoimpression;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * ニコ動コメント感情分析のMapクラスです。
 *
 * @author M.Yoshida
 *
 */
public class NicoImpressionMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);

	private Map<Long, List<Text>> metas = new HashMap<Long, List<Text>>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException,
			InterruptedException {
		String dataFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		System.err.println(dataFileName);
		Path[] pathes = context.getLocalCacheArchives();
		String path = pathes[0].getName() + "/0000.dat";
		System.err.println(path);
		File f = new File(path);
		System.err.println(FileUtils.readLines(f).get(0));
		//		try (
		//				InputStream in = ClassLoader.getSystemResourceAsStream("./0000.dat");) {
		//			System.out.println(org.apache.commons.io.IOUtils.lineIterator(in, "UTF-8").next());
		//		}
		//		System.out.println(new File(context.getWorkingDirectory() + "/0000.dat").exists());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//		synchronized (NicoImpressionMap.class) {
		//			String dataFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		//
		//		}
		//
		//		ObjectMapper mapper = new ObjectMapper();
		//		JsonNode root = mapper.readTree(value.toString());
		//		Iterator<JsonNode> tags = root.path("tags").getElements();
		//
		//
		//
		//		while (tags.hasNext()) {
		//			JsonNode tag = tags.next().path("tag");
		//			context.write(new Text(tag.getTextValue()), new IntWritable(1));
		//		}

		//		System.err.println(value);
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(value.toString());
		context.write(new Text(root.path("no").getValueAsText()), ONE);
	}

}
