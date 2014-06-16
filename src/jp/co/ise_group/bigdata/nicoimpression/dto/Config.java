package jp.co.ise_group.bigdata.nicoimpression.dto;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 感情分析の設定
 * 
 * @author M.Yoshida
 */
public final class Config {

	/** 感情分析の設定 */
	private static Config config;

	/** 動画をコメント数で足きりするときの閾値 */
	private final int commentThreshold;
	/** 勘定の定義 */
	private final ImpressionDef[] impressionDefs;

	/**
	 * 感情分析の設定ファイルを読み込みます。
	 * 
	 * @param context
	 *            Hadoopのコンテキスト
	 * @throws IOException
	 *             感情分析の設定ファイルの読み込みやパースに失敗した場合
	 */
	public static void readConfig(Mapper<LongWritable, Text, Text, MapWritable>.Context context) throws IOException {
		if (config == null) {
			@SuppressWarnings("deprecation")
			Path[] pathes = context.getLocalCacheFiles();
			ObjectMapper mapper = new ObjectMapper();
			config = mapper.readValue(new File(pathes[0].getName()), Config.class);
		}
	}

	public static Config get() {
		return config;
	}

	@JsonCreator
	public Config(//
			@JsonProperty("commentThreshold") int commentThreshold,//
			@JsonProperty("impressionDefs") ImpressionDef[] impressionDefs) {
		this.commentThreshold = commentThreshold;
		this.impressionDefs = impressionDefs;
	}

	public int getCommentThreshold() {
		return commentThreshold;
	}

	public ImpressionDef[] getImpressionDefs() {
		return impressionDefs;
	}

}
