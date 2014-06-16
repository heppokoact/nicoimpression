package jp.co.ise_group.bigdata.nicoimpression;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jp.co.ise_group.bigdata.nicoimpression.dto.Config;
import jp.co.ise_group.bigdata.nicoimpression.dto.ImpressionDef;
import jp.co.ise_group.bigdata.nicoimpression.dto.Metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * ニコ動コメント感情分析のMapクラスです。
 * 
 * @author M.Yoshida
 */
public class NicoImpressionMap extends Mapper<LongWritable, Text, Text, MapWritable> {

	/** 処理中の動画ID */
	private Text videoId;
	/** 処理中の動画のタグ */
	private Text[] tags;
	/** 全コメント数 */
	private int commentCounter;
	/** コメント数カウンタ */
	private Map<String, MutableInt> impressionCounter = new HashMap<String, MutableInt>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {
		// 感情定義を読み込み
		synchronized (Config.class) {
			Config.readConfig(context);
		}

		// 処理対象データファイル名および動画IDを取得
		String dataFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		videoId = new Text(StringUtils.substringBefore(dataFileName, ".dat"));

		// この動画を含むメタデータファイルを読み込み
		synchronized (Metadata.class) {
			Metadata.readMetadata(context, dataFileName);
		}

		// この動画のメタデータを取得
		Metadata metadata = Metadata.get().get(videoId);
		commentCounter = metadata.getCommentCounter();
		tags = metadata.getTags();
		if (tags == null) {
			throw new IllegalInputException("メタデータに動画ID" + videoId + "のデータが含まれていません。（処理データファイル名：" + dataFileName + "）");
		}

		// コメント数カウンタを初期化
		for (ImpressionDef def : Config.get().getImpressionDefs()) {
			impressionCounter.put(def.getImpressionId(), new MutableInt());
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// コメント数が閾値未満ならmap処理はすべてスキップ
		if (Config.get().getCommentThreshold() > commentCounter) {
			return;
		}

		// コメントを感情分析
		String comment = value.toString();
		for (ImpressionDef def : Config.get().getImpressionDefs()) {
			for (Pattern pattern : def.getPatterns()) {
				Matcher matcher = pattern.matcher(comment);
				if (matcher.find()) {
					String impressionId = def.getImpressionId();
					impressionCounter.get(impressionId).increment();
				}
			}
		}
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {
		// 集計したコメント数をこの動画の全コメント数に対する割合に変換する
		// また、シリアライズできる型に変換する
		MapWritable map = new MapWritable();
		double dCommentCounter = (double) commentCounter;
		for (Entry<String, MutableInt> e : impressionCounter.entrySet()) {
			int count = e.getValue().intValue();
			double rate = count / dCommentCounter;
			map.put(new Text(e.getKey()), new DoubleWritable(rate));
		}

		// タグごとに結果を書き込み
		for (Text tag : tags) {
			context.write(tag, map);
		}
	}

}
