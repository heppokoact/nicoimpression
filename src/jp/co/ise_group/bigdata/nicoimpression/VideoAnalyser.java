package jp.co.ise_group.bigdata.nicoimpression;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jp.co.ise_group.bigdata.nicoimpression.conf.Config;
import jp.co.ise_group.bigdata.nicoimpression.conf.ImpressionDef;
import jp.co.ise_group.bigdata.nicoimpression.conf.Metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 動画１個分のMap処理を行います。
 * 
 * @author M.Yoshida
 */
public class VideoAnalyser {

	/** Hadoopのコンテキスト */
	private Mapper<Text, Text, Text, MapWritable>.Context context;
	/** この動画のコメントを格納しているファイル名（SequenceFile化する前のファイル名） */
	private Text dataFileName;
	/** 処理中の動画ID */
	private Text videoId;
	/** コメント数が閾値を超えているかどうか */
	private boolean exceedThreshold;
	/** 処理中の動画のタグ */
	private Text[] tags;
	/** 全コメント数 */
	private int commentCounter;
	/** コメント数カウンタ */
	private Map<String, MutableInt> impressionCounter = new HashMap<String, MutableInt>();

	/**
	 * 
	 * @param context
	 *            Hadoopのコンテキスト
	 * @param dataFileName
	 *            この動画のコメントを格納しているファイル名（SequenceFile化する前のファイル名）
	 * @throws JsonProcessingException
	 *             コメントデータのJSONのパースに失敗した場合
	 * @throws IOException
	 *             コメントデータの読み込みに失敗した場合
	 */
	public VideoAnalyser(Mapper<Text, Text, Text, MapWritable>.Context context, Text dataFileName)
			throws JsonProcessingException, IOException {
		this.context = context;
		this.dataFileName = new Text(dataFileName);

		// 動画IDを取得
		videoId = new Text(StringUtils.substringBefore(dataFileName.toString(), ".dat"));

		// 動画メタデータ読み込み
		readMetadata();

		// コメント数が閾値を超えていなければ解析しない
		exceedThreshold = commentCounter >= Config.get().getCommentThreshold();
		if (!exceedThreshold) {
			return;
		}

		// コメント数カウンタを初期化
		initImpressionCounter();
	}

	/**
	 * この動画に関するメタデータを読み込む
	 * 
	 * @throws JsonProcessingException
	 *             コメントデータのJSONのパースに失敗した場合
	 * @throws IOException
	 *             コメントデータの読み込みに失敗した場合
	 */
	private void readMetadata() throws JsonProcessingException, IOException {
		// この動画を含むメタデータファイルを読み込み
		synchronized (Metadata.class) {
			Metadata.readMetadata(context, dataFileName.toString());
		}

		// この動画のメタデータを取得
		Metadata metadata = Metadata.get().get(videoId);
		commentCounter = metadata.getCommentCounter();
		tags = metadata.getTags();
		if (tags == null) {
			throw new IllegalInputException("メタデータに動画ID" + videoId + "のデータが含まれていません。（処理データファイル名：" + dataFileName + "）");
		}
	}

	/**
	 * コメント数カウンタを初期化
	 */
	private void initImpressionCounter() {
		for (ImpressionDef def : Config.get().getImpressionDefs()) {
			impressionCounter.put(def.getImpressionId(), new MutableInt());
		}
	}

	/**
	 * コメント１件のデータを分析する
	 * 
	 * @param commentData
	 *            コメント１件のデータ（JSON）
	 * @throws JsonProcessingException
	 *             コメントデータのJSONのパースに失敗した場合
	 * @throws IOException
	 *             コメントデータの読み込みに失敗した場合
	 */
	public void analyzeComment(String commentData) throws JsonProcessingException, IOException {
		// コメント数が閾値を超えていなければ分析しない
		if (!exceedThreshold) {
			return;
		}

		// コメントデータ（JSON）から実際のコメントだけを抜き出す
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(commentData);
		String comment = root.path("comment").getTextValue();
		// コメントはなぜかnullのことがあるのでnullチェック
		if (comment == null) {
			return;
		}

		// このコメントが各感情を含むかどうかを分析
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

	/**
	 * 集計したコメント数をこの動画の全コメント数に対する割合に変換する また、シリアライズできる型に変換し、結果出力する
	 * 
	 * @throws IOException
	 *             結果の出力に失敗した場合
	 * @throws InterruptedException
	 *             割り込みが入った場合
	 */
	public void summary() throws IOException, InterruptedException {
		// コメント数が閾値を超えていなければ分析しない
		if (!exceedThreshold) {
			return;
		}

		// 集計したコメント数をこの動画の全コメント数に対する割合に変換
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

	public Text getDataFileName() {
		return dataFileName;
	}

}
