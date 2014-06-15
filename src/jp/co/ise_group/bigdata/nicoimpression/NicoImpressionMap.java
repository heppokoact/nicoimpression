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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * ニコ動コメント感情分析のMapクラスです。
 * 
 * @author M.Yoshida
 * 
 */
public class NicoImpressionMap extends Mapper<LongWritable, Text, Text, MapWritable> {

	/** 感情定義 */
	private static ImpressionDef[] impressionDefs;
	/** 動画メタデータ */
	private static ConcurrentHashMap<Text, List<Text>> metadatas = new ConcurrentHashMap<Text, List<Text>>();
	/** 処理中の動画ID */
	private Text videoId;
	/** 処理中の動画のタグ */
	private List<Text> tags;
	/** 全コメント数 */
	private int allCommentCount = 0;
	/** コメント数カウンタ */
	private Map<String, MutableInt> commentCounter = new HashMap<String, MutableInt>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {
		// 感情定義を読み込み
		synchronized (NicoImpressionMap.class) {
			readImpressionDef(context);
		}
		// 処理対象データファイル名および動画IDを取得
		String dataFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		videoId = new Text(StringUtils.substringBefore(dataFileName, ".dat"));

		// 動画IDのメタデータを読み込み
		synchronized (metadatas) {
			readMetadata(context, dataFileName);
		}

		// この動画のタグ情報を取得
		tags = metadatas.get(videoId);
		if (tags == null) {
			throw new IllegalInputException("メタデータに動画ID" + videoId + "のデータが含まれていません。（処理データファイル名：" + dataFileName + "）");
		}

		// コメント数カウンタを初期化
		for (ImpressionDef def : impressionDefs) {
			commentCounter.put(def.getImpressionId(), new MutableInt());
		}
	}

	/**
	 * 感情定義を読み込みます。
	 * 
	 * @param context
	 *            Hadoopのコンテキスト
	 * @throws IOException
	 *             感情定義が読み込めなかった場合
	 */
	private void readImpressionDef(Mapper<LongWritable, Text, Text, MapWritable>.Context context) throws IOException {
		// すでに読み込まれている場合何もしない
		if (impressionDefs != null) {
			return;
		}

		@SuppressWarnings("deprecation")
		Path[] pathes = context.getLocalCacheFiles();
		ObjectMapper mapper = new ObjectMapper();
		impressionDefs = mapper.readValue(new File(pathes[0].getName()), ImpressionDef[].class);
	}

	/**
	 * 引数のデータファイルのメタデータを含んでいるメタデータファイルを読み込みます。
	 * 
	 * @param context
	 *            Hadoopのコンテキスト
	 * @param dataFileName
	 *            データファイル名
	 * @throws IOException
	 *             メタデータファイルを読み込めなかった場合
	 * @throws JsonProcessingException
	 *             メタデータファイルのJSONのパースに失敗した場合
	 */
	private void readMetadata(Mapper<LongWritable, Text, Text, MapWritable>.Context context, String dataFileName)
			throws IOException, JsonProcessingException {
		// すでに読み込まれている場合、何もしない
		if (metadatas.containsKey(videoId)) {
			return;
		}

		// メタデータファイル名を取得
		Matcher matcher = Pattern.compile("\\d+").matcher(dataFileName);
		if (!matcher.find()) {
			throw new IllegalInputException("処理対象データファイル名から動画IDを取得できませんでした。（処理データファイル名：" + dataFileName + "）");
		}
		int movieNo = Integer.valueOf(matcher.group());
		String metaFileName = String.format("%08d", movieNo).substring(0, 4) + ".dat";
		// メタデータファイルディレクトリを取得
		@SuppressWarnings("deprecation")
		Path[] pathes = context.getLocalCacheArchives();
		String metaFilePath = pathes[0].getName() + "/" + metaFileName;
		// メタデータファイルを読み込み
		ObjectMapper mapper = new ObjectMapper();
		for (String line : FileUtils.readLines(new File(metaFilePath))) {
			JsonNode root = mapper.readTree(line);
			String vId = root.path("video_id").getTextValue();
			List<Text> tagList = new ArrayList<>();
			for (JsonNode tag : root.path("tags")) {
				String tagName = tag.path("tag").getTextValue();
				tagList.add(new Text(tagName));
			}
			metadatas.putIfAbsent(new Text(vId), tagList);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 全コメント数をカウントアップ
		allCommentCount++;

		// コメントを感情分析
		String comment = value.toString();
		for (ImpressionDef def : impressionDefs) {
			for (Pattern pattern : def.getPatterns()) {
				Matcher matcher = pattern.matcher(comment);
				if (matcher.find()) {
					String impressionId = def.getImpressionId();
					commentCounter.get(impressionId).increment();
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
		double dAllCommentCount = (double) allCommentCount;
		for (Entry<String, MutableInt> e : commentCounter.entrySet()) {
			int count = e.getValue().intValue();
			double rate = count / dAllCommentCount;
			map.put(new Text(e.getKey()), new DoubleWritable(rate));
		}

		// タグごとに結果を書き込み
		for (Text tag : tags) {
			context.write(tag, map);
		}
	}

}
