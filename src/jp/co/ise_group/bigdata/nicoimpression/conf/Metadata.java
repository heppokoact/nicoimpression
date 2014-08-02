package jp.co.ise_group.bigdata.nicoimpression.conf;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jp.co.ise_group.bigdata.nicoimpression.IllegalInputException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

/**
 * 動画メタデータ
 * 
 * @author M.Yoshida
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Metadata {

	/** メタデータ */
	private static ConcurrentHashMap<Text, Metadata> metadatas = new ConcurrentHashMap<Text, Metadata>();
	/** メタデータを読み込み済のデータファイル名 */
	private static final Set<String> readFiles = new HashSet<String>();

	/** 動画ID */
	private final Text videoId;
	/** コメント数 */
	private final int commentCounter;
	/** タグ */
	private final Text[] tags;

	/**
	 * 引数のデータファイルのメタデータを含んでいるメタデータファイルを読み込みます。
	 * 
	 * @param context
	 *            Hadoopのコンテキスト
	 * @param dataFileName
	 *            読み込むメタデータファイル名
	 * @throws IOException
	 *             メタデータファイルを読み込めなかった場合
	 * @throws JsonProcessingException
	 *             メタデータファイルのJSONのパースに失敗した場合
	 */
	public static void readMetadata(Mapper<Text, Text, Text, MapWritable>.Context context, String dataFileName)
			throws IOException, JsonProcessingException {
		// すでに読み込まれている場合、何もしない
		if (readFiles.contains(dataFileName)) {
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
		URI[] uris = context.getCacheArchives();
		Path path = new Path(uris[0]);
		String metaFilePath = path.getName() + "/" + metaFileName;

		// メタデータファイルを読み込み
		ObjectMapper mapper = new ObjectMapper();
		for (String line : FileUtils.readLines(new File(metaFilePath))) {
			Metadata metadata = mapper.readValue(line, Metadata.class);
			metadatas.putIfAbsent(metadata.getVideoId(), metadata);
		}

		// 読み込み済ファイル名に追加
		readFiles.add(dataFileName);
	}

	public static Map<Text, Metadata> get() {
		return metadatas;
	}

	@JsonCreator
	public Metadata(//
			@JsonProperty("video_id") String videoId,//
			@JsonProperty("comment_counter") int commentCounter,//
			@JsonProperty("tags") @JsonDeserialize(using = TagsDeserializer.class) Text[] tags) {
		this.videoId = new Text(videoId);
		this.commentCounter = commentCounter;
		this.tags = tags;
	}

	public Text getVideoId() {
		return videoId;
	}

	public int getCommentCounter() {
		return commentCounter;
	}

	public Text[] getTags() {
		return tags;
	}

	/**
	 * タグ情報のデシリアライザ
	 */
	public static class TagsDeserializer extends JsonDeserializer<Text[]> {

		@Override
		public Text[] deserialize(JsonParser parser, DeserializationContext context) throws IOException,
				JsonProcessingException {
			ObjectCodec codec = parser.getCodec();
			JsonNode tagsNode = codec.readTree(parser);

			if (tagsNode == null) {
				throw new IllegalInputException("tags属性が取得できません。");
			}

			if (tagsNode.size() == 0) {
				return new Text[] {};
			}

			Text[] deserialized = new Text[tagsNode.size()];
			Iterator<JsonNode> tagsIterator = tagsNode.getElements();
			for (int i = 0; tagsIterator.hasNext(); i++) {
				JsonNode tagNode = tagsIterator.next();
				deserialized[i] = new Text(tagNode.path("tag").getTextValue());
			}

			return deserialized;
		}

	}

}
