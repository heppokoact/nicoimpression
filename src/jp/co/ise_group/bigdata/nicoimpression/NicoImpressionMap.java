package jp.co.ise_group.bigdata.nicoimpression;

import java.io.IOException;

import jp.co.ise_group.bigdata.nicoimpression.conf.Config;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ニコ動コメント感情分析のMapクラスです。
 * 
 * @author M.Yoshida
 */
public class NicoImpressionMap extends Mapper<Text, Text, Text, MapWritable> {

	/** 動画１個分の分析を行うオブジェクト */
	private VideoAnalyser analyser;

	@Override
	protected void setup(Mapper<Text, Text, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {
		// 感情定義を読み込み
		synchronized (Config.class) {
			Config.readConfig(context);
		}
	}

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {
		// 初回及びファイル名の切れ目でanalyserを作り変える
		if (analyser == null || !key.equals(analyser.getDataFileName())) {
			if (analyser != null) {
				analyser.summary();
			}
			analyser = new VideoAnalyser(context, key);
		}

		analyser.analyzeComment(value.toString());
	}

	@Override
	protected void cleanup(Mapper<Text, Text, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {
		analyser.summary();
	}
}
