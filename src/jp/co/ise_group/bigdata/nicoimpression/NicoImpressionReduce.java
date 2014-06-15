/*
 * タイトル：単語カウントReduce処理
 * 説明　　：単語カウントのReduce処理を実行する。
 *
 * 著作権　：Copyright(c) 2014 Information System Engineering Co., Ltd. All Rights Reserved.
 * 会社名　：株式会社情報システム工学
 *
 * 変更履歴：2014.02.11 Tuesday
 * 　　　　：新規登録
 *
 */
package jp.co.ise_group.bigdata.nicoimpression;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * ニコ動コメント感情分析のReduceクラスです。
 * 
 * @author M.Yoshida
 */
public class NicoImpressionReduce extends Reducer<Text, MapWritable, NullWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException,
			InterruptedException {
		int count = 0;
		Map<Text, MutableDouble> rateSum = new HashMap<Text, MutableDouble>();
		rateSum.put(new Text("laugh"), new MutableDouble());

		// このタグに紐づく全動画の感情データを合計
		for (MapWritable value : values) {
			count++;
			for (Entry<Writable, Writable> e : value.entrySet()) {
				double rate = ((DoubleWritable) e.getValue()).get();
				rateSum.get(e.getKey()).add(rate);
			}
		}

		// このタグに紐づく全動画の感情データを平均し、最終的な結果データを保持したMapを作成
		Map<String, Object> resultMap = new LinkedHashMap<String, Object>();
		resultMap.put("tag", key.toString());
		resultMap.put("count", count);
		for (Entry<Text, MutableDouble> e : rateSum.entrySet()) {
			double rate = e.getValue().doubleValue() / count;
			resultMap.put(e.getKey().toString(), rate);
		}

		// 最終結果をJSONに整形してContextに書き込み
		ObjectMapper mapper = new ObjectMapper();
		String resultJson = mapper.writeValueAsString(resultMap);
		context.write(NullWritable.get(), new Text(resultJson));
	}
}
