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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ニコ動コメント感情分析のReduceクラスです。
 *
 * @author M.Yoshida
 */
public class NicoImpressionReduce extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	/*
	 * (非 Javadoc)
	 *
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}

}
