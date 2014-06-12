package jp.co.ise_group.bigdata.nicoimpression;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ニコ動コメント感情分析のDriverクラスです。
 *
 * @author M.Yoshida
 */
public class NicoImpressionDriver extends Configured implements Tool {

	/** ジョブ名 */
	private static final String JOB_NAME = "nicoimpression";

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] <indir> <outdir>\n",
					getClass().getSimpleName());
			return -1;
		}

		//		FileUtil.symLink("E:\\pleiades\\workspace\\nicoimpression\\0000.zip", "");
		//
		//		return 0;

		File outDir = new File(args[1]);
		FileUtils.deleteDirectory(outDir);

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, JOB_NAME);
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(NicoImpressionMap.class);
		job.setReducerClass(NicoImpressionReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * 単語カウント処理を実行します。
	 *
	 * @param args
	 *            コマンド引数なし
	 * @throws Exception
	 *             例外が発生した場合
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new NicoImpressionDriver(), args));
	}

}
