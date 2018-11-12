import org.apache.hadoop.fs.Path

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

object HadoopSorter {
    @Throws(IOException::class, InterruptedException::class, ClassNotFoundException::class)
    fun sort(input: String, output: String) {
        val inputPath = Path(input)
        val outputDir = Path(output)
        val conf = Configuration(true)
        val job = Job.getInstance(conf)
        job.setJarByClass(HadoopSorter::class.java)
        job.setMapperClass(MapTask::class.java)
        job.setReducerClass(ReduceTask::class.java)
        job.numReduceTasks = 1

        job.mapOutputKeyClass = IntWritable::class.java
        job.mapOutputValueClass = IntWritable::class.java
        job.outputKeyClass = IntWritable::class.java
        job.outputValueClass = IntWritable::class.java
        job.setSortComparatorClass(IntComparator::class.java)

        FileInputFormat.addInputPath(job, inputPath)
        job.setInputFormatClass(TextInputFormat::class.java)

        FileOutputFormat.setOutputPath(job, outputDir)
        job.setOutputFormatClass(TextOutputFormat::class.java)

        val code = if (job.waitForCompletion(true)) 0 else 1
        System.exit(code)
    }

}
