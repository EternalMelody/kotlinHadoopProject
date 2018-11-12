import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat


import java.io.IOException


object HadoopValidator {

    class ValidatorMapTask : Mapper<LongWritable, Text, IntWritable, IntWritable>() {
        internal var preLine: String? = null

        @Throws(InterruptedException::class)
        public override fun map(key: LongWritable, value: Text, context: Context) {
            val line = value.toString()
            val tokens = line.split(Constants.OUTPUT_KEY_VALUE_DIVIDER.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            val valuePart = Integer.parseInt(tokens[1])
            if (preLine != null) {
                val preValuePart = Integer.parseInt(preLine!!.split(Constants.OUTPUT_KEY_VALUE_DIVIDER.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[1])
                if (valuePart < preValuePart) throw InterruptedException("Validation failed, the numbers are not sorted")
            }
            preLine = line
        }
    }

    @Throws(IOException::class, InterruptedException::class, ClassNotFoundException::class)
    fun validate(input: String, output: String) {
        val inputPath = Path(input)
        val outputDir = Path(output)
        val conf = Configuration(true)
        val job = Job.getInstance(conf)
        job.setJarByClass(HadoopValidator::class.java)
        job.setMapperClass(ValidatorMapTask::class.java)
        job.numReduceTasks = 0

        FileInputFormat.addInputPath(job, inputPath)
        job.setInputFormatClass(TextInputFormat::class.java)

        FileOutputFormat.setOutputPath(job, outputDir)
        job.setOutputFormatClass(TextOutputFormat::class.java)

        val code = if (job.waitForCompletion(true)) 0 else 1
        if (code == 0) println("Validation passed: the numbers are sorted")
        System.exit(code)
    }
}
