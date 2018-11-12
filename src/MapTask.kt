import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class MapTask : Mapper<LongWritable, Text, IntWritable, IntWritable>() {
    @Throws(java.io.IOException::class, InterruptedException::class)
    public override fun map(key: LongWritable, value: Text, context: Context) {
        val line = value.toString()
        val tokens = line.split(Constants.KEY_VALUE_DIVIDER.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val index = Integer.parseInt(tokens[0])
        val randomNumber = Integer.parseInt(tokens[1])
        // We want to sort by the random number, so we use that as the key, and we put the index as value
        context.write(IntWritable(randomNumber), IntWritable(index))
    }
}
