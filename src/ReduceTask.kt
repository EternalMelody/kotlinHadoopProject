import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.Reducer

class ReduceTask : Reducer<IntWritable, IntWritable, IntWritable, IntWritable>() {
    @Throws(java.io.IOException::class, InterruptedException::class)
    public override fun reduce(key: IntWritable, list: Iterable<IntWritable>, context: Context) {
        for (value in list) {
            context.write(value, key)
        }
    }
}
