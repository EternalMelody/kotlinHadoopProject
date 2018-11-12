import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.WritableComparator

import java.nio.ByteBuffer

class IntComparator : WritableComparator(IntWritable::class.java) {

    override fun compare(b1: ByteArray, s1: Int, l1: Int,
                         b2: ByteArray, s2: Int, l2: Int): Int {

        val v1 = ByteBuffer.wrap(b1, s1, l1).int
        val v2 = ByteBuffer.wrap(b2, s2, l2).int

        return v1.compareTo(v2)
    }
}
