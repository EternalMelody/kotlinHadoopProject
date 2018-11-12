import java.io.*
import java.nio.charset.StandardCharsets
import java.util.Random

object FilterGenerator {
    @Throws(FileNotFoundException::class)
    fun generate() {
        val wantedSize = 10.0

        val random = Random()
        val file = File(Constants.UNSORTED_FILE_NAME)
        val start = System.currentTimeMillis()
        val writer = PrintWriter(BufferedWriter(OutputStreamWriter(FileOutputStream(file), StandardCharsets.UTF_8)), false)
        var counter = 0
        var index = 0
        while (true) {
            for (i in 0..99) {
                val number = random.nextInt(Integer.MAX_VALUE)
                writer.println("${index++} ${Constants.KEY_VALUE_DIVIDER} $number")
            }
            //Check to see if the current size is what we want it to be
            if (++counter == 10000) {
                System.out.printf("Current file size: %.3f GB%n", file.length() / 1e9)
                if (file.length() >= wantedSize * 1e9) {
                    writer.close()
                    break
                } else {
                    counter = 0
                }
            }
        }
        val time = System.currentTimeMillis() - start
        System.out.printf("Took %.1f seconds to create a file of %.3f GB", time / 1e3, file.length() / 1e9)
    }
}
