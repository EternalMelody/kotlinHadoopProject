fun main(args: Array<String>) {
    val method = args[0]
    if (method == "generate") {
        FilterGenerator.generate()
    } else if (method == "sort") {
        HadoopSorter.sort(Constants.UNSORTED_FILE_NAME, Constants.SORTED_FILE_DIR)
    } else if (method == "validate") {
        HadoopValidator.validate(Constants.SORTED_FILE_DIR + "/" + Constants.SORTED_FILE_NAME, Constants.VALIDATED_FILE_DIR)
    } else {
        throw Exception("Wrong method parameter")
    }
}