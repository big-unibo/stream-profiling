package it.unibo.big.streamprofiling.generator

import it.unibo.big.streamprofiling.generator.seed.SimulationActions.SimulationAction

object Constants {
    private const val DEFAULT_FREQUENCY = 1000L
    private const val DEFAULT_NUMBER_OF_SEEDS = 10
    private const val DEFAULT_SEEDS_VALUES = 10
    private const val DEFAULT_SEEDS_COMMON_VALUES = 1
    private const val DEFAULT_LIFETIME = 10000000L
    private const val DEFAULT_UNLIFETIME = 0L
    private const val DEFAULT_DURATION_OF_SIMULATION =  60000 * 10L
    private const val DEFAULT_PROBABILITY = 0.9
    private const val DEFAULT_SIMULATION_ACTIONS_STRING = "STATIC"

    /**
     * Generic function, that from a string parameter returns is array representation, with sorted values. One for each seed.
     *
     *
     * @param n number of seeds, the result must be an array of size n
     * @param s the input parameter string, can be a string in the format [v1,v2,v3] or v, where v is a unique value that is used for every seed (n times)
     * @param decode a function that from a string return the reified type T, and essentially parse the string
     *
     * @return the decoded array of size N with T values
     */
    private inline fun <reified T: Any> fromParametersToArrayList(n: Int, s: String, decode: (s: String) -> T) : ArrayList<T> {
        return (if(s.startsWith("[") && s.endsWith("]")) {
            val r = s.substring(1, s.length - 1).split(",").map{decode(it)}.toTypedArray()
            require(r.size == n)
            r
        } else {
            Array(n) {decode(s)}
        }).toCollection(ArrayList())
    }

    /**
     * A wrapper for fromParametersToArrayList (above)
     *
     * @param n number of seeds, the result must be an array of size n
     * @param args the overall arguments
     * @param argNumber the index of the argument we want to extract
     * @param defaultValue the default value, if args[argNumber] is not present
     * @param decode a function that from a string return the reified type T, and essentially parse the string
     *
     * @return the decoded array of size N with T values
     */
    private inline fun <reified T: Any> fromParametersToArrayList(n: Int, args: Array<String>, argNumber: Int, defaultValue: Any, decode: (s: String) -> T) : ArrayList<T> {
        return fromParametersToArrayList(n, getToString(args, argNumber, defaultValue), decode)
    }

    /**
     * @param args the overall arguments
     * @param argNumber the index of the argument we want to extract
     * @param defaultValue the default value, if args[argNumber] is not present
     *
     * @return args[argNumber] if is present in args, otherwise the default value to string
     */
    private fun <T: Any> getToString(args: Array<String>, argNumber: Int, defaultValue: T): String {
        return if(args.size > argNumber) args[argNumber] else defaultValue.toString()
    }

    /**
     * @param fileName the file name of the generated seeds data
     * @param simulationActions the actions that are made for each seed
     * @param frequency the time that is between the generation of two schemas of each seed (array of values in milliseconds)
     * @param numberOfSeeds the number of seeds
     * @param seedsValues the number of values for each seed
     * @param seedsCommonItems the number of common items between seeds, every seed has the same shared item
     * @param lifeTime the lifeTime of each seed (in milliseconds)
     * @param unlifeTime the unlifeTime of each seed (in milliseconds)
     * @param probability the provability of each seed, at the moment each value in the seed has the same probability
     * @param durationOfSimulation the simulation duration (in milliseconds)
     *
     */
    data class GenerationParameters(val fileName: String, val simulationActions: ArrayList<SimulationAction>, val frequency: ArrayList<Long>, val numberOfSeeds: Int,
                                    val seedsValues: ArrayList<Int>, val seedsCommonItems: Int, val lifeTime: ArrayList<Long>, val unlifeTime: ArrayList<Long>,
                                    val probability: ArrayList<Double>, val durationOfSimulation: Long)

    /**
     * @param array an array list
     * @return string simulation of the array list, if it is a set with just an element returns the element, otherwise the array in the form [e1,e2,...,en]
     */
    private fun <T: Any> s(array: ArrayList<T>): String {
        return if(array.toSet().size == 1) {
            array[0].toString()
        } else {
            "[${array.joinToString(separator = ",")}]"
        }
    }

    /**
     * Function to get configuration parameters using defaults value in case the parameters are not given.
     *
     * @param args the overall arguments
     * @return the generation parameters
     */
    fun getParameters(args: Array<String>): GenerationParameters {

        /**
         * @param argNumber the index of the argument we want to extract
         * @param defaultValue the default value, if args[argNumber] is not present
         * @param decode a function that from a string return the type T, and essentially parse the string
         * @return the argNumber parameter with type T
         */
        fun <T: Any> getToType(argNumber: Int, defaultValue: T, decode: (s: String) -> T): T {
            return decode(getToString(args, argNumber, defaultValue))
        }

        val numberOfSeeds : Int = getToType(0, DEFAULT_NUMBER_OF_SEEDS){it.toInt()}
        val f = fromParametersToArrayList(numberOfSeeds, args, 1, DEFAULT_FREQUENCY) {it.toLong()}
        val simulationActions = fromParametersToArrayList(numberOfSeeds, args, 2, DEFAULT_SIMULATION_ACTIONS_STRING) {SimulationAction.from(it)}
        val numberOfSeedsValues = fromParametersToArrayList(numberOfSeeds, args, 3, DEFAULT_SEEDS_VALUES) {it.toInt()}
        val numberOfCommonItems : Int = getToType(4, DEFAULT_SEEDS_COMMON_VALUES){it.toInt()}
        val lifeTime = fromParametersToArrayList(numberOfSeeds, args, 5, DEFAULT_LIFETIME){it.toLong()}
        val unlifeTime = fromParametersToArrayList(numberOfSeeds, args, 6, DEFAULT_UNLIFETIME){it.toLong()}
        val probability = fromParametersToArrayList(numberOfSeeds, args, 7, DEFAULT_PROBABILITY){it.toDouble()}
        val duration: Long = getToType(8, DEFAULT_DURATION_OF_SIMULATION){it.toLong()}

        val defaultFileName: String = "stream-profiling/seeds_f${s(f)}_n${numberOfSeeds}_values${s(numberOfSeedsValues)}_common${numberOfCommonItems}_duration${duration}" +
                "_lifetime${s(lifeTime)}_unlifeTime${s(unlifeTime)}_simulationActions${s(simulationActions)}_probability${s(probability)}"
        val fileName: String = getToType(9, defaultFileName){ it }
        return GenerationParameters(fileName, simulationActions, f, numberOfSeeds, numberOfSeedsValues, numberOfCommonItems, lifeTime, unlifeTime, probability, duration)
    }
}