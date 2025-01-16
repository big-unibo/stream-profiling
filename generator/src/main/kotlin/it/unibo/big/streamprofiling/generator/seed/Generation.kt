package it.unibo.big.streamprofiling.generator.seed

import it.unibo.big.streamprofiling.generator.seed.SimulationActions.SimulationAction
import it.unibo.big.streamprofiling.generator.seed.SimulationActions.createSeedSimulation

/**
 * Utility object for seed generation
 */
object Generation {
    /**
     * The used alphabet for seed example
     */
    val ALPHABET: List<String> = ('A'..'Z')
        .flatMap{ ('A'..'Z').filter{it1 -> it1 <= it}.map { it2 -> it2.toString() + it } }

    /**
     * The used seed number, it increases each time the generateSeeds method is called
     */
    private var seedCount = 0

    /**
     * Method for seeds generation with a number of common elements.
     *
     * @param numberOfSeeds the total number of seeds want to generate
     * @param numberOfSeedsValues the number of elements in a seed
     * @param numberOfCommonItems number of items that are in common between a pair of seeds
     * @param fileName the file where store the simulation
     * @param simulationActions the array of simulations for each generated seed
     * @param frequency the time in that is between the generation of two schemas in the new seeds (default 1000)
     * @param lifeTime the lifetime of the new seed (default 10000000)
     * @param unlifeTime the unlife time of the new seed (default 0, infinite seed)
     * @param probability the values probability
     *
     * @return the seeds object for start the simulation
     */
    fun generateSeeds(numberOfSeeds: Int, numberOfSeedsValues: ArrayList<Int>, numberOfCommonItems: Int, fileName: String,
                      simulationActions: ArrayList<SimulationAction>, frequency: ArrayList<Long>, lifeTime: ArrayList<Long>, unlifeTime: ArrayList<Long>, probability: ArrayList<Double>): Seeds {
        require(numberOfSeedsValues.size == numberOfSeeds && simulationActions.size == numberOfSeeds &&
                frequency.size == numberOfSeeds && lifeTime.size == numberOfSeeds && unlifeTime.size == numberOfSeeds && probability.size == numberOfSeeds)
        val result = generateSeeds(numberOfSeeds, numberOfSeedsValues, numberOfCommonItems, simulationActions, frequency, lifeTime, unlifeTime, probability)
        result.toYaml(fileName)
        return result
    }

    private fun generateSeeds(numberOfSeeds: Int, numberOfSeedsValues: ArrayList<Int>, numberOfCommonItems: Int,
                              simulationActions: ArrayList<SimulationAction>, frequency: ArrayList<Long>, lifeTime: ArrayList<Long>, unlifeTime: ArrayList<Long>,
                              probability: ArrayList<Double>, commonItems: List<String> = emptyList(), from: List<Seed> = emptyList(), seeds: Set<String> = emptySet(),
                              mergeSeedsMap: Map<String, Set<String>> = emptyMap()
    ): Seeds {
        if(seeds.size < numberOfSeeds) { //check number of distinct generated seed names
            val numberOfSeedsValuesForAction = simulationActions[seeds.size].seedValues(numberOfSeedsValues[seeds.size], numberOfCommonItems)
            var mergeSeedsMapUpdate: Map<String, Set<String>> = mergeSeedsMap

            val (newCommonItems: List<String>, newSeed: List<Seed>) = when(simulationActions[seeds.size]) {
                is SimulationAction.MERGE -> {

                    val destinationSeed = (simulationActions[seeds.size] as SimulationAction.MERGE).destinationSeed
                    var seedsForMerge = numberOfSeedsValuesForAction
                    var extraValues = emptySet<String>()

                    if(mergeSeedsMap.containsKey(destinationSeed)) {
                        //change number of seeds for the generation adding extra values
                        seedsForMerge -= numberOfSeedsValues[seeds.size] - numberOfCommonItems
                        extraValues = mergeSeedsMapUpdate[destinationSeed] ?: throw IllegalStateException("A MERGE seed needs merge items values")
                    }

                    val (seed, newCommonItems, fixedValues) = getSeedElements(seedsForMerge, numberOfCommonItems, frequency, lifeTime, unlifeTime, probability, commonItems, from, seeds, extraValues)

                    if(!mergeSeedsMap.containsKey(destinationSeed)) {
                        val mergeValues = fixedValues.take(numberOfSeedsValues[seeds.size] - numberOfCommonItems).toSet()
                        mergeSeedsMapUpdate = mergeSeedsMap.plus(destinationSeed to mergeValues)
                    }
                    Pair(
                        newCommonItems,
                        createSeedSimulation(simulationActions[seeds.size], seed, newCommonItems,
                        mergeSeedsMapUpdate[destinationSeed] ?: throw IllegalStateException("A MERGE seed needs merge items values"))
                    )
                }
                else -> {
                    val (seed, newCommonItems, _) = getSeedElements(numberOfSeedsValuesForAction, numberOfCommonItems,
                        frequency, lifeTime, unlifeTime, probability, commonItems, from, seeds)
                    Pair(newCommonItems, createSeedSimulation(simulationActions[seeds.size], seed, commonItems))
                }
            }
            //recursive call
            return generateSeeds(numberOfSeeds, numberOfSeedsValues, numberOfCommonItems, simulationActions, frequency, lifeTime, unlifeTime,
                probability, newCommonItems, from + newSeed, seeds + "s$seedCount", mergeSeedsMapUpdate)
        } else {
            return Seeds(from)
        }
    }

    /**
     * Data class for seed generation wrapped in a single private method
     */
    private data class SeedElementsGeneration(val seed: Seed, val newCommonItems: List<String>, val fixedValues: List<String>)

    /**
     * Wraps seeds creation
     *
     * @param numberOfSeedValues values for generate the seed, w.r.t. the Simulation Action
     * @param extraValues other values for the seed, necessary for Merge
     */
    private fun getSeedElements(numberOfSeedValues: Int, numberOfCommonItems: Int, frequency: ArrayList<Long>, lifeTime: ArrayList<Long>, unlifeTime: ArrayList<Long>,
                                probability: ArrayList<Double>, commonItems: List<String>, from: List<Seed>, seeds: Set<String>, extraValues: Set<String> = emptySet()
    ): SeedElementsGeneration {
        val values: List<String> = if (from.isEmpty()) {
            ALPHABET.take(numberOfSeedValues) + extraValues
        } else {
            val usedValues: List<String> = from.flatMap { it -> it.values.map{it.key}.toList() }.distinct()
            ALPHABET.filter { !usedValues.contains(it) }.take(numberOfSeedValues - commonItems.size) + extraValues
        }
        val newCommonItems = if(from.isEmpty()) values.take(numberOfCommonItems) else commonItems
        val fixedValues = values.filter{x -> !newCommonItems.contains(x)}
        //add high probabilities to values that are not in common
        val valuesMap: Map<String, Double> = addFixedProbabilityToValuesList(fixedValues, probability[seeds.size]) +
                addFixedProbabilityToValuesList(newCommonItems, probability[seeds.size])
        //generate the new seed
        seedCount += 1
        val seed = Seed("s$seedCount", "s$seedCount", valuesMap, frequency[seeds.size], lifeTime[seeds.size], unlifeTime[seeds.size])

        return SeedElementsGeneration(seed, newCommonItems, fixedValues)
    }

    /**
     * @param valuesList the input list of values
     * @param probability fixed probability value
     * @return a map of fixed probabilities for each of the value of values
     */
    private fun addFixedProbabilityToValuesList(
        valuesList: List<String>, probability: Double,
    ): Map<String, Double> = valuesList.withIndex().associate { it.value to probability }
}