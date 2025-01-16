package it.unibo.big.streamprofiling.generator

import it.unibo.big.streamprofiling.generator.seed.Generation

/**
 * Run the seeds' simulation from new generated data
 */
fun main(args: Array<String>) {
    val (fileName, simulationActions, f, numberOfSeeds, numberOfSeedsValues, numberOfCommonItems, lifeTime, unlifeTime, probability, duration) = Constants.getParameters(
        args
    )

    val seeds = Generation.generateSeeds(
        numberOfSeeds,
        numberOfSeedsValues,
        numberOfCommonItems,
       "${fileName}.yml", simulationActions, f, lifeTime, unlifeTime, probability
    )

    seeds.writeAsCsv("${fileName}.csv")
    seeds.write("${fileName}_SIM.csv", duration)
}