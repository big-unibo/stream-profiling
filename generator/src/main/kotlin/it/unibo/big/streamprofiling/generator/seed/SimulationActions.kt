package it.unibo.big.streamprofiling.generator.seed

object SimulationActions {

    sealed class SimulationAction {
        object STATIC : SimulationAction()
        object FADEIN : SimulationAction()
        object SLIDE : SimulationAction()

        data class MERGE(val destinationSeed: String) : SimulationAction()
        data class SPLIT(val destinationSeed: String) : SimulationAction()

        companion object {
            fun from(value: String): SimulationAction = when(value) {
                "STATIC" -> STATIC
                "SLIDE" -> SLIDE
                "FADEIN" -> FADEIN
                else -> {
                    // Check if value starts with "SPLIT:" or "MERGE:"
                    if (value.startsWith("SPLIT:") || value.startsWith("MERGE:")) {
                        val parts = value.split(":")
                        val seed = parts[1]
                        if (parts[0] == "SPLIT") SPLIT(seed) else MERGE(seed)
                    } else {
                        // Input value doesn't match any expected values
                        STATIC
                    }
                }
            }
        }

        override fun toString(): String {
            return when(this) {
                STATIC -> "STATIC"
                SLIDE -> "SLIDE"
                FADEIN -> "FADEIN"
                else -> {super.toString()}
            }
        }

        /**
         * From an action return the number of seed values mapped to it.
         *
         * @param initialValues for the first seed
         * @param commonItems the common items for every seed
         *
         * @return the total number of values for each seed action
         */
        fun seedValues(initialValues: Int, commonItems: Int): Int = when(this) {
            STATIC -> initialValues
            FADEIN -> initialValues
            SLIDE -> (initialValues * 2) - commonItems
            is SPLIT -> (initialValues * 3) - (2 * commonItems)
            is MERGE -> (initialValues * 2) - commonItems
        }
    }

    fun createSeedSimulation(simulationAction: SimulationAction, seed: Seed, commonItems: List<String>, mergeSeedsValues: Set<String> = emptySet()): List<Seed> {
        return when(simulationAction) {
            SimulationAction.STATIC -> listOf(seed)
            SimulationAction.FADEIN -> listOf(seed.copy(startsTime = seed.unlifeTime))
            SimulationAction.SLIDE -> {
                //ensure that value maps has
                val simulationItems = seed.values.filterKeys { x -> !commonItems.contains(x) }
                val seedsBlocks = simulationItems.toList().chunked(simulationItems.size / 2)
                slide(seed, commonItems, seedsBlocks[0], seedsBlocks[1], seed.name)
            }
            is SimulationAction.SPLIT -> {
                val destinationSeed = simulationAction.destinationSeed
                val simulationItems = seed.values.filterKeys { x -> !commonItems.contains(x) }
                val numberOfBlocks = 3 //3 blocks, one for start and two for the two split clusters
                val seedsBlocks = simulationItems.toList().chunked(simulationItems.size / numberOfBlocks)

                slide(seed, commonItems, seedsBlocks[0], seedsBlocks[1], seed.name, 2) +
                        slide(seed, commonItems, seedsBlocks[0], seedsBlocks[2], destinationSeed, 2)
            }
            is SimulationAction.MERGE -> {
                require(mergeSeedsValues.isNotEmpty())
                val destinationSeed = simulationAction.destinationSeed
                //ensure that value maps has
                val simulationItems = seed.values.filterKeys { x -> !commonItems.contains(x) }
                val (secondBlock, firstBlock) = simulationItems.toList().partition { mergeSeedsValues.contains(it.first) }
                slide(seed, commonItems, firstBlock, secondBlock, destinationSeed)
            }
        }
    }

    /**
     * @param seed the input seed
     * @param commonItems the set of common items in each step
     * @param firstBlock the map of values in the first simulation block
     * @param secondBlock the map of values in the second simulation block
     * @param destinationSeed the name of the destination seed
     * @param frequencyMultiplier the frequency multiplier, for increase delay in the generation in case of simultaneous actions (e.g. slide)
     *
     * @return slide seeds in 4 step, from first block to second block
     */
    private fun slide(
        seed: Seed,
        commonItems: List<String>,
        firstBlock: List<Pair<String, Double>>,
        secondBlock: List<Pair<String, Double>>,
        destinationSeed: String,
        frequencyMultiplier: Long = 1
    ): List<Seed> {
        require(firstBlock.size == secondBlock.size)
        val seedLifeTime = (seed.lifeTime / (firstBlock.size + 1)) //+1 for first step
        val seedFrequency = (if(seed.frequency > seedLifeTime) seedLifeTime else seed.frequency) * frequencyMultiplier
        val commonItemsMap = seed.values.filterKeys { x -> commonItems.contains(x) }

        //first step
        val firstSeed = Seed(seed.name, seed.type, commonItemsMap + firstBlock, seedFrequency, seedLifeTime, -1)
        // other seeds
        val otherSeeds = List(firstBlock.size) { index ->
            val seedName = if (index < firstBlock.size / 2) seed.name else destinationSeed
            val seedType = if (index < firstBlock.size / 2) seed.type else destinationSeed

            Seed(seedName, seedType, commonItemsMap + firstBlock.filterIndexed { i, _ -> i > index } + secondBlock.filterIndexed { i, _ -> i <= index },
                seedFrequency, seedLifeTime, if (index == firstBlock.size - 1) seed.unlifeTime else -1, seedLifeTime * (index + 1))
        }
        return listOf(firstSeed) + otherSeeds
    }

}