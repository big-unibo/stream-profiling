package it.unibo.big.streamprofiling.generator.seed

import com.charleskorn.kaml.Yaml
import it.unibo.big.streamprofiling.generator.seed.Generation.ALPHABET
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.FileOutputStream
import java.util.*
import kotlin.system.measureTimeMillis


/**
 * Writing information for a seed.
 *
 * @param time time when the data is produced
 * @param name the seed name
 * @param value the generated value
 */
@Serializable
data class SimulationRow(
    val time: Long,
    val name: String,
    val value: String
)

/**
 * A seed with information about data to generate, this kind of seed is unstoppable
 *
 * @param name the seed name, i.e. cluster name
 * @param values the values of the seed schema withs its own probabilities, for generate random schemas
 * @param frequency the time in that is between the of two schemas of the seed (in milliseconds)
 * @param lifeTime it is the time when the seed is active
 * @param unlifeTime the time when the seed is not active, if -1 the seed is active only for the lifetime (no loop)
 * @param startsTime 0 if when the seed starts have to immediately send data, >0 otherwise.
 *                     When false the seed starts with a sleep(startsLiving). DEFAULT: 0
 */
@Serializable
data class Seed(
    val name: String,
    val type: String,
    val values: Map<String, Double>,
    val frequency: Long,
    val lifeTime: Long,
    val unlifeTime: Long,
    val startsTime: Long = 0
) {
    var writingList: List<SimulationRow> = emptyList()

    init {
        require(frequency in 1..lifeTime && (unlifeTime == -1L || unlifeTime == 0L || unlifeTime >= lifeTime) && lifeTime >= 0) { "Seed parameters are not correct." }
    }

    /**
     * Auxiliar for run a co-routine for the seed, it sends message in activeLifeTime.
     * Sent messages includes the seed name as a key and the random generated schemas as payload.
     *
     * @param delayFunction the function used for delay the seed
     * @param stopCondition the stop condition for the simulation
     * @param action for send the message
     */
    private suspend fun execute(delayFunction: suspend (seedTime: Long, timeMillis: Long, elapsedTime: Long) -> Long, stopCondition: (seedTime: Long, seedStartingTime: Long) -> Boolean,
                                action: (seedTime: Long, name: String, schema: String) -> Unit) {
        var activeTime = 0L
        var seedTime = Date().time
        val seedStartingTime = seedTime
        if(startsTime > 0) {
            seedTime = delayFunction(seedTime, startsTime, 0)
        }
        val oneShotSeed = unlifeTime == -1L
        while (stopCondition(seedTime, seedStartingTime) && !(oneShotSeed && activeTime >= lifeTime)) {
            if (activeTime <= lifeTime || unlifeTime == 0L) {
                val elapsed = 5 + measureTimeMillis { //add 5 ms as elapsed sleep time
                    var schemaSeq: Set<String> = emptySet()
                    while(schemaSeq.isEmpty()) {
                        schemaSeq = values.filter { e -> e.value >= Random().nextDouble() }.keys
                    }
                    val schema = schemaSeq.joinToString()
                    action(seedTime, name, schema)
                    println("$seedTime $schema (name = $name, type = $type) - times seconds ${frequency.toDouble() / 1000}")
                }
                seedTime = delayFunction(seedTime, frequency, elapsed) //take time to send message and subtract to sleep
                activeTime += frequency
            } else {
                activeTime = 0
                seedTime = delayFunction(seedTime, unlifeTime, 0)
            }
        }
    }

    /**
     * Method for write with the seed co-routine
     * Sent messages includes the seed name as a key and the random generated schemas as payload.
     *
     * @param duration the duration of the simulation
     */
    suspend fun write(duration: Long) {
        execute({seedTime: Long, timeMillis: Long, _ -> seedTime + timeMillis }, { seedTime: Long, seedStartingTime: Long -> seedTime - seedStartingTime < duration},
            { seedTime: Long, name: String, schema : String -> writingList += listOf(SimulationRow(seedTime, name, schema)) })
    }
}

/**
 * The class that encompasses a list of seed and give methods for run the simulation
 *
 * @param seeds the input list of seeds
 */
@Serializable
class Seeds(private val seeds: List<Seed>) {
    /**
     * Writes the simulation in a file
     *
     * @param fileName the used file name for write the simulation
     * @param duration the duration of the simulation
     */
    fun write(fileName: String, duration: Long) {
        val csvPrinter = CSVPrinter(FileOutputStream(fileName).bufferedWriter(), CSVFormat.DEFAULT)
        runBlocking {
            seeds.map { x -> launch { x.write(duration) } }.joinAll()
        }
        csvPrinter.printRecord(listOf("time", "name", "value"))
        seeds.map { x -> x.writingList }.flatten().sortedBy { r -> r.time }.forEach { r -> csvPrinter.printRecord(listOf(r.time, r.name, r.value)) }
        csvPrinter.flush()
        csvPrinter.close()
    }

    /**
     * Writes on a yaml file this configuration
     *
     * @param fileName the file name
     */
    fun toYaml(fileName: String) = Yaml.default.encodeToStream(serializer(), this, FileOutputStream(fileName))

    fun writeAsCsv(fileName: String) {
        val csvPrinter = CSVPrinter(FileOutputStream(fileName).bufferedWriter(), CSVFormat.DEFAULT)
        //write header
        csvPrinter.printRecord(listOf("name", "type", "frequency", "lifeTime", "unlifeTime", "startsTime") + ALPHABET)
        //write values
        seeds.forEach{
            csvPrinter.printRecord(
                listOf(it.name, it.type, it.frequency, it.lifeTime, it.unlifeTime, it.startsTime) + ALPHABET.map{it2 -> it.values.getOrDefault(it2, 0)})
        }
        csvPrinter.flush()
        csvPrinter.close()
    }
}
