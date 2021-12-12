import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.neo4j.driver.*
import kotlin.math.pow
import kotlin.streams.*
import java.util.stream.*

import org.neo4j.driver.Values.parameters;
import java.io.File

val fileName = "inputData.json"
var inputData: JsonData? = null
var dbWriter: DBWriter = DBWriter()

const val defaultPopulation = 1000
val degree = 4

var relations: MutableList<Pair<Int, Int>> = ArrayList(defaultPopulation * degree/2)

class DBWriter :
    AutoCloseable {

    private val uri = "bolt://localhost:7687"
    private val user = "thamus"
    private val password = "thamus"
    private val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    private val session = driver.session()
//    private val session = driver.session(SessionConfig.forDatabase("thamus"))

    @Throws(Exception::class)
    override fun close() {
        driver.close()
    }

    fun clearDatabase(){
        val queryString = "MATCH (n) DETACH DELETE n"
        session.writeTransaction{ tx -> tx.run(queryString)}
    }

    fun deleteAllRelations(){
        val queryString = "MATCH (:Person)-[r:KNOWS]-() DELETE r"
        session.writeTransaction{ tx -> tx.run(queryString)}
    }

    fun deleteNodesWithNoRelation(){
        val queryString = "MATCH (n) WHERE NOT (n)--() DELETE (n)"
        session.writeTransaction{ tx -> tx.run(queryString)}
    }

    fun savePersonToDatabase(person: Person){

        session.writeTransaction { tx ->
            val result: Result = tx.run(
                "CREATE (p:Person) " +
                        "SET p.age = \$age " +
                        "SET p.health = \$health " +
                        "SET p.riskOfInfection = \$riskOfInfection " +
                        "SET p.riskOfInfectingOthers = \$riskOfInfectingOthers " +
                        "SET p.isVaccinated = \$isVaccinated " +
                        "SET p.isSick = false " +
                        "RETURN id(p) ",
                parameters("age", person.age,
                    "health", person.health,
                    "riskOfInfection", person.riskOfInfection,
                    "riskOfInfectingOthers", person.riskOfInfectingOthers,
                    "isVaccinated", person.isVaccinated)
            )
            println(result.single()[0])
        }
    }

    fun saveRelations(){

        for(relation in relations){
            val queryString = "MATCH (p1:Person) WHERE id(p1) = " + relation.first + " " +
                                "MATCH (p2:Person) WHERE id(p2) = " + relation.second + " " +
                                "CREATE (p1)-[rel:KNOWS]->(p2) "
            session.writeTransaction{ tx -> tx.run(queryString)}
        }
    }


    fun nodeDegree(id: Int): Int {
        var out: Int = 0
        val queryString = "MATCH (p1:Person) WHERE id(p1) = " + id +
                            " MATCH (p1)--(p2) " +
                            "RETURN count(p2)"
        session.writeTransaction{ tx ->
            val result: Result = tx.run(queryString)
            out = result.single()[0].asInt()
        }

        return out
    }

    fun deleteRelation(id1: Int, id2: Int){
        val queryString = "MATCH (p1:Person) WHERE id(p1) = " + id1 +
                            " MATCH (p2:Person) WHERE id(p2) = " + id2 +
                            " MATCH (p1)-[r:KNOWS]-(p2)" +
                            " DELETE r"
        session.writeTransaction{ tx -> tx.run(queryString)}
    }

    fun addRelation(id1: Int, id2: Int){
        val queryString = "MATCH (p1:Person) WHERE id(p1) = " + id1 + " " +
                            "MATCH (p2:Person) WHERE id(p2) = " + id2 + " " +
                            "CREATE (p1)-[rel:KNOWS]->(p2) "
        session.writeTransaction{ tx -> tx.run(queryString)}
    }

    fun setupSeparateGraphsCounter(counter: Int){
        val queryString1 = "CALL gds.graph.create(\n" +
                            "    'myGraph" + counter + "',\n" +
                            "    'Person',\n" +
                            "    'KNOWS',\n" +
                            "    {}\n" +
                            ")\n"
        session.writeTransaction{ tx -> tx.run(queryString1)}
    }

    fun destroySeparateGraphCounter(counter: Int){
        val queryString = "CALL gds.graph.drop('myGraph$counter')"
        session.writeTransaction{ tx -> tx.run(queryString)}
    }

    fun numberOfSeparateGraphs(counter: Int): Int{
        setupSeparateGraphsCounter(counter)
        var out: Int = 0
        val queryString2 = "CALL gds.wcc.stream('myGraph" + counter + "') " +
                            "YIELD nodeId, componentId " +
                            "RETURN count(DISTINCT componentId)"
//        val queryString2 = "CALL gds.wcc.stream(" +
//                            "{ nodeProjection: 'Person', " +
//                            "  relationshipProjection: 'KNOWS'}) " +
//                            "YIELD nodeId, componentId " +
//                            "RETURN count(DISTINCT componentId)"
        session.writeTransaction{ tx ->
            val result: Result = tx.run(queryString2)
            out = result.single()[0].asInt()
        }

        return out
    }

}


//    fun savePeopleToDatabase(){
//        var queryString: String = ""
//        var counter = 0
//
//        for(person in people){
//            queryString = queryString + "CREATE (p"+counter+":Person) " +
//                                            "SET p"+counter+".age = " + person.age +
//                                            " SET p"+counter+".health = " + person.health +
//                                            " SET p"+counter+".riskOfInfection = " + person.riskOfInfection +
//                                            " SET p"+counter+".riskOfInfectingOthers = " + person.riskOfInfectingOthers +
//                                            " SET p"+counter+".isVaccinated = " + person.isVaccinated +
//                                            " SET p"+counter+".isSick = false "
//            counter++
//
//            if(counter.mod(10) == 0 || counter == people.size){
//                driver.session().writeTransaction {tx -> tx.run(queryString)}
//                queryString = ""
//            }
//        }
//    }

data class Person(var age: Int,
                  var health: Double,
                  var riskOfInfection: Double,
                  var riskOfInfectingOthers: Double,
                  var isVaccinated: Boolean)

@Serializable
data class JsonData(var totalPopulation: Int,
                    var agePopularity: ArrayList<Pair<String, Double>>,
                    var preexistingConditionPopularity: Double,
                    var preexistingConditionImpact: Double,
                    var avgPopulationHealth: Double,
                    var populationVaccinated: Double,
                    var populationWearingMasks: Double)

fun readInputData(){

    val inputString = File(fileName).readText()

    inputData = Json.decodeFromString<JsonData>(inputString)
}


fun generateAndSavePeople(){

    if(inputData != null){

        val data: JsonData = inputData!!
        for(ageRange in data.agePopularity ){
            val lowEnd = ageRange.first.substringBefore('-', "0").toInt()
            val highEnd = ageRange.first.substringAfter('-', "99").toInt()
            val percent = ageRange.second.toDouble()/100

            for(i in 1..(data.totalPopulation*percent).toInt()){
                val age = (lowEnd..highEnd).random()

                var health = data.avgPopulationHealth
                if((1..100).random() < data.preexistingConditionPopularity)
                    health-=data.preexistingConditionImpact

                val riskOfInfectingOthers = 0.1
                val riskOfInfection = 0.1
                val isVaccinated = (1..100).random() < data.populationVaccinated

                var person: Person = Person(age, health, riskOfInfection, riskOfInfectingOthers, isVaccinated)

                dbWriter.savePersonToDatabase(person)
            }
        }
    }
}

fun generateRegularRelations(){
    relations.clear()
    dbWriter.deleteAllRelations()

    val population = inputData!!.totalPopulation

    for(i in 0 until population){
        if(degree.mod(2)==0) {
            for(j in 1..degree/2)
                relations.add(Pair(i, (i + j).mod(population)))
//                relations[i] = (i+j).mod(population)
        }
        else{
            if(i.mod(2) == 0){
                for(j in 1..degree/2 + 1)
                    relations.add(Pair(i, (i + j).mod(population)))
//                    relations[i] = (i+j).mod(population)

            }
            else{
                for(j in 1..degree/2)
                    relations.add(Pair(i, (i + j).mod(population)))
//                    relations[i] = (i+j).mod(population)
            }
        }
    }
//    dbWriter.saveRelations()

}

fun generateNodeDegreeInfo(id: Int, range: Int): Map<Int,Int>{
    val lowerRange = (id - range).mod(inputData!!.totalPopulation)
    val higherRange = (id + range).mod(inputData!!.totalPopulation)

    var result: MutableMap<Int, Int> = HashMap<Int, Int>()

    if(lowerRange < higherRange){
        for(i in lowerRange .. higherRange){
            if(i != id){
                result[i] = dbWriter.nodeDegree(i) + 1
            }
        }
    }
    else{
        for(i in lowerRange .. inputData!!.totalPopulation){
            if(i != id){
                result[i] = dbWriter.nodeDegree(i) + 1
            }
        }
        for(i in 0 .. higherRange){
            if(i != id){
                result[i] = dbWriter.nodeDegree(i) + 1
            }
        }
    }
    return result
}

fun drawWeightedRandomNode(nodeDegrees: Map<Int,Int>, pi: Double): Int{

    var weights: MutableMap<Int,Double> = HashMap(nodeDegrees.size)

    var weightSum: Double = 0.0

    //change to parallel ?
    nodeDegrees.forEach { entry ->
        val weight = (entry.value).toDouble().pow(pi)
        weights[entry.key]  = weight
        weightSum+=weight
    }

    var rand = (0..weightSum.toInt()).random().toDouble()

    weights.forEach{ node ->
        rand-=node.value
        if(rand <= 0)
            return node.key
    }

    return weights.keys.random()
}

fun rewireRelationsNew(){
    val p = 0.6
    val pi = 5.0

    val population = inputData?.totalPopulation ?: defaultPopulation

    val nodeDegrees: MutableMap<Int, Int> = HashMap(population)

    for(i in 0..population)
        nodeDegrees[i] = degree

    val relationsToRemove: MutableList<Pair<Int, Int>> = ArrayList(relations.size/3)
    val relationsToAdd: MutableList<Pair<Int, Int>> = ArrayList(relations.size/3)

    for(i in 1..3){
        relations.parallelStream().filter { (0..100).random() <= (p / 3) * 100 }
            .forEach { relation ->

                relationsToRemove.add(relation)

                val higherDegreeNodeId = if(nodeDegrees[relation.first]!! >= nodeDegrees[relation.second]!!)
                                        relation.first else relation.second

                val lowerDegreeNodeId = if(higherDegreeNodeId == relation.first)
                                        relation.second else relation.first

                nodeDegrees[lowerDegreeNodeId] = nodeDegrees[lowerDegreeNodeId]!! - 1

                var newNodeId: Int
                do {
                    newNodeId = drawWeightedRandomNode(nodeDegrees, pi)
                }while(newNodeId == higherDegreeNodeId)

                nodeDegrees[newNodeId] = nodeDegrees[newNodeId]!! + 1

                relationsToAdd.add(Pair(higherDegreeNodeId, newNodeId))

            }

        for(relation in relationsToRemove)
            relations.remove(relation)

        for(relation in relationsToAdd)
            relations.add(relation)

        relationsToRemove.clear()
        relationsToAdd.clear()
    }

    dbWriter.saveRelations()

}

fun rewireRelations(){

    val p = 0.6
    val pi = 5.0

    for(i in 1..3){
        var removedRelations: MutableList<Pair<Int, Int>> = ArrayList()
        var relationsToAdd: MutableList<Pair<Int, Int>> = ArrayList()

        //change to parallel stream
        //remove saving relations to db
        for(relation in relations){
            if((0..100).random() <= (p/3)*100){
                dbWriter.deleteRelation(relation.first, relation.second)
                removedRelations.add(relation)

                //change taking node degree from DB to parallel stream on relations map
                val nodeToGetRelation = if (dbWriter.nodeDegree(relation.first) >= dbWriter.nodeDegree(relation.second))
                                            relation.first
                                        else relation.second
                val nodeDegrees = generateNodeDegreeInfo(nodeToGetRelation, inputData!!.totalPopulation/2)
                val newNodeId = drawWeightedRandomNode(nodeDegrees, pi)

                dbWriter.addRelation(nodeToGetRelation, newNodeId)
                relationsToAdd.add(Pair(nodeToGetRelation, newNodeId))
            }
        }
        for(r in removedRelations){
            relations.remove(r)
        }
        for(r in relationsToAdd){
            relations.add(r)
        }
    }
}

fun main(args: Array<String>) {

//    dbWriter.clearDatabase()

    readInputData()

    generateAndSavePeople()

    var rewireCounter = 0
    do {
        generateRegularRelations()
//        rewireRelations()
        rewireRelationsNew()

        rewireCounter++
        println(rewireCounter)


        val numberOfGraphs = dbWriter.numberOfSeparateGraphs(rewireCounter)
//        println("number of graphs: $numberOfGraphs")
    }while(numberOfGraphs > 1)

//    for(i in 1..rewireCounter){
//        dbWriter.destroySeparateGraphCounter(i)
//    }

}