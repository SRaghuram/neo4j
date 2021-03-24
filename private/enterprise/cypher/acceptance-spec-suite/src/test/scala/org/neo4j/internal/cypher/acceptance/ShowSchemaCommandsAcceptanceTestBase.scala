/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.graphdb.schema.IndexSettingImpl
import org.neo4j.values.storable.RandomValues

import java.util.concurrent.ThreadLocalRandom

abstract class ShowSchemaCommandsAcceptanceTestBase extends SchemaCommandsAcceptanceTestBase {

  val defaultBtreeOptionsString: String =
    "{indexConfig: {" +
      s"`$cartesian3dMax`: [1000000.0, 1000000.0, 1000000.0]," +
      s"`$cartesian3dMin`: [-1000000.0, -1000000.0, -1000000.0]," +
      s"`$cartesianMax`: [1000000.0, 1000000.0]," +
      s"`$cartesianMin`: [-1000000.0, -1000000.0]," +
      s"`$wgs3dMax`: [180.0, 90.0, 1000000.0]," +
      s"`$wgs3dMin`: [-180.0, -90.0, -1000000.0]," +
      s"`$wgsMax`: [180.0, 90.0]," +
      s"`$wgsMin`: [-180.0, -90.0]}, " +
      s"indexProvider: '$nativeProvider'}"

  val defaultBtreeOptionsMap: Map[String, Object] = Map(
    "indexConfig" -> Map(
      cartesian3dMax -> Array(1000000.0, 1000000.0, 1000000.0),
      cartesian3dMin -> Array(-1000000.0, -1000000.0, -1000000.0),
      cartesianMax -> Array(1000000.0, 1000000.0),
      cartesianMin -> Array(-1000000.0, -1000000.0),
      wgs3dMax -> Array(180.0, 90.0, 1000000.0),
      wgs3dMin -> Array(-180.0, -90.0, -1000000.0),
      wgsMax -> Array(180.0, 90.0),
      wgsMin -> Array(-180.0, -90.0)),
    "indexProvider" -> nativeProvider)

  val label2 = "Label2"
  val labelWhitespace = "Label 1"
  val labelWhitespace2 = "Label 2"
  val labelBackticks = "`Label``3`"

  val relTypeWhitespace = "reltype 3"
  val relTypeBackticks = "`rel`type`"

  val propWhitespace = "prop 1"
  val propWhitespace2 = "prop 2"
  val propBackticks = "`prop`4"
  val propBackticks2 = "`prop5``"

  private val random: ThreadLocalRandom = ThreadLocalRandom.current()
  val randomValues: RandomValues = RandomValues.create(random)

  // Constraint help methods

  def createDefaultConstraints(): Unit = {
    createDefaultUniquenessConstraint()
    createDefaultNodeKeyConstraint()
    createDefaultNodeExistsConstraint()
    createDefaultRelExistsConstraint()
    graph.awaitIndexesOnline()
  }

  def createConstraint(constraintType: ShowConstraintType, name: String, label: String, property: String): Option[Map[String, Any]] = {
    val escapedName = s"`${escapeBackticks(name)}`"
    val escapedLabel = s"`${escapeBackticks(label)}`"
    val escapedProperty = s"`${escapeBackticks(property)}`"
    var randomOptionsMap: Option[Map[String, Any]] = None

    val query = constraintType match {
      case UniqueConstraints =>
        val randomOptions = randomBtreeOptions()
        randomOptionsMap = Some(randomOptions._2)
        s"CREATE CONSTRAINT $escapedName ON (n:$escapedLabel) ASSERT (n.$escapedProperty) IS UNIQUE OPTIONS ${randomOptions._1}"
      case NodeKeyConstraints =>
        val randomOptions = randomBtreeOptions()
        randomOptionsMap = Some(randomOptions._2)
        s"CREATE CONSTRAINT $escapedName ON (n:$escapedLabel) ASSERT (n.$escapedProperty) IS NODE KEY OPTIONS ${randomOptions._1}"
      case _: NodeExistsConstraints =>
        s"CREATE CONSTRAINT $escapedName ON (n:$escapedLabel) ASSERT (n.$escapedProperty) IS NOT NULL"
      case _: RelExistsConstraints =>
        s"CREATE CONSTRAINT $escapedName ON ()-[r:$escapedLabel]-() ASSERT (r.$escapedProperty) IS NOT NULL"
      case unexpectedType =>
        throw new IllegalArgumentException(s"Unexpected constraint type for constraint create command: ${unexpectedType.prettyPrint}")
    }
    executeSingle(query)
    randomOptionsMap
  }

  def createDefaultUniquenessConstraint(): Unit = graph.createUniqueConstraintWithName("constraint1", label, prop)

  def createDefaultNodeKeyConstraint(): Unit = graph.createNodeKeyConstraintWithName("constraint2", label2, prop2)

  def createDefaultNodeExistsConstraint(): Unit = graph.createNodeExistenceConstraintWithName("constraint3", label, prop2)

  def createDefaultRelExistsConstraint(): Unit = graph.createRelationshipExistenceConstraintWithName("constraint4", relType, prop)

  // Create statements help methods

  def dropAllFromNames(names: List[String], schemaType: String): Unit = {
    names.foreach(name => executeSingle(s"DROP $schemaType `$name`"))
  }

  def recreateAllFromCreateStatements(createStatements: List[String]): Unit = {
    createStatements.foreach(statement => executeSingle(statement))
  }

  // Options help methods

  def assertCorrectOptionsMap(options: Object, correctOptions: Map[String, Any]): Unit = {
    options match {
      case optionsMap: Map[String, Any] =>
        optionsMap.keys should be(Set("indexConfig", "indexProvider"))
        optionsMap("indexProvider") should be(correctOptions("indexProvider"))
        optionsMap("indexConfig") match {
          case config: Map[String, Any] =>
            val correctConfig = correctOptions("indexConfig").asInstanceOf[Map[String, Any]]
            config.keys should be(correctConfig.keys)
            config.keys.foreach(key => config(key) should be(correctConfig(key)))
          case noMap => fail(s"Index config was on the wrong format, expected a Map, but got a ${noMap.getClass}")
        }
      case noMap => fail(s"Index options was on the wrong format, expected a Map, but got a ${noMap.getClass}")
    }
  }

  def randomBtreeOptions(): (String, Map[String, Object]) = {
    val provider = randomBtreeProvider()
    val indexConfig = randomBtreeSetting()
    val optionsString = s"{indexConfig: {${indexConfig._1}}, indexProvider: '$provider'}"
    val optionsMap = Map("indexConfig" -> indexConfig._2, "indexProvider" -> provider)
    (optionsString, optionsMap)
  }

  private def randomBtreeProvider(): String = {
    val availableProviders = SchemaIndex.values().map(value => value.providerName())
    randomValues.among(availableProviders)
  }

  private def randomBtreeSetting(): (String, Map[String, Array[Double]]) = {
    val indexSettings: Array[IndexSettingImpl] = IndexSettingImpl.values
    val configMap = indexSettings.foldLeft(Map.empty[String, Array[Double]])((acc, setting) => {
      val settingName = setting.getSettingName
      if (settingName.startsWith("spatial")) {
        val settingValue = randomSpatialValue(setting)
        acc + (settingName -> settingValue)
      } else {
        acc
      }
    })

    val configString = configMap.map {
      case (key, value) => s"`$key`: [${value.mkString(", ")}]"
    }.mkString(", ")

    (configString, configMap)
  }

  private def randomSpatialValue(indexSetting: IndexSettingImpl): Array[Double] = {
    indexSetting match {
      case IndexSettingImpl.SPATIAL_CARTESIAN_MIN =>
        negative(randomValues.nextCartesianPoint.coordinate)
      case IndexSettingImpl.SPATIAL_CARTESIAN_MAX =>
        positive(randomValues.nextCartesianPoint.coordinate)
      case IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN =>
        negative(randomValues.nextCartesian3DPoint.coordinate)
      case IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX =>
        positive(randomValues.nextCartesian3DPoint().coordinate())
      case IndexSettingImpl.SPATIAL_WGS84_MIN =>
        negative(randomValues.nextGeographicPoint().coordinate())
      case IndexSettingImpl.SPATIAL_WGS84_MAX =>
        positive(randomValues.nextGeographicPoint.coordinate)
      case IndexSettingImpl.SPATIAL_WGS84_3D_MIN =>
        negative(randomValues.nextGeographic3DPoint.coordinate)
      case IndexSettingImpl.SPATIAL_WGS84_3D_MAX =>
        positive(randomValues.nextGeographic3DPoint.coordinate)
      case setting => fail("Unexpected spatial index setting: " + setting.getSettingName)
    }
  }

  private def positive(values: Array[Double]): Array[Double] = {
    values.map(i => Math.abs(i))
  }

  private def negative(values: Array[Double]): Array[Double] = {
    values.map(i => -Math.abs(i))
  }

  def withoutColumns(result: List[Map[String, Any]], columns: List[String]): List[Map[String, Any]] = {
    result.map(innerMap => innerMap.filterNot(entry => columns.contains(entry._1)))
  }

  def escapeBackticks(str: String): String = str.replaceAll("`", "``")
}
