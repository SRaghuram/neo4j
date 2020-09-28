/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.spatial.Point
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.Values

class SpatialFunctionsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("toString on points") {
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({x:1, y:2})) AS s").toList should equal(List(Map("s" -> "point({x: 1.0, y: 2.0, crs: 'cartesian'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude:1, latitude:2, height:3})) AS s").toList should equal(List(Map("s" -> "point({x: 1.0, y: 2.0, z: 3.0, crs: 'wgs-84-3d'})")))
  }

  test("2D geometric points should validate and possibly wrap coordinates") {
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 180, latitude: 0})) AS s").toList should
      equal(List(Map("s" -> "point({x: 180.0, y: 0.0, crs: 'wgs-84'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: -180, latitude: 0})) AS s").toList should
      equal(List(Map("s" -> "point({x: -180.0, y: 0.0, crs: 'wgs-84'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 190, latitude: 0})) AS s").toList should
      equal(List(Map("s" -> "point({x: -170.0, y: 0.0, crs: 'wgs-84'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: -190, latitude: 0})) AS s").toList should
      equal(List(Map("s" -> "point({x: 170.0, y: 0.0, crs: 'wgs-84'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 360, latitude: 0})) AS s").toList should
      equal(List(Map("s" -> "point({x: 0.0, y: 0.0, crs: 'wgs-84'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: -360, latitude: 0})) AS s").toList should
      equal(List(Map("s" -> "point({x: 0.0, y: 0.0, crs: 'wgs-84'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 540, latitude: 0})) AS s").toList should
      equal(List(Map("s" -> "point({x: 180.0, y: 0.0, crs: 'wgs-84'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: -540, latitude: 0})) AS s").toList should
      equal(List(Map("s" -> "point({x: -180.0, y: 0.0, crs: 'wgs-84'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 0, latitude: 90})) AS s").toList should
      equal(List(Map("s" -> "point({x: 0.0, y: 90.0, crs: 'wgs-84'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 0, latitude: -90})) AS s").toList should
      equal(List(Map("s" -> "point({x: 0.0, y: -90.0, crs: 'wgs-84'})")))

    // no wrapping for y coordinates
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 0, latitude: 91})) AS s", errorType = "InvalidArgumentException",
      message = "Cannot create WGS84 point with invalid coordinate: [0.0, 91.0]. Valid range for Y coordinate is [-90, 90].")
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 0, latitude: -91})) AS s", errorType = "InvalidArgumentException",
      message = "Cannot create WGS84 point with invalid coordinate: [0.0, -91.0]. Valid range for Y coordinate is [-90, 90].")
  }

  test("3D geometric points should validate and possibly wrap coordinates") {
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 180, latitude: 0, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: 180.0, y: 0.0, z: 9999.0, crs: 'wgs-84-3d'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: -180, latitude: 0, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: -180.0, y: 0.0, z: 9999.0, crs: 'wgs-84-3d'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 190, latitude: 0, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: -170.0, y: 0.0, z: 9999.0, crs: 'wgs-84-3d'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: -190, latitude: 0, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: 170.0, y: 0.0, z: 9999.0, crs: 'wgs-84-3d'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 360, latitude: 0, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: 0.0, y: 0.0, z: 9999.0, crs: 'wgs-84-3d'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: -360, latitude: 0, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: 0.0, y: 0.0, z: 9999.0, crs: 'wgs-84-3d'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 540, latitude: 0, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: 180.0, y: 0.0, z: 9999.0, crs: 'wgs-84-3d'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: -540, latitude: 0, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: -180.0, y: 0.0, z: 9999.0, crs: 'wgs-84-3d'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 0, latitude: 90, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: 0.0, y: 90.0, z: 9999.0, crs: 'wgs-84-3d'})")))
    executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN toString(point({longitude: 0, latitude: -90, height: 9999})) AS s").toList should
      equal(List(Map("s" -> "point({x: 0.0, y: -90.0, z: 9999.0, crs: 'wgs-84-3d'})")))

    // no wrapping for y coordinates
    failWithError(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN toString(point({longitude: 0, latitude: 91, height: 9999})) AS s",
      errorType = "InvalidArgumentException",
      message = "Cannot create WGS84 point with invalid coordinate: [0.0, 91.0, 9999.0]. Valid range for Y coordinate is [-90, 90].")
    failWithError(Configs.InterpretedAndSlottedAndPipelined,
      query = "RETURN toString(point({longitude: 0, latitude: -91, height: 9999})) AS s",
      errorType = "InvalidArgumentException",
      message = "Cannot create WGS84 point with invalid coordinate: [0.0, -91.0, 9999.0]. Valid range for Y coordinate is [-90, 90].")
  }

  test("point function should work with literal map") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({latitude: 12.78, longitude: 56.7}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with literal map and cartesian coordinates") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x: 2.3, y: 4.5, crs: 'cartesian'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.3, 4.5))))
  }

  test("point function should work with literal map and 3D cartesian coordinates") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "RETURN point({x: 2.3, y: 4.5, z: 6.7, crs: 'cartesian-3D'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 2.3, 4.5, 6.7))))
  }

  test("point function should work with literal map and srid") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x: 2.3, y: 4.5, srid: 4326}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 2.3, 4.5))))
  }

  test("point function should work with literal map and geographic coordinates") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({longitude: 2.3, latitude: 4.5, crs: 'WGS-84'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 2.3, 4.5))))
  }

  test("point function should work with node with only valid properties") {
    val result = executeWith(Configs.InterpretedAndSlotted, "CREATE (n {latitude: 12.78, longitude: 56.7}) RETURN point(n) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with node with some invalid properties") {
    val result = executeWith(Configs.InterpretedAndSlotted,
      "CREATE (n {latitude: 12.78, longitude: 56.7, banana: 'yes', some: 1.2, andAlso: [1,2]}) RETURN point(n) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should fail gracefully with node with missing properties") {
   failWithError(Configs.InterpretedAndSlotted, "CREATE (n {latitude: 12.78, y: 2}) RETURN point(n) as point",
     "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'")
  }

  test("point function should work with relationship with only valid properties") {
    executeSingle("CREATE ()-[:REL {x: 1, y: 2, z: 3, crs: 'cartesian-3D'}]->()")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH ()-[r:REL]->() RETURN point(r) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1, 2, 3))))
  }

  test("point function should work with relationship with some invalid properties") {
    executeSingle("CREATE ()-[:REL {x: 1, a: 2, y: 3, z: 4, prop: 'junk', crs: 'cartesian-3D'}]->()")

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH ()-[r:REL]->() RETURN point(r) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1, 3, 4))))
  }

  test("point function should fail gracefully with relationship with missing properties") {
    executeSingle("CREATE ()-[:REL {x: 1, a: 2, z: 3, crs: 'cartesian-3D'}]->()")
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "MATCH ()-[r:REL]->() RETURN point(r) as point",
      "A cartesian-3d point must contain 'x', 'y' and 'z'")
  }

  test("point function should not work with NaN or infinity") {
    for(invalidDouble <- Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)) {
      failWithError(Configs.InterpretedAndSlottedAndPipelined,
        "RETURN point({x: 2.3, y: $v}) as point", "Cannot create a point with non-finite coordinate values", params = Map(("v", invalidDouble)))
    }
  }

  test("point function should not work with literal map and incorrect cartesian CRS") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined,
      "RETURN point({x: 2.3, y: 4.5, crs: 'cart'}) as point", "Unknown coordinate reference system: cart")
  }

  test("point function should not work with literal map of 2 coordinates and incorrect cartesian-3D crs") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x: 2.3, y: 4.5, crs: 'cartesian-3D'}) as point",
      "Cannot create point with 3D coordinate reference system and 2 coordinates. Please consider using equivalent 2D coordinate reference system")
  }

  test("point function should not work with literal map of 3 coordinates and incorrect cartesian crs") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined,
      "RETURN point({x: 2.3, y: 4.5, z: 6.7, crs: 'cartesian'}) as point",
      "Cannot create point with 2D coordinate reference system and 3 coordinates. Please consider using equivalent 3D coordinate reference system")
  }

  test("point function should not work with literal map and incorrect geographic CRS") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x: 2.3, y: 4.5, crs: 'WGS84'}) as point",
      "Unknown coordinate reference system: WGS84")
  }

  test("point function should not work with literal map of 2 coordinates and incorrect WGS84-3D crs") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x: 2.3, y: 4.5, crs: 'WGS-84-3D'}) as point",
      "Cannot create point with 3D coordinate reference system and 2 coordinates. Please consider using equivalent 2D coordinate reference system")
  }

  test("point function should not work with literal map of 3 coordinates and incorrect WGS84 crs") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined,
      "RETURN point({x: 2.3, y: 4.5, z: 6.7, crs: 'wgs-84'}) as point",
        "Cannot create point with 2D coordinate reference system and 3 coordinates. Please consider using equivalent 3D coordinate reference system")
  }

  test("point function should work with integer arguments") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x: 2, y: 4}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 2, 4))))
  }

  // We can un-ignore this if/when we re-enable strict map checks in PointFunction.scala
  ignore("point function should throw on unrecognized map entry") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x: 2, y:3, a: 4}) as point", "Unknown key 'a' for creating new point")
  }

  test("should fail properly if missing cartesian coordinates") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN point($params) as point",
      "A cartesian point must contain 'x' and 'y'",
      params = Map("params" -> Map("y" -> 1.0, "crs" -> "cartesian")))
  }

  test("should fail properly if missing geographic longitude") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN point($params) as point",
      "A wgs-84 point must contain 'latitude' and 'longitude'",
      params = Map("params" -> Map("latitude" -> 1.0, "crs" -> "WGS-84")))
  }

  test("should fail properly if missing geographic latitude") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN point($params) as point",
      "A wgs-84 point must contain 'latitude' and 'longitude'",
      params = Map("params" -> Map("longitude" -> 1.0, "crs" -> "WGS-84")))
  }

  test("should fail properly if unknown coordinate system") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN point($params) as point",
      "Unknown coordinate reference system: WGS-1337",
      params = Map("params" -> Map("x" -> 1, "y" -> 2, "crs" -> "WGS-1337")))
  }

  test("should default to Cartesian if missing cartesian CRS") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x: 2.3, y: 4.5}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.3, 4.5))))
  }

  test("point function with invalid coordinate types should give reasonable error") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined,
      "return point({x: 'apa', y: 0, crs: 'cartesian'})", "Cannot assign")
  }

  test("point function with invalid crs types should give reasonable error") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined,
      "return point({x: 0, y: 0, crs: 5})", "Cannot assign")
  }

  test("should default to WGS84 if missing geographic CRS") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({longitude: 2.3, latitude: 4.5}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 2.3, 4.5))))
  }

  test("should allow Geographic CRS with x/y coordinates") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x: 2.3, y: 4.5, crs: 'WGS-84'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 2.3, 4.5))))
  }

  test("should not allow Cartesian CRS with latitude/longitude coordinates") {
    failWithError(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({longitude: 2.3, latitude: 4.5, crs: 'cartesian'}) as point",
      "Geographic points does not support coordinate reference system: cartesian")
  }

  test("point function should work with previous map") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "WITH {latitude: 12.78, longitude: 56.7} as data RETURN point(data) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with node properties") {
    // Given
    createLabeledNode(Map("latitude" -> 12.78, "longitude" -> 56.7), "Place")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (p:Place) RETURN point({latitude: p.latitude, longitude: p.longitude}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with relationship properties") {
    // Given
    relate(createNode(), createNode(), "PASS_THROUGH", Map("latitude" -> 12.78, "longitude" -> 56.7))

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH ()-[r:PASS_THROUGH]->() RETURN point({latitude: r.latitude, longitude: r.longitude}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with node as map") {
    // Given
    createLabeledNode(Map("latitude" -> 12.78, "longitude" -> 56.7), "Place")

    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (p:Place) RETURN point(p) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with null input") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point(null) as p")
    result.toList should equal(List(Map("p" -> null)))
  }

  test("point function should return null if the map that backs it up contains a null") {
    var result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({latitude:null, longitude:3}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))

    result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({latitude:3, longitude:null}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))

    result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x:null, y:3}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))

    result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN point({x:3, y:null}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))
  }

  test("point function should fail on wrong type") {
    val config = Configs.All
    failWithError(config, "RETURN point(1) as dist", "Type mismatch: expected Map, Node or Relationship but was Integer")
  }

  test("point should be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val result = executeWith(Configs.InterpretedAndSlotted, "MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))
  }

  test("point should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.withTx( tx =>
      tx.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point").close()
    )

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    // Then
    result.toList.length should be(1)
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("3D point should be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val config = Configs.InterpretedAndSlotted
    val result = executeWith(config, "MATCH (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.2, 3.4, 5.6))))
  }

  test("3D point should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.withTx( tx =>
      tx.execute("MATCH (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point").close()
    )

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingVariables("point")))

    // Then
    result.toList.length should be(1)
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.2, 3.4, 5.6))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/sr-org/9157/")
  }

  test("inequality on cartesian points") {
    // case same point
    shouldCompareLike("point({x: 0, y: 0})", "point({x: 0, y: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = true)

    // case top right quadrant
    shouldCompareLike("point({x: 1, y: 1})", "point({x: 0, y: 0})", a_GT_b = true, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
    // case bottom left quadrant
    shouldCompareLike("point({x: -1, y: -1})", "point({x: 0, y: 0})", a_GT_b = false, a_LT_b = true, a_GTEQ_b = false, a_LTEQ_b = true)
    // case top left quadrant
    shouldCompareLike("point({x: -1, y: 1})", "point({x: 0, y: 0})", a_GT_b = null, a_LT_b = null, a_GTEQ_b = null, a_LTEQ_b = null)
    // case bottom right quadrant
    shouldCompareLike("point({x: 1, y: -1})", "point({x: 0, y: 0})", a_GT_b = null, a_LT_b = null, a_GTEQ_b = null, a_LTEQ_b = null)

    // case straight top
    shouldCompareLike("point({x: 0, y: 1})", "point({x: 0, y: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
    // case straight right
    shouldCompareLike("point({x: 1, y: 0})", "point({x: 0, y: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
    // case straight bottom
    shouldCompareLike("point({x: 0, y: -1})", "point({x: 0, y: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = false, a_LTEQ_b = true)
    // case straight left
    shouldCompareLike("point({x: -1, y: 0})", "point({x: 0, y: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = false, a_LTEQ_b = true)
  }

  test("inequality on geographic points") {
    // case same point
    shouldCompareLike("point({longitude: 0, latitude: 0})", "point({longitude: 0, latitude: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = true)

    // case top right quadrant
    shouldCompareLike("point({longitude: 1, latitude: 1})", "point({longitude: 0, latitude: 0})", a_GT_b = true, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
    // case bottom left quadrant
    shouldCompareLike("point({longitude: -1, latitude: -1})", "point({longitude: 0, latitude: 0})", a_GT_b = false, a_LT_b = true, a_GTEQ_b = false, a_LTEQ_b = true)
    // case top left quadrant
    shouldCompareLike("point({longitude: -1, latitude: 1})", "point({longitude: 0, latitude: 0})", a_GT_b = null, a_LT_b = null, a_GTEQ_b = null, a_LTEQ_b = null)
    // case bottom right quadrant
    shouldCompareLike("point({longitude: 1, latitude: -1})", "point({longitude: 0, latitude: 0})", a_GT_b = null, a_LT_b = null, a_GTEQ_b = null, a_LTEQ_b = null)

    // case straight top
    shouldCompareLike("point({longitude: 0, latitude: 1})", "point({longitude: 0, latitude: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
    // case straight right
    shouldCompareLike("point({longitude: 1, latitude: 0})", "point({longitude: 0, latitude: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
    // case straight bottom
    shouldCompareLike("point({longitude: 0, latitude: -1})", "point({longitude: 0, latitude: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = false, a_LTEQ_b = true)
    // case straight left
    shouldCompareLike("point({longitude: -1, latitude: 0})", "point({longitude: 0, latitude: 0})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = false, a_LTEQ_b = true)

    // the poles might be the same point, but in the effective projection onto 2D plane, they are not the same
    shouldCompareLike("point({longitude: -1, latitude: 90})", "point({longitude: 1, latitude: 90})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = false, a_LTEQ_b = true)
    shouldCompareLike("point({longitude: 1, latitude: 90})", "point({longitude: -1, latitude: 90})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
    shouldCompareLike("point({longitude: -1, latitude: -90})", "point({longitude: 1, latitude: -90})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = false, a_LTEQ_b = true)
    shouldCompareLike("point({longitude: 1, latitude: -90})", "point({longitude: -1, latitude: -90})", a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
  }

  test("inequality on 3D points") {
    Seq("cartesian-3D","WGS-84-3D").foreach { crsName =>
      (-1 to 1).foreach { x =>
        (-1 to 1).foreach { y =>
          (-1 to 1).foreach { z =>
            val same = Seq(x, y, z).forall(_ == 0)
            val lteq = Seq(x, y, z).forall(_ <= 0)
            val gteq = Seq(x, y, z).forall(_ >= 0)
            val onAxis = Seq(x, y, z).contains(0)
            val a = s"point({x: $x, y: $y, z: $z, crs: '$crsName'})"
            val b = s"point({x: 0, y: 0, z: 0, crs: '$crsName'})"
            if (same) {
              shouldCompareLike(a, b, a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = true)
            } else {
              if (onAxis) {
                if (lteq) {
                  shouldCompareLike(a, b, a_GT_b = false, a_LT_b = false, a_GTEQ_b = false, a_LTEQ_b = true)
                } else if (gteq) {
                  shouldCompareLike(a, b, a_GT_b = false, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
                } else {
                  shouldCompareLike(a, b, a_GT_b = null, a_LT_b = null, a_GTEQ_b = null, a_LTEQ_b = null)
                }
              } else {
                if (lteq) {
                  shouldCompareLike(a, b, a_GT_b = false, a_LT_b = true, a_GTEQ_b = false, a_LTEQ_b = true)
                } else if (gteq) {
                  shouldCompareLike(a, b, a_GT_b = true, a_LT_b = false, a_GTEQ_b = true, a_LTEQ_b = false)
                } else {
                  shouldCompareLike(a, b, a_GT_b = null, a_LT_b = null, a_GTEQ_b = null, a_LTEQ_b = null)
                }
              }
            }
          }
        }
      }
    }
  }

  test("inequality on mixed points") {
    shouldCompareLike("point({longitude: 0, latitude: 0})", "point({x: 0, y: 0})", a_GT_b = null, a_LT_b = null, a_GTEQ_b = null, a_LTEQ_b = null)
  }

  private def shouldCompareLike(a: String, b: String, a_GT_b: Any, a_LT_b: Any, a_GTEQ_b: Any, a_LTEQ_b: Any): Unit = {
    val query =
      s"""WITH $a as a, $b as b
         |RETURN a > b, a < b, a >= b, a <= b
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query).toList
    withClue(s"Comparing '$a' to '$b'") {
      result should equal(List(Map("a > b" -> a_GT_b, "a < b" -> a_LT_b, "a >= b" -> a_GTEQ_b, "a <= b" -> a_LTEQ_b)))
    }
  }

  test("array of points should be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val query =
      """
        |UNWIND [1,2,3] as num
        |WITH point({x: num, y: num}) as point
        |WITH collect(point) as points
        |MATCH (place:Place) SET place.location = points
        |RETURN points
      """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("point")))

    // Then
    result.toList should equal(List(Map("points" -> List(
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 3.0, 3.0)
    ))))
  }

  test("array of cartesian points should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.withTx( tx =>
      tx.execute(
        """
          |UNWIND [1,2,3] as num
          |WITH point({x: num, y: num}) as point
          |WITH collect(point) as points
          |MATCH (place:Place) SET place.location = points
          |RETURN place.location as points
      """.stripMargin).close()
    )

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as points",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("points")))

    // Then
    val points = result.columnAs("points").toList.head.asInstanceOf[Array[_]]
    points should equal(Array(
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 3.0, 3.0)
    ))
  }

  test("array of 3D cartesian points should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.withTx( tx =>
      tx.execute(
        """
          |UNWIND [1,2,3] as num
          |WITH point({x: num, y: num, z: num}) as point
          |WITH collect(point) as points
          |MATCH (place:Place) SET place.location = points
          |RETURN place.location as points
        """.stripMargin).close()
    )

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as points",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("points")))

    // Then
    val points = result.columnAs("points").toList.head.asInstanceOf[Array[_]]
    points should equal(Array(
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 2.0, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 3.0, 3.0, 3.0)
    ))
  }

  test("array of 3D WGS84 points should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.withTx( tx =>
      tx.execute(
        """
          |UNWIND [1,2,3] as num
          |WITH point({longitude: num, latitude: num, height: num}) as point
          |WITH collect(point) as points
          |MATCH (place:Place) SET place.location = points
          |RETURN place.location as points
        """.stripMargin).close()
    )

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as points",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("points")))

    // Then
    val points = result.columnAs("points").toList.head.asInstanceOf[Array[_]]
    points should equal(Array(
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 1.0, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 2.0, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 3.0, 3.0, 3.0)
    ))
  }

  test("array of wgs84 points should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.withTx( tx =>
      tx.execute(
        """
          |UNWIND [1,2,3] as num
          |WITH point({latitude: num, longitude: num}) as point
          |WITH collect(point) as points
          |MATCH (place:Place) SET place.location = points
          |RETURN place.location as points
        """.stripMargin).close()
    )

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as points",
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("Projection").containingArgumentForProjection("points")))

    // Then
    val points = result.columnAs("points").toList.head.asInstanceOf[Array[_]]
    points should equal(Array(
      Values.pointValue(CoordinateReferenceSystem.WGS84, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.WGS84, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.WGS84, 3.0, 3.0)
    ))
  }

  test("array of mixed points should not be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val query =
      """
        |WITH [point({x: 1, y: 2}), point({latitude: 1, longitude: 2})] as points
        |MATCH (place:Place) SET place.location = points
        |RETURN points
      """.stripMargin

    // Then
    failWithError(Configs.InterpretedAndSlotted, query, "Collections containing point values with different CRS can not be stored in properties.")
  }

  test("accessors on 2D cartesian points") {
    // given
    graph.withTx( tx => tx.execute("CREATE (:P {p : point({x: 1, y: 2})})"))

    // when
    val result = executeWith(Configs.All, "MATCH (n:P) WITH n.p AS p RETURN p.x, p.y, p.crs, p.srid")

    // then
    result.toList should be(List(Map("p.x" -> 1.0, "p.y" -> 2.0, "p.crs" -> "cartesian", "p.srid" -> 7203)))
    failWithError(Configs.All, "MATCH (n:P) WITH n.p AS p RETURN p.latitude", "Field: latitude is not available")
    failWithError(Configs.All, "MATCH (n:P) WITH n.p AS p RETURN p.z", "Field: z is not available")
  }

  test("accessors on 3D cartesian points") {
    // given
    graph.withTx( tx => tx.execute("CREATE (:P {p : point({x: 1, y: 2, z:3})})"))

    // when
    val result = executeWith(Configs.All, "MATCH (n:P) WITH n.p AS p RETURN p.x, p.y, p.z, p.crs, p.srid")

    // then
    result.toList should be(List(Map("p.x" -> 1.0, "p.y" -> 2.0, "p.z" -> 3.0, "p.crs" -> "cartesian-3d", "p.srid" -> 9157)))
    failWithError(Configs.All, "MATCH (n:P) WITH n.p AS p RETURN p.latitude", "Field: latitude is not available")
  }

  test("accessors on 2D geographic points") {
    // given
    graph.withTx( tx => tx.execute("CREATE (:P {p : point({longitude: 1, latitude: 2})})"))

    // when
    val result = executeWith(Configs.All,  "MATCH (n:P) WITH n.p AS p RETURN p.longitude, p.latitude, p.crs, p.x, p.y, p.srid")

    // then
    result.toList should be(List(Map("p.longitude" -> 1.0, "p.latitude" -> 2.0, "p.crs" -> "wgs-84", "p.x" -> 1.0, "p.y" -> 2.0, "p.srid" -> 4326)))
    failWithError(Configs.All, "MATCH (n:P) WITH n.p AS p RETURN p.height", "Field: height is not available")
    failWithError(Configs.All, "MATCH (n:P) WITH n.p AS p RETURN p.z", "Field: z is not available")
  }

  test("accessors on 3D geographic points") {
    // given
    graph.withTx( tx => tx.execute("CREATE (:P {p : point({longitude: 1, latitude: 2, height:3})})"))

    // when
    val result = executeWith(Configs.All,
      "MATCH (n:P) WITH n.p AS p RETURN p.longitude, p.latitude, p.height, p.crs, p.x, p.y, p.z, p.srid")

    // then
    result.toList should be(List(Map("p.longitude" -> 1.0, "p.latitude" -> 2.0, "p.height" -> 3.0, "p.crs" -> "wgs-84-3d", "p.srid" -> 4979,
      "p.x" -> 1.0, "p.y" -> 2.0, "p.z" -> 3.0)))
  }
}
