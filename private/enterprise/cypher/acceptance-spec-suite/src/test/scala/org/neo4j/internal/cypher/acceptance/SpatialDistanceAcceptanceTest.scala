/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.TestConfiguration
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.Values

class SpatialDistanceAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("distance function should work on co-located points") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "WITH point({latitude: 12.78, longitude: 56.7}) as point RETURN distance(point,point) as dist",
      planComparisonStrategy = ComparePlansWithAssertion(_ should
        includeSomewhere.aPlan("Projection").containingArgumentForProjection("dist")
          .onTopOf(includeSomewhere.aPlan("Projection").containingArgumentForProjection("point"))))

    result.toList should equal(List(Map("dist" -> 0.0)))
  }

  test("distance function should work on co-located points in 3D") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "WITH point({latitude: 12.78, longitude: 56.7, height: 198.2}) as point RETURN distance(point,point) as dist",
      planComparisonStrategy = ComparePlansWithAssertion(_ should
        includeSomewhere.aPlan("Projection").containingArgumentForProjection("dist")
          .onTopOf(includeSomewhere.aPlan("Projection").containingArgumentForProjection("point"))))

    result.toList should equal(List(Map("dist" -> 0.0)))
  }

  test("distance function should work on nearby cartesian points") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({x: 2.3, y: 4.5, crs: 'cartesian'}) as p1, point({x: 1.1, y: 5.4, crs: 'cartesian'}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should
        includeSomewhere.aPlan("Projection").containingArgumentForProjection("dist")
          .onTopOf(includeSomewhere.aPlan("Projection").containingArgumentForProjection("p1", "p2"))))

    result.columnAs("dist").next().asInstanceOf[Double] should equal(1.5)
  }

  test("distance function should work on nearby points") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({longitude: 12.78, latitude: 56.7}) as p1, point({latitude: 56.71, longitude: 12.79}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should
        includeSomewhere.aPlan("Projection").containingArgumentForProjection("dist")
          .onTopOf(includeSomewhere.aPlan("Projection").containingArgumentForProjection("p1", "p2"))))

    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(1270)
  }

  test("distance function should work on nearby points in 3D") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({longitude: 12.78, latitude: 56.7, height: 100}) as p1, point({latitude: 56.71, longitude: 12.79, height: 100}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should
        includeSomewhere.aPlan("Projection").containingArgumentForProjection("dist")
          .onTopOf(includeSomewhere.aPlan("Projection").containingArgumentForProjection("p1", "p2"))))

    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(1270)
  }

  test("distance function should work on distant points") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({latitude: 56.7, longitude: 12.78}) as p1, point({longitude: -51.9, latitude: -16.7}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should
        includeSomewhere.aPlan("Projection").containingArgumentForProjection("dist")
          .onTopOf(includeSomewhere.aPlan("Projection").containingArgumentForProjection("p1", "p2"))))

    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(10116214)
  }

  test("distance function should work on distant points in 3D") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({latitude: 56.7, longitude: 12.78, height: 100}) as p1, point({longitude: -51.9, latitude: -16.7, height: 100}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should
        includeSomewhere.aPlan("Projection").containingArgumentForProjection("dist")
          .onTopOf(includeSomewhere.aPlan("Projection").containingArgumentForProjection("p1", "p2"))))

    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(10116373)
  }

  test("distance function should work on 3D cartesian points") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({x: 1.2, y: 3.4, z: 5.6}) as p1, point({x: 1.2, y: 3.4, z: 6.6}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should
        includeSomewhere.aPlan("Projection").containingArgumentForProjection("dist")
          .onTopOf(includeSomewhere.aPlan("Projection").containingArgumentForProjection("p1", "p2"))))

    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(1)
  }

  test("distance function should not fail if provided with points from different CRS") {
    val localConfig = Configs.InterpretedAndSlottedAndPipelined
    val res = executeWith(localConfig,
      """WITH point({x: 2.3, y: 4.5, crs: 'cartesian'}) as p1, point({longitude: 1.1, latitude: 5.4, crs: 'WGS-84'}) as p2
        |RETURN distance(p1,p2) as dist""".stripMargin)
    res.columnAs[AnyRef]("dist").next() should be (null)
  }

  test("distance function should return null if provided with points with different dimensions") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """WITH point({x: 2.3, y: 4.5}) as p1, point({x: 1.2, y: 3.4, z: 5.6}) as p2
        |RETURN distance(p1,p2) as dist""".stripMargin)
    val dist = result.columnAs[Any]("dist").next()
    assert(dist == null)
  }

  test("distance function should measure distance from Copenhagen train station to Neo4j in Malmö") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({latitude: 55.672874, longitude: 12.564590}) as p1, point({latitude: 55.611784, longitude: 12.994341}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should
        includeSomewhere.aPlan("Projection").containingArgumentForProjection("dist")
          .onTopOf(includeSomewhere.aPlan("Projection").containingArgumentForProjection("p1", "p2"))))

    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(27842)
  }

  test("distance function should work with two null inputs") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "RETURN distance(null, null) as dist")
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should return null with lhs null input") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({latitude: 55.672874, longitude: 12.564590}) as p1
        |RETURN distance(null, p1) as dist
      """.stripMargin)
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should return null with rhs null input") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({latitude: 55.672874, longitude: 12.564590}) as p1
        |RETURN distance(p1, null) as dist
      """.stripMargin)
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should return null if a point is null") {
    var result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "RETURN distance(point({latitude:3,longitude:7}),point({latitude:null, longitude:3})) as dist;")
    result.toList should equal(List(Map("dist" -> null)))

    result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "RETURN distance(point({latitude:3,longitude:null}),point({latitude:7, longitude:3})) as dist;")
    result.toList should equal(List(Map("dist" -> null)))

    result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "RETURN distance(point({x:3,y:7}),point({x:null, y:3})) as dist;")
    result.toList should equal(List(Map("dist" -> null)))

    result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      "RETURN distance(point({x:3,y:null}),point({x:7, y:3})) as dist;")
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should work for points with different aliases") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({latitude: 12, longitude: 55.1, srid: 4326}) as p1
        |RETURN distance(point({x:55, y:12, srid: 4326}), p1) as dist
      """.stripMargin)
    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(10889)
  }

  test("distance function should work for points with and without explicit srid") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({latitude: 12, longitude: 55.1, srid: 4326}) as p1
        |RETURN distance(point({latitude: 12, longitude: 55}), p1) as dist
      """.stripMargin)
    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(10889)
  }

  test("distance function should work for points with and without explicit crs") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({x: 0, y: 0}) as p1
        |RETURN distance(point({x: 3, y: 4, crs:'cartesian'}), p1) as dist
      """.stripMargin)
    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(5)
  }

  test("distance function should work for points in same coordinate system") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
        |WITH point({latitude: 12, longitude: 55.1, srid: 4326}) as p1
        |RETURN distance(point({latitude: 12, longitude: 55, crs: 'WGS-84'}), p1) as dist
      """.stripMargin)
    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(10889)
  }

  test("points with distance query and mixed crs") {
    // Given
    executeSingle("CREATE (p:Place) SET p.location = point({y: 56.7, x: 12.78, crs: 'cartesian'})")
    executeSingle("CREATE (p:Place) SET p.location = point({y: 55.7, x: 11.78, crs: 'cartesian'})")
    executeSingle("CREATE (p:Place) SET p.location = point({y: 50.7, x: 12.78, crs: 'cartesian'})")
    executeSingle("CREATE (p:Place) SET p.location = point({y: 56.7, x: 10.78, crs: 'cartesian'})")
    executeSingle("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    executeSingle("CREATE (p:Place) SET p.location = point({latitude: 55.7, longitude: 11.78, crs: 'WGS-84'})")
    executeSingle("CREATE (p:Place) SET p.location = point({latitude: 50.7, longitude: 12.78, crs: 'WGS-84'})")
    executeSingle("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 10.78, crs: 'WGS-84'})")
    executeSingle("CREATE (p:Place) SET p.location = point({y: 56.7, x: 12.78, z: 100.0})")
    executeSingle("CREATE (p:Place) SET p.location = point({y: 55.7, x: 11.78, z: 100.0})")
    executeSingle("CREATE (p:Place) SET p.location = point({y: 50.7, x: 12.78, z: 100.0})")
    executeSingle("CREATE (p:Place) SET p.location = point({y: 56.7, x: 10.78, z: 100.0})")
    executeSingle("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, height: 100.0})")
    executeSingle("CREATE (p:Place) SET p.location = point({latitude: 55.7, longitude: 11.78, height: 100.0})")
    executeSingle("CREATE (p:Place) SET p.location = point({latitude: 50.7, longitude: 12.78, height: 100.0})")
    executeSingle("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 10.78, height: 100.0})")

    Set(CoordinateReferenceSystem.WGS84, CoordinateReferenceSystem.Cartesian,
      CoordinateReferenceSystem.WGS84_3D, CoordinateReferenceSystem.Cartesian_3D).foreach { crs =>
      val zText = if (crs.getDimension == 3) ", z: 100.0" else ""
      val point = if (crs.isGeographic) s"point({latitude: 55.7, longitude: 11.78$zText})" else s"point({y: 55.7, x: 11.78$zText})"
      val distance = if (crs.isGeographic) 1000 else 1
      val expected = if (crs.getDimension == 3) Values.pointValue(crs, 11.78, 55.7, 100) else Values.pointValue(crs, 11.78, 55.7)
      // When
      val query =
        s"""MATCH (p:Place)
           |WHERE distance(p.location, $point) < $distance
           |RETURN p.location as point
        """.stripMargin
      val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

      // Then
      result.toList should equal(List(Map("point" -> expected)))
    }
  }

  test("indexed points with distance query and points within bbox") {
    // Given
    graph.createIndex("Place", "location")
    setupPointsBothCRS()

    // <= cartesian
    {
      val query =
        s"""MATCH (p:Place)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= 10
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, -10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, -10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 9.99))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
    }
    // < cartesian
    {
      val query =
        s"""MATCH (p:Place)
           |WHERE distance(p.location, point({y: 0, x: 0, crs: 'cartesian'})) < 10
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 9.99))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = false)
    }
    // <= geographic
    {
      val query =
        s"""WITH distance(point({latitude: 0, longitude: 0, crs: 'WGS-84'}), point({latitude: 10, longitude: 0, crs: 'WGS-84'})) as d
           |MATCH (p:Place)
           |WHERE distance(p.location, point({latitude: 0, longitude: 0, crs: 'WGS-84'})) <= d
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, -10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 0, -10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 0))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
    }
    // < geographic
    {
      val query =
        s"""WITH distance(point({latitude: 0, longitude: 0, crs: 'WGS-84'}), point({latitude: 0, longitude: 10, crs: 'WGS-84'})) as d
           |MATCH (p:Place)
           |WHERE distance(p.location, point({latitude: 0, longitude: 0, crs: 'WGS-84'})) < d
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 0))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = false)
    }
  }

  test("should use index for distance query of points with maxDistance in horizon") {
    // Given
    graph.createIndex("Place", "location")
    setupPointsBothCRS()

    // <= cartesian
    {
      val query =
        s"""WITH 10 AS maxDistance
           |MATCH (p:Place)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, -10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, -10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 9.99))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
    }
  }

  test("should not use index for distance query of points with maxDistance in horizon") {
    // Given
    graph.createIndex("Place", "location")
    setupPointsBothCRS()

    executeSingle(
      """MATCH (p:Place) CREATE (:Preference {maxDistance: 10})<-[:R]-(p),
        |                       (:Preference {maxDistance: 10})<-[:R]-(p),
        |                       (:Preference {maxDistance: 10})<-[:R]-(p)""".stripMargin)

    // <= cartesian
    {
      val query =
        s"""MATCH (p:Place)-->(x:Preference)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= x.maxDistance
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, -10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, -10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 9.99))
      )
      val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
      result.executionPlanDescription() shouldNot includeSomewhere.aPlan("NodeIndexSeekByRange").containingArgumentRegex(".*distance.*".r)
      result.toList.toSet should equal(expected)
    }
  }

  test("indexed points at date line") {
    // Given
    graph.createIndex("Place", "location")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 0, longitude: -180})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 0, longitude: 180})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 0, longitude: -170})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 0, longitude: 170})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 10, longitude: -180})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 10, longitude: 180})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 10, longitude: -170})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 10, longitude: 170})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: -10, longitude: -180})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: -10, longitude: 180})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: -10, longitude: -170})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: -10, longitude: 170})")

    // Create enough points so that an index seek gets planned
    Range(0, 50).foreach(i => executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: $i, longitude: $i})"))

    // Have a slightly bigger circle, and expect points on both sides of the date line, except the "corners" of the square.
    Seq("<=","<").foreach { inequality =>
      withClue(s"When using distance $inequality d\n") {
        val query =
          s"""WITH distance(point({latitude: 0, longitude: 180, crs: 'WGS-84'}), point({latitude: 0, longitude: 169, crs: 'WGS-84'})) as d
             |MATCH (p:Place)
             |WHERE distance(p.location, point({latitude: 0, longitude: 180, crs: 'WGS-84'})) $inequality d
             |RETURN p.location as point
        """.stripMargin

        // Then
        val expected = Set(
          Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, -180, 0)),
          Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 180, 0)),
          Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, -170, 0)),
          Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 170, 0)),
          Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, -180, 10)),
          Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 180, 10)),
          Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, -180, -10)),
          Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 180, -10))
        )
        expectResultsAndIndexUsage(query, expected, inclusiveRange = inequality.contains("="))
      }
    }
  }

  test("indexed 3D points with distance query and points within bbox") {
    // Given
    graph.createIndex("Place", "location")
    setupPointsBothCRS(Seq(-10, 0, 10))

    // <= cartesian
    {
      val query =
        s"""MATCH (p:Place)
           |WHERE distance(p.location, point({x: 0, y: 0, z: 0})) <= 10
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 0, -10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 9.99, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 10, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, -10, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, -10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 0, 10))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
    }
    // < cartesian
    {
      val query =
        s"""MATCH (p:Place)
           |WHERE distance(p.location, point({y: 0, x: 0, z: 0})) < 10
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 9.99, 0))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = false)
    }
    // <= geographic
    {
      val query =
        s"""WITH distance(point({latitude: 0, longitude: 0, height: 0}), point({latitude: 10, longitude: 0, height: 0})) as d
           |MATCH (p:Place)
           |WHERE distance(p.location, point({latitude: 0, longitude: 0, height: 0})) <= d
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 10, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, -10, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, -10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 0, -1000000)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 0, 1000000))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
    }
    // < geographic
    {
      val query =
        s"""WITH distance(point({latitude: 0, longitude: 0, height: 0}), point({latitude: 0, longitude: 10, height: 0})) as d
           |MATCH (p:Place)
           |WHERE distance(p.location, point({latitude: 0, longitude: 0, height: 0})) < d
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 0, -1000000)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 0, 1000000))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = false)
    }
  }

  test("doughnut shape query uses the index") {
    // Given
    graph.createIndex("Place", "location")
    setupPointsBothCRS()

    // <= cartesian
    {
      val query =
        s"""MATCH (p:Place)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= 10 and distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) > 5
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, -10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, -10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 9.99))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
    }
    // < cartesian
    {
      val query =
        s"""MATCH (p:Place)
           |WHERE distance(p.location, point({y: 0, x: 0, crs: 'cartesian'})) < 10 and distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) > 5
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 9.99))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = false)
    }
    // <= geographic
    {
      val query =
        s"""WITH distance(point({latitude: 0, longitude: 0, crs: 'WGS-84'}), point({latitude: 10, longitude: 0, crs: 'WGS-84'})) as d
           |MATCH (p:Place)
           |WHERE distance(p.location, point({latitude: 0, longitude: 0, crs: 'WGS-84'})) <= d and distance(p.location, point({latitude: 0, longitude: 0, crs: 'WGS-84'})) > d / 2
           |RETURN p.location as point
            """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 10)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, -10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 0, -10))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
    }
    // < geographic
    {
      val query =
        s"""WITH distance(point({latitude: 0, longitude: 0, crs: 'WGS-84'}), point({latitude: 0, longitude: 10, crs: 'WGS-84'})) as d
           |MATCH (p:Place)
           |WHERE distance(p.location, point({latitude: 0, longitude: 0, crs: 'WGS-84'})) < d and distance(p.location, point({latitude: 0, longitude: 0, crs: 'WGS-84'})) > d / 2
           |RETURN p.location as point
            """.stripMargin

      // Then
      val expected = Set.empty
      expectResultsAndIndexUsage(query, expected, inclusiveRange = false)
    }
  }

  test("doughnut shape query uses the index in 3D") {
    // Given
    graph.createIndex("Place", "location")
    setupPointsBothCRS(Seq(0))

    // <= cartesian
    {
      val query =
        s"""MATCH (p:Place)
           |WHERE distance(p.location, point({x: 0, y: 0, z: 0})) <= 10 and distance(p.location, point({x: 0, y: 0, z: 0})) > 5
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 10, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, -10, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, -10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 9.99, 0))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
    }
    // < cartesian
    {
      val query =
        s"""MATCH (p:Place)
           |WHERE distance(p.location, point({y: 0, x: 0, z: 0})) < 10 and distance(p.location, point({x: 0, y: 0, z: 0})) > 5
           |RETURN p.location as point
        """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 9.99, 0))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = false)
    }
    // <= geographic
    {
      val query =
        s"""WITH distance(point({latitude: 0, longitude: 0, height: 0}), point({latitude: 10, longitude: 0, height: 0})) as d
           |MATCH (p:Place)
           |WHERE distance(p.location, point({latitude: 0, longitude: 0, height: 0})) <= d and distance(p.location, point({latitude: 0, longitude: 0, height: 0})) > d / 2
           |RETURN p.location as point
            """.stripMargin

      // Then
      val expected = Set(
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 10, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, 10, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, -10, 0, 0)),
        Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 0, -10, 0))
      )
      expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
    }
    // < geographic
    {
      val query =
        s"""WITH distance(point({latitude: 0, longitude: 0, height: 0}), point({latitude: 0, longitude: 10, height: 0})) as d
           |MATCH (p:Place)
           |WHERE distance(p.location, point({latitude: 0, longitude: 0, height: 0})) < d and distance(p.location, point({latitude: 0, longitude: 0, height: 0})) > d / 2
           |RETURN p.location as point
            """.stripMargin

      // Then
      val expected = Set.empty
      expectResultsAndIndexUsage(query, expected, inclusiveRange = false)
    }
  }

  test("should use unique index for cartesian distance query") {
    // Given
    graph.createUniqueIndex("Place", "location")

    // Create 1000 unique nodes
    for (i <- 0 to 999) {
      val y = 34 + i * 0.001
      createLabeledNode(Map("location" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 105, y)), "Place")
    }

    // When
    val query =
      """
        |MATCH (p:Place)
        |WHERE distance(p.location, point({crs: 'cartesian', x: 105, y: 34 })) < 0.1
        |RETURN count(p)
      """.stripMargin

    // Then
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("Filter").containingArgumentRegex("distance.*".r)
    plan should includeSomewhere.aPlan("NodeUniqueIndexSeekByRange")
      .containingArgumentRegex(("UNIQUE p:Place\\(location\\) WHERE distance\\(.+?\\) " +  "< " + ".*").r)

    result.toList should equal(List(Map("count(p)" -> 100)))
  }

  ignore("projecting distance into variable still uses index") {
    // Given
    graph.createIndex("Place", "location")
    setupPointsBothCRS()

    val query =
      s"""MATCH (p:Place)
         |WITH distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) as d, p
         |WHERE d <= 10 and d > 5
         |RETURN p.location as point
        """.stripMargin

    // Then
    val expected = Set(
      Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 10, 0)),
      Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 10)),
      Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, -10, 0)),
      Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, -10)),
      Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 0)),
      Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 9.99))
    )
    expectResultsAndIndexUsage(query, expected, inclusiveRange = true)
  }

  test("invalid location with index") {
    // Given
    graph.createIndex("Place", "location")
    executeSingle("CREATE (p:Place) SET p.location = 5")
    Range(11, 100).foreach(i => executeSingle(s"CREATE (p:Place) SET p.location = point({y: $i, x: $i, crs: 'cartesian'})"))

    val query =
      s"""MATCH (p:Place)
         |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= 10
         |RETURN p.location as point
        """.stripMargin
    // When & Then
    expectResultsAndIndexUsage(query, Set.empty, inclusiveRange = true)
  }

  test("invalid location without index") {
    // Given
    executeSingle("CREATE (p:Place) SET p.location = 5")
    Range(11, 100).foreach(i => executeSingle(s"CREATE (p:Place) SET p.location = point({y: $i, x: $i, crs: 'cartesian'})"))

    val query =
      s"""MATCH (p:Place)
         |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= 10
         |RETURN p.location as point
        """.stripMargin
    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    // Then
    val plan = result.executionPlanDescription()
    plan should includeSomewhere
      .aPlan("Projection").containingArgumentForProjection("point")
      .onTopOf(aPlan("Filter").containingArgumentRegex("distance.*".r)
        .onTopOf(includeSomewhere.aPlan("NodeByLabelScan").containingArgument("p:Place")))
    result.toList.toSet should equal(Set.empty)
  }

  test("no error for distance with no point when using parameters") {
    // Given
    graph.createIndex("Place", "location")
    executeSingle("CREATE (p:Place) SET p.location = point({y: 0, x: 0, crs: 'cartesian'})")
    Range(11, 100).foreach(i => executeSingle(s"CREATE (p:Place) SET p.location = point({y: $i, x: $i, crs: 'cartesian'})"))

    val query =
      """MATCH (p:Place)
        |WHERE distance(p.location, $poi) <= 10
        |RETURN p.location as point
        """.stripMargin
    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, params = Map("poi" -> 5))

    // Then
    result.toList shouldBe empty

    // And given
    executeSingle(s"DROP INDEX ON :Place(location)")
    // when
    val resultNoIndex = executeWith(Configs.InterpretedAndSlottedAndPipelined, query,  params = Map("poi" -> 5))

    // Then
    resultNoIndex.toList shouldBe empty
  }

  test("no error for distance with no point when using no parameters") {
    // Given
    graph.createIndex("Place", "location")
    executeSingle("CREATE (p:Place) SET p.location = point({y: 0, x: 0, crs: 'cartesian'})")
    Range(11, 100).foreach(i => executeSingle(s"CREATE (p:Place) SET p.location = point({y: $i, x: $i, crs: 'cartesian'})"))

    val query =
      """MATCH (p:Place)
        |WHERE distance(p.location, 5) <= 10
        |RETURN p.location as point
      """.stripMargin
    // When
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    // Then
    result.toList shouldBe empty

    // And given
    executeSingle(s"DROP INDEX ON :Place(location)")
    // when
    val resultNoIndex = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    // Then
    resultNoIndex.toList shouldBe empty
  }

  private def setupPointsCartesian(zText: String = ""): Unit = {
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: -10, x: -10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: -10, x: 10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: 10, x: -10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: 10, x: 10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: -10, x: 0$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: 10, x: 0$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: 0, x: -10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: 0, x: 10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: 0, x: 0$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({y: 9.99, x: 0$zText})")

    // Create enough points so that an index seek gets planned
    Range(11, 100).foreach(i => executeSingle(s"CREATE (p:Place) SET p.location = point({y: $i, x: $i$zText})"))
  }

  private def setupPointsWGS84(zText: String = ""): Unit = {
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: -10, longitude: -10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: -10, longitude: 10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 10, longitude: -10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 10, longitude: 10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: -10, longitude: 0$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 10, longitude: 0$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 0, longitude: -10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 0, longitude: 10$zText})")
    executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: 0, longitude: 0$zText})")

    // Create enough points so that an index seek gets planned
    Range(11, 89).foreach(i => executeSingle(s"CREATE (p:Place) SET p.location = point({latitude: $i, longitude: $i$zText})"))
  }

  private def setupPointsBothCRS(): Unit = {
    setupPointsCartesian()
    setupPointsWGS84()
  }

  private def setupPointsBothCRS(zSet: Seq[Int]): Unit = {
    zSet.foreach { z =>
      setupPointsCartesian(s", z: $z")
      setupPointsWGS84(s", z: ${z}00000")
    }
  }

  private def expectResultsAndIndexUsage(query: String, expectedResults: Set[_ <: Any], inclusiveRange: Boolean,
                                         config: TestConfiguration = Configs.InterpretedAndSlottedAndPipelined): Unit = {
    val result = executeWith(config, query)

    // Then
    val plan = result.executionPlanDescription()
    plan should includeSomewhere
      .aPlan("Projection").containingArgumentForProjection("point")
      .onTopOf(aPlan("Filter").containingArgumentRegex("distance.*".r)
        .onTopOf(includeSomewhere.aPlan("NodeIndexSeekByRange")
          .containingArgumentRegex(("p:Place\\(location\\) WHERE distance\\(.+?\\) " + (if (inclusiveRange) "<= " else "< ") + ".*").r)))
    result.toList.toSet should equal(expectedResults)
  }
}
