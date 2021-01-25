/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite

class MergeIntoPlanningAcceptanceTest extends ExecutionEngineFunSuite{

  test("ON CREATE with update one property") {
    //given
    createNode("A")
    createNode("B")

    //when
    val update = execute("""MATCH (a {name:'A'}), (b {name:'B'})
                           |MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = 'foo'""".stripMargin)

    //then
    update.executionPlanDescription() should includeSomewhere.aPlan("Expand(Into)")
  }

  test("ON CREATE with deleting one property") {
    //given
    createNode("A")
    createNode("B")

    //when
    val update = execute("""MATCH (a {name:'A'}), (b {name:'B'})
                           |MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = null""".stripMargin)

    //then
    update.executionPlanDescription() should includeSomewhere.aPlan("Expand(Into)")
  }

  test("ON CREATE with update all properties from node") {
    //given
    createNode("A")
    createNode("B")

    //when
    val update = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r = a")

    //then
    update.executionPlanDescription() should includeSomewhere.aPlan("Expand(Into)")
  }

  test("ON MATCH with update all properties from node") {
    //note the props here should be overwritten with ON MATCH
    relate(createNode("A"), createNode("B"), "TYPE", Map("foo" -> "bar"))

    //when
    val update = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON MATCH SET r = a")

    //then
    update.executionPlanDescription() should includeSomewhere.aPlan("Expand(Into)")
  }

  test("ON CREATE with update properties from literal map") {
    //given
    createNode("A")
    createNode("B")

    //when
    val update = execute("""MATCH (a {name:'A'}), (b {name:'B'})
                           |MERGE (a)-[r:TYPE]->(b) ON CREATE SET r += {foo: 'bar', bar: 'baz'}""".stripMargin)

    //then
    update.executionPlanDescription() should includeSomewhere.aPlan("Expand(Into)")
  }

  test("ON MATCH with update properties from literal map") {
    //given
    relate(createNode("A"), createNode("B"), "TYPE", Map("foo" -> "bar"))

    //when
    val update = execute("""MATCH (a {name:'A'}), (b {name:'B'})
                           |MERGE (a)-[r:TYPE]->(b) ON MATCH SET r += {foo: 'baz', bar: 'baz'}""".stripMargin)

    //then
    update.executionPlanDescription() should includeSomewhere.aPlan("Expand(Into)")
  }
}
