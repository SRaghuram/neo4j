/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Boolean.FALSE

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.javacompat.DeprecationAcceptanceTest.ChangedResults
import org.neo4j.graphdb
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.impl.notification.NotificationCode
import org.neo4j.graphdb.impl.notification.NotificationCode.CARTESIAN_PRODUCT
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_COMPILED_RUNTIME
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE
import org.neo4j.graphdb.impl.notification.NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD
import org.neo4j.graphdb.impl.notification.NotificationCode.INDEX_HINT_UNFULFILLABLE
import org.neo4j.graphdb.impl.notification.NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY
import org.neo4j.graphdb.impl.notification.NotificationCode.MISSING_LABEL
import org.neo4j.graphdb.impl.notification.NotificationCode.MISSING_PROPERTY_NAME
import org.neo4j.graphdb.impl.notification.NotificationCode.MISSING_REL_TYPE
import org.neo4j.graphdb.impl.notification.NotificationCode.REPEATED_REL_IN_PATTERN_EXPRESSION
import org.neo4j.graphdb.impl.notification.NotificationCode.RUNTIME_UNSUPPORTED
import org.neo4j.graphdb.impl.notification.NotificationCode.SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY
import org.neo4j.graphdb.impl.notification.NotificationCode.SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY
import org.neo4j.graphdb.impl.notification.NotificationCode.UNBOUNDED_SHORTEST_PATH
import org.neo4j.graphdb.impl.notification.NotificationDetail
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.bindingVarLengthRelationship
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.cartesianProduct
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.deprecatedField
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.deprecatedName
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.index
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.indexSeekOrScan
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.label
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.propertyName
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.relationshipType
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.suboptimalIndex
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.procedure.Procedure

import scala.collection.JavaConverters.setAsJavaSetConverter

class NotificationAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Need to override so that graph.execute will not throw an exception
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_hints_error -> FALSE,
    GraphDatabaseSettings.query_non_indexed_label_warning_threshold -> java.lang.Long.valueOf(10)
  )

  override def initTest(): Unit = {
    super.initTest()
    val procedures = this.graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures])
    procedures.registerProcedure(classOf[NotificationAcceptanceTest.TestProcedures])
  }

  test("Warn on binding variable length relationships") {
    val res1 = executeSingle("explain MATCH ()-[rs*]-() RETURN rs", Map.empty)

    res1.notifications should contain(
      DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP.notification(new graphdb.InputPosition(16, 1, 17),
        bindingVarLengthRelationship("rs")))

    val res2 = executeSingle("explain MATCH p = ()-[*]-() RETURN relationships(p) AS rs", Map.empty)

    res2.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.FeatureDeprecationWarning."
  }

  test("Warn on deprecated standalone procedure calls") {
    val result = executeSingle("explain CALL oldProc()", Map.empty)

    result.notifications.toList should equal(
      List(
        DEPRECATED_PROCEDURE.notification(new graphdb.InputPosition(8, 1, 9), deprecatedName("oldProc", "newProc"))))
  }

  test("Warn on deprecated in-query procedure calls") {
    val result = executeSingle("explain CALL oldProc() RETURN 1", Map.empty)

    result.notifications.toList should equal(
      List(DEPRECATED_PROCEDURE.notification(new graphdb.InputPosition(8, 1, 9), deprecatedName("oldProc", "newProc"))))
  }

  test("Warn on deprecated procedure result field") {
    val result = executeSingle("explain CALL changedProc() YIELD oldField RETURN oldField", Map.empty)

    result.notifications.toList should equal(
      List(
        DEPRECATED_PROCEDURE_RETURN_FIELD.notification(new graphdb.InputPosition(33, 1, 34),
          deprecatedField("changedProc", "oldField"))))
  }

  test("Warn for cartesian product") {
    val result = executeSingle("explain match (a)-->(b), (c)-->(d) return *", Map.empty)

    result.notifications.toList should equal(List(
      CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(8, 1, 9), cartesianProduct(Set("c", "d").asJava))))
  }

  test("Warn for cartesian product when running 3.5") {
    val result = executeSingle("explain cypher 3.5 match (a)-->(b), (c)-->(d) return *", Map.empty)

    result.notifications.toList should equal(List(
      CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(19, 1, 20), cartesianProduct(Set("c", "d").asJava))))
  }

  test("Warn for cartesian product with runtime=legacy_compiled") {
    val result = executeSingle("explain cypher runtime=legacy_compiled match (a)-->(b), (c)-->(d) return count(*)", Map.empty)

    result.notifications.toList should equal(List(
      DEPRECATED_COMPILED_RUNTIME.notification(graphdb.InputPosition.empty),
      CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(39, 1, 40), cartesianProduct(Set("c", "d").asJava)),
      RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty, Factory.message("Runtime unsupported", "CountStar() is not supported"))))
  }

  test("Warn unsupported runtime with explain and runtime=legacy_compiled") {
    val result = executeSingle(
      """explain cypher runtime=legacy_compiled
         RETURN reduce(y=0, x IN [0] | x) AS z""", Map.empty)

    result.notifications.toList should equal(List(
      DEPRECATED_COMPILED_RUNTIME.notification(graphdb.InputPosition.empty),
      RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty,
        Factory.message("Runtime unsupported",
          "Expression of ReduceExpression(ReduceScope(Variable(y),Variable(x),Variable(x)),Parameter(  AUTOINT0,Integer),Parameter(  AUTOLIST1,List<Any>)) not yet supported"))))
  }

  test("Warn for cartesian product with runtime=interpreted") {
    val result = executeSingle("explain cypher runtime=interpreted match (a)-->(b), (c)-->(d) return *", Map.empty)

    result.notifications.toList should equal(List(
      CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(35, 1, 36), cartesianProduct(Set("c", "d").asJava))))
  }

  test("Don't warn for cartesian product when not using explain") {
    val result = executeWith(Configs.CartesianProduct, "match (a)-->(b), (c)-->(d) return *")

    result.notifications shouldBe empty
  }

  test("warn when requesting runtime=legacy_compiled on an unsupported query") {
    val result = executeSingle("EXPLAIN CYPHER runtime=legacy_compiled MATCH (a)-->(b), (c)-->(d) RETURN count(*)", Map.empty)
    result.notifications should contain(RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty,
      Factory.message("Runtime unsupported", "CountStar() is not supported")))
  }

  test("warn once when a single index hint cannot be fulfilled") {
    val result = executeSingle("EXPLAIN MATCH (n:Person) USING INDEX n:Person(name) WHERE n.name = 'John' RETURN n", Map.empty)
    result.notifications.toSet should contain(
      INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, index("Person", "name")))
  }

  test("warn for each unfulfillable index hint") {
    val result = executeSingle(
      """EXPLAIN MATCH (n:Person), (m:Party), (k:Animal)
        |USING INDEX n:Person(name)
        |USING INDEX m:Party(city)
        |USING INDEX k:Animal(species)
        |WHERE n.name = 'John' AND m.city = 'Reykjavik' AND k.species = 'Sloth'
        |RETURN n""".stripMargin, Map.empty)

    result.notifications should contain(
      INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, index("Person", "name")))
    result.notifications should contain(
      INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, index("Party", "city")))
    result.notifications should contain(
      INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, index("Animal", "species")))
  }

  test("Warnings should work on potentially cached queries") {
    val resultWithoutExplain = executeWith(Configs.CartesianProduct,
      "match (a)-->(b), (c)-->(d) return *")
    val resultWithExplain = executeWith(Configs.CartesianProduct,
      "explain match (a)-->(b), (c)-->(d) return *")

    resultWithoutExplain.notifications.toList shouldBe empty
    resultWithExplain.notifications.toList should equal(
      List(CARTESIAN_PRODUCT.notification(new graphdb.InputPosition(48, 1, 49), cartesianProduct(Set("c", "d").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with a single label") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with explicit label check") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' AND (n:Person) RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava))
    ))
  }

  test(
    "warn for unfulfillable index seek when using dynamic property lookup with a single label and negative predicate") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] <> 'value' RETURN n", Map.empty)

    result.notifications shouldBe empty
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with range seek") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] > 10 RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with range seek (reverse)") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE 10 > n['key-' + n.name] RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava))))
  }

  test(
    "warn for unfulfillable index seek when using dynamic property lookup with a single label and property existence check with exists")
  {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE exists(n['na' + 'me']) RETURN n", Map.empty)

    result.notifications should contain(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava)))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with a single label and starts with") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] STARTS WITH 'Foo' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with a single label and regex") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] =~ 'Foo*' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with a single label and IN") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person) WHERE n['key-' + n.name] IN ['Foo', 'Bar'] RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava))))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with multiple labels") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n:Person:Foo) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications should contain(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava)))
  }

  test("warn for unfulfillable index seek when using dynamic property lookup with multiple indexed labels") {
    graph.createIndex("Person", "name")
    graph.createIndex("Jedi", "weapon")

    val result = executeSingle("EXPLAIN MATCH (n:Person:Jedi) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person", "Jedi").asJava))))
  }

  test("should not warn when using dynamic property lookup with no labels") {
    graph.createIndex("Person", "name")

    val result = executeSingle("EXPLAIN MATCH (n) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications shouldBe empty
  }

  test("should warn when using dynamic property lookup with both a static and a dynamic property") {
    graph.createIndex("Person", "name")

    val result = executeSingle(
      "EXPLAIN MATCH (n:Person) WHERE n.name = 'Tobias' AND n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications should equal(Set(INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty,
      indexSeekOrScan(
        Set("Person").asJava))))
  }

  test("should not warn when using dynamic property lookup with a label having no index") {
    graph.createIndex("Person", "name")
    createLabeledNode("Foo")

    val result = executeSingle("EXPLAIN MATCH (n:Foo) WHERE n['key-' + n.name] = 'value' RETURN n", Map.empty)

    result.notifications shouldBe empty
  }

  test("should not warn for eager before load csv") {
    val result = executeSingle(
      "EXPLAIN MATCH (n) DELETE n WITH * LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE () RETURN line", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("LoadCSV").withLHS(aPlan("Eager"))
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.EagerOperatorWarning"
  }

  test("should warn for eager after load csv") {
    val result = executeSingle(
      "EXPLAIN MATCH (n) LOAD CSV FROM 'file:///ignore/ignore.csv' AS line WITH * DELETE n MERGE () RETURN line", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("Eager").withLHS(includeSomewhere.aPlan("LoadCSV"))
    result.notifications.map(_.getCode) should contain("Neo.ClientNotification.Statement.EagerOperatorWarning")
  }

  test("should warn for eager after load csv in 3.5") {
    val result = executeSingle(
      "EXPLAIN CYPHER 3.5 MATCH (n) LOAD CSV FROM 'file:///ignore/ignore.csv' AS line WITH * DELETE n MERGE () RETURN line", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("Eager").withLHS(includeSomewhere.aPlan("LoadCSV"))
    result.notifications.map(_.getCode) should contain("Neo.ClientNotification.Statement.EagerOperatorWarning")
  }

  test("should not warn for load csv without eager") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (:A) CREATE (:B) RETURN line", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("LoadCSV")
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.EagerOperatorWarning"
  }

  test("should not warn for eager without load csv") {
    val result = executeSingle("EXPLAIN MATCH (a), (b) CREATE (c) RETURN *", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("Eager")
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.EagerOperatorWarning"
  }

  test("should not warn for eager that precedes load csv") {
    val result = executeSingle(
      "EXPLAIN MATCH (a), (b) CREATE (c) WITH c LOAD CSV FROM 'file:///ignore/ignore.csv' AS line RETURN *", Map.empty)

    result.executionPlanDescription() should includeSomewhere.aPlan("LoadCSV").withLHS(includeSomewhere.aPlan("Eager"))
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.EagerOperatorWarning"
  }

  test("should warn for large label scans combined with load csv") {
    1 to 11 foreach { _ => createLabeledNode("A") }
    val result = executeSingle("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *", Map.empty)
    result.executionPlanDescription() should includeSomewhere.aPlan.withLHS(aPlan("LoadCSV")).withRHS(aPlan("NodeByLabelScan"))
    result.notifications.map(_.getCode) should contain("Neo.ClientNotification.Statement.NoApplicableIndexWarning")
  }

  test("should warn for large label scans with merge combined with load csv") {
    1 to 11 foreach { _ => createLabeledNode("A") }
    val result = executeSingle("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *", Map.empty)
    result.executionPlanDescription() should includeSomewhere.aPlan.withLHS(aPlan("LoadCSV")).withRHS(aPlan("AntiConditionalApply"))
    result.notifications.map(_.getCode) should contain("Neo.ClientNotification.Statement.NoApplicableIndexWarning")
  }

  test("should not warn for small label scans combined with load csv") {
    createLabeledNode("A")
    val result = executeSingle("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MATCH (a:A) RETURN *", Map.empty)
    result.executionPlanDescription() should includeSomewhere.aPlan.withLHS(aPlan("LoadCSV")).withRHS(aPlan("NodeByLabelScan"))
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.NoApplicableIndexWarning"
  }

  test("should not warn for small label scans with merge combined with load csv") {
    createLabeledNode("A")
    val result = executeSingle("EXPLAIN LOAD CSV FROM 'file:///ignore/ignore.csv' AS line MERGE (a:A) RETURN *", Map.empty)
    result.executionPlanDescription() should includeSomewhere.aPlan.withLHS(aPlan("LoadCSV")).withRHS(aPlan("AntiConditionalApply"))
    result.notifications.map(_.getCode) should not contain "Neo.ClientNotification.Statement.NoApplicableIndexWarning"
  }

  test("should warn for misspelled/missing label") {
    //given
    createLabeledNode("Person")

    //when
    val resultMisspelled = executeSingle("EXPLAIN MATCH (n:Preson) RETURN *", Map.empty)
    val resultCorrectlySpelled = executeSingle("EXPLAIN MATCH (n:Person) RETURN *", Map.empty)

    //then
    resultMisspelled.notifications should contain(
      MISSING_LABEL.notification(new graphdb.InputPosition(17, 1, 18), label("Preson")))

    resultCorrectlySpelled.notifications shouldBe empty
  }

  test("should warn for missing label and recompile if warning was invalidated later") {
    // when
    val resultFirst = executeSingle("EXPLAIN MATCH (a:NO_SUCH_THING) RETURN a")

    // then
    resultFirst.notifications should contain only MISSING_LABEL.notification(new graphdb.InputPosition(17, 1, 18), label("NO_SUCH_THING"))

    // given
    executeSingle("CREATE (:NO_SUCH_THING)")

    // when
    val resultSecond = executeSingle("EXPLAIN MATCH (a:NO_SUCH_THING) RETURN a")

    //then
    resultSecond.notifications shouldBe empty
  }

  test("should warn for multiple missing labels and recompile if warning was invalidated later") {
    // when
    val resultFirst = executeSingle("EXPLAIN MATCH (a:NO_SUCH_THING)-->(:OTHER) RETURN a")

    // then
    resultFirst.notifications should contain allOf(
      MISSING_LABEL.notification(new graphdb.InputPosition(17, 1, 18), label("NO_SUCH_THING")),
      MISSING_LABEL.notification(new graphdb.InputPosition(36, 1, 37), label("OTHER"))
    )

    // given
    executeSingle("CREATE (:NO_SUCH_THING)")

    // when
    val resultSecond = executeSingle("EXPLAIN MATCH (a:NO_SUCH_THING)-->(:OTHER) RETURN a")

    //then
    resultSecond.notifications should contain only MISSING_LABEL.notification(new graphdb.InputPosition(36, 1, 37), label("OTHER"))
  }

  test("should not warn for missing label on update") {

    //when
    val result = executeSingle("EXPLAIN CREATE (n:Person)", Map.empty)

    //then
    result.notifications shouldBe empty
  }

  test("should warn for misspelled/missing relationship type") {
    //given
    relate(createNode(), createNode(), "R")

    //when
    val resultMisspelled = executeSingle("EXPLAIN MATCH ()-[r:r]->() RETURN *", Map.empty)
    val resultCorrectlySpelled = executeSingle("EXPLAIN MATCH ()-[r:R]->() RETURN *", Map.empty)

    resultMisspelled.notifications should contain(
      MISSING_REL_TYPE
        .notification(new graphdb.InputPosition(20, 1, 21), NotificationDetail.Factory.relationshipType("r")))

    resultCorrectlySpelled.notifications shouldBe empty
  }

  test("should warn for missing rel type and recompile if warning was invalidated later") {
    // when
    val resultFirst = executeSingle("EXPLAIN MATCH (a)-[:NO_SUCH_THING]->(a) RETURN a")

    // then
    resultFirst.notifications should contain only MISSING_REL_TYPE.notification(new graphdb.InputPosition(20, 1, 21), relationshipType("NO_SUCH_THING"))

    // given
    executeSingle("CREATE (a)-[:NO_SUCH_THING]->(a)")

    // when
    val resultSecond = executeSingle("EXPLAIN MATCH (a)-[:NO_SUCH_THING]->(a) RETURN a")

    //then
    resultSecond.notifications shouldBe empty
  }

  test("should warn for multiple missing rel types and recompile if warning was invalidated later") {
    // when
    val resultFirst = executeSingle("EXPLAIN MATCH (a)-[:NO_SUCH_THING]->(a)<-[:OTHER]-() RETURN a")

    // then
    resultFirst.notifications should contain allOf(
      MISSING_REL_TYPE.notification(new graphdb.InputPosition(20, 1, 21), relationshipType("NO_SUCH_THING")),
      MISSING_REL_TYPE.notification(new graphdb.InputPosition(43, 1, 44), relationshipType("OTHER"))
    )

    // given
    executeSingle("CREATE (a)-[:NO_SUCH_THING]->(a)")

    // when
    val resultSecond = executeSingle("EXPLAIN MATCH (a)-[:NO_SUCH_THING]->(a)<-[:OTHER]-() RETURN a")

    //then
    resultSecond.notifications should contain only MISSING_REL_TYPE.notification(new graphdb.InputPosition(43, 1, 44), relationshipType("OTHER"))
  }

  test("should warn for misspelled/missing property names") {
    //given
    createNode(Map("prop" -> 42))
    //when
    val resultMisspelled = executeSingle("EXPLAIN MATCH (n) WHERE n.propp = 43 RETURN n", Map.empty)
    val resultCorrectlySpelled = executeSingle("EXPLAIN MATCH (n) WHERE n.prop = 43 RETURN n", Map.empty)

    resultMisspelled.notifications should contain(
      NotificationCode.MISSING_PROPERTY_NAME.notification(new graphdb.InputPosition(26, 1, 27), propertyName("propp")))

    resultCorrectlySpelled.notifications shouldBe empty
  }

  test("should warn for missing property key name and recompile if warning was invalidated later") {
    // when
    val resultFirst = executeSingle("EXPLAIN MATCH (a) RETURN a.name")

    // then
    resultFirst.notifications should contain only MISSING_PROPERTY_NAME.notification(new graphdb.InputPosition(27, 1, 28), propertyName("name"))

    // given
    executeSingle("CREATE (a{name:'Tim'})")

    // when
    val resultSecond = executeSingle("EXPLAIN MATCH (a) RETURN a.name")

    //then
    resultSecond.notifications shouldBe empty
  }

  test("should warn for multiple missing property key names and recompile if warning was invalidated later") {
    // when
    val resultFirst = executeSingle("EXPLAIN MATCH (a) RETURN a.name, a.age")

    // then
    resultFirst.notifications should contain allOf(
      MISSING_PROPERTY_NAME.notification(new graphdb.InputPosition(27, 1, 28), propertyName("name")),
      MISSING_PROPERTY_NAME.notification(new graphdb.InputPosition(35, 1, 36), propertyName("age"))
    )

    // given
    executeSingle("CREATE (a{name:'Tim'})")

    // when
    val resultSecond = executeSingle("EXPLAIN MATCH (a) RETURN a.name, a.age")

    //then
    resultSecond.notifications should contain only MISSING_PROPERTY_NAME.notification(new graphdb.InputPosition(35, 1, 36), propertyName("age"))
  }

  test("should not warn for missing properties on update") {
    val result = executeSingle("EXPLAIN CREATE (n {prop: 42})", Map.empty)

    result.notifications shouldBe empty
  }

  test("should not warn on timestamp function") {
    val result = executeSingle("EXPLAIN WITH timestamp() as ts RETURN ts")

    result.notifications shouldBe empty
  }

  test("should warn about unbounded shortest path") {
    val res = executeSingle("EXPLAIN MATCH p = shortestPath((n)-[*]->(m)) RETURN m", Map.empty)

    res.notifications should contain(
      UNBOUNDED_SHORTEST_PATH.notification(new graphdb.InputPosition(34, 1, 35)))
  }

  test("should not warn about literal maps") {
    val res = executeSingle("explain return { id: 42 } ", Map.empty)

    res.notifications should be(empty)
  }

  test("do not warn when creating a node with non-existent label when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n:Category)", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when merging a node with non-existent label when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE (n:Category)", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when setting on a node a non-existent label when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n) SET n:Category", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when creating a rel with non-existent type when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when merging a rel with non-existent type when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE ()-[:T]->()", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when creating a node with non-existent prop key id when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE (n) SET n.p = 'a'", Map.empty)

    result.notifications shouldBe empty
  }

  test("do not warn when merging a node with non-existent prop key id when using load csv") {
    val result = executeSingle(
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row MERGE (n) ON CREATE SET n.p = 'a'", Map.empty)

    result.notifications shouldBe empty
  }

  test("should warn when using contains on an index with SLOW_CONTAINS limitation") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name CONTAINS 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should contain(SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name")))
  }

  test("should warn when using ends with on an index with SLOW_CONTAINS limitation") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name ENDS WITH 'son' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should contain(SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name")))
  }

  test("should warn when using contains on a unique index with SLOW_CONTAINS limitation") {
    graph.createUniqueConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name CONTAINS 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should contain(SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name")))
  }

  test("should warn when using ends with on a unique index with SLOW_CONTAINS limitation") {
    graph.createUniqueConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name ENDS WITH 'son' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should contain(SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name")))
  }

  test("should not warn when using starts with on an index with SLOW_CONTAINS limitation") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name STARTS WITH 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }

  test("should not warn when using starts with on a unique index with SLOW_CONTAINS limitation") {
    graph.createUniqueConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name STARTS WITH 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }

  test("Should warn about repeated rel variable in pattern expression") {
    val query = "EXPLAIN MATCH ()-[r]-() RETURN size( ()-[r]-()-[r]-() ) AS size"

    val result = executeSingle(query, Map.empty)
    result.notifications should contain(REPEATED_REL_IN_PATTERN_EXPRESSION.notification(
      new graphdb.InputPosition(41, 1, 42),
      Factory.repeatedRel("r")))
  }

  test("Should warn about repeated rel variable in pattern comprehension") {
    val query = "EXPLAIN MATCH ()-[r]-() RETURN [ ()-[r]-()-[r]-() | r ] AS rs"
    val result = executeSingle(query, Map.empty)
    result.notifications should contain(REPEATED_REL_IN_PATTERN_EXPRESSION.notification(
      new graphdb.InputPosition(37, 1, 38),
      Factory.repeatedRel("r")))
  }
}

class LuceneIndexNotificationAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Need to override so that graph.execute will not throw an exception
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_hints_error -> FALSE,
    GraphDatabaseSettings.query_non_indexed_label_warning_threshold -> java.lang.Long.valueOf(10),
    GraphDatabaseSettings.default_schema_provider -> "lucene+native-3.0"
  )

  test("should not warn when using contains on an index with no special behaviours") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name CONTAINS 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }

  test("should not warn when using ends with on an index with no special behaviours") {
    graph.createIndex("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name ENDS WITH 'son' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }

  test("should not warn when using contains on a unique index with no special behaviours") {
    graph.createUniqueConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name CONTAINS 'er' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }

  test("should not warn when using ends with on a unique index with no special behaviours") {
    graph.createUniqueConstraint("Person", "name")
    val query = "EXPLAIN MATCH (a:Person) WHERE a.name ENDS WITH 'son' RETURN a"
    val result = executeSingle(query, Map.empty)
    result.notifications should not contain SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, suboptimalIndex("Person", "name"))
  }
}

object NotificationAcceptanceTest {

  class TestProcedures {

    @Procedure("newProc")
    def newProc(): Unit = {}

    @Deprecated
    @Procedure(name = "oldProc", deprecatedBy = "newProc")
    def oldProc(): Unit = {}

    @Procedure("changedProc")
    def changedProc(): java.util.stream.Stream[ChangedResults] =
      java.util.stream.Stream.builder().add(new ChangedResults).build()
  }

}
