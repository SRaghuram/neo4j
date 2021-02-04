/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Path
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class VarLengthExpandQueryPlanAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("Plan should have right relationship direction") {
    setUp("From")
    val query = "MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"

    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)").containingArgumentRegex("""\(e\)\<\-\[anon_[0-9]*\*\.\.4\]\-\(a\)""".r)
        plan should includeSomewhere.aPlan("NodeByLabelScan").containingArgument("e:To")
      }))
  }

  test("Plan should have right relationship direction, other direction") {
    setUp("To")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)").containingArgumentRegex("""\(a\)\-\[anon_[0-9]*\*\.\.4\]\-\>\(e\)""".r)
        plan should includeSomewhere.aPlan("NodeByLabelScan").containingArgument("a:From")
      }))
  }

  test("Plan pruning var expand on distinct var-length match") {
    val query = "MATCH (a)-[*1..2]->(c) RETURN DISTINCT c"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("Plan pruning var expand on distinct var-length match with projection and aggregation") {
    val query = "MATCH (a)-[*1..2]->(c) WITH DISTINCT c RETURN count(*)"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("query with distinct aggregation") {
    val query = "MATCH (from)-[*1..3]->(to) RETURN count(DISTINCT to)"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("Simple query that filters between expand and distinct") {
    val query = "MATCH (a)-[*1..3]->(b:X) RETURN DISTINCT b"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("Query that aggregates before making the result DISTINCT") {
    val query = "MATCH (a)-[:R*1..3]->(b) WITH count(*) AS count RETURN DISTINCT count"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
  }

  test("Double var expand with distinct result") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T*1..3]->(c) RETURN DISTINCT c"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("var expand followed by normal expand") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("optional match can be solved with PruningVarExpand") {
    val query = "MATCH (a) OPTIONAL MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("should not rewrite when doing non-distinct aggregation") {
    val query = "MATCH (a)-[*1..3]->(b) RETURN b, count(*)"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
  }

  test("on longer var-lengths, we also use PruningVarExpand") {
    val query = "MATCH (a)-[*4..5]->(b) RETURN DISTINCT b"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(Pruning)")
      }))
  }

  test("Do not plan pruning var expand for length=1") {
    val query = "MATCH (a)-[*1..1]->(b) RETURN DISTINCT b"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
  }

  test("AllNodesInPath") {
    graph.withTx( tx => tx.execute("CREATE (a:A {foo: 'bar'})-[:REL]->(b:B {foo: 'bar'})-[:REL]->(c:C {foo: 'bar'})-[:REL]->(d:D {foo: 'bar', name: 'd'})"))
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE all(i IN nodes(p) WHERE i.foo = 'bar')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("AllRelationships") {
    graph.withTx( tx => tx.execute("CREATE (a:A)-[:REL {foo: 'bar'}]->(b:B)-[:REL {foo: 'bar'}]->(c:C)-[:REL {foo: 'bar'}]->(d:D {name: 'd'})"))
    val query = """MATCH p = (pA)-[:REL*3..3  {foo:'bar'}]->(pB)
                  |WHERE all(i IN relationships(p) WHERE i.foo = 'bar')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("AllRelationshipsInPath") {
    graph.withTx( tx => tx.execute("CREATE (a:A)-[:REL {foo: 'bar'}]->(b:B)-[:REL {foo: 'bar'}]->(c:C)-[:REL {foo: 'bar'}]->(d:D {name: 'd'})"))
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE all(i IN relationships(p) WHERE i.foo = 'bar')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("NoNodeInPath") {
    graph.withTx( tx => tx.execute("CREATE (a:A {foo: 'bar'})-[:REL]->(b:B {foo: 'bar'})-[:REL]->(c:C {foo: 'bar'})-[:REL]->(d:D {foo: 'bar', name: 'd'})"))
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE none(i IN nodes(p) WHERE i.foo = 'barz')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("NoRelationshipInPath") {
    graph.withTx( tx => tx.execute("CREATE (a:A)-[:REL {foo: 'bar'}]->(b:B)-[:REL {foo: 'bar'}]->(c:C)-[:REL {foo: 'bar'}]->(d:D {name: 'd'})"))
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE none(i IN relationships(p) WHERE i.foo = 'barz')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("AllNodesInPath with inner predicate using labelled nodes of the path") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN nodes(p) WHERE single(y IN nodes(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with inner predicate using labelled named nodes of the path") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (start:NODE)-[rel*1]->(end:NODE)
        | WHERE ALL(x IN nodes(p) WHERE single(y IN nodes(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with inner predicate using nodes of the path") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1,node2)

    val query =
      """
        | MATCH p = ()-[*1]->()
        | WHERE ALL(x IN nodes(p) WHERE single(y IN nodes(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with complex inner predicate using the start node and end node") {
    val node1 = createLabeledNode(Map("prop" -> 1), "NODE")
    val node2 = createLabeledNode(Map("prop" -> 1),"NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (start:NODE)-[*1..2]->(end:NODE)
        | WHERE ALL(x IN nodes(p) WHERE x.prop = nodes(p)[0].prop AND x.prop = nodes(p)[1].prop)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN nodes(p) WHERE length(p) = 1)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with inner predicate only using start node") {
    val node1 = createLabeledNode(Map("prop" -> 5),"NODE")
    val node2 = createLabeledNode(Map("prop" -> 5),"NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (n)-[r*1]->()
        | WHERE ALL(x IN nodes(p) WHERE x.prop = n.prop)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllRelationshipsInPath with inner predicate using relationships of the path") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN relationships(p) WHERE single(y IN relationships(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllRelationshipsInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN relationships(p) WHERE length(p) = 1)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("NoNodesInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1..2]->(:NODE)
        | WHERE NONE(x IN nodes(p) WHERE length(p) = 2)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("NoRelationshipsInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1..2]->(:NODE)
        | WHERE NONE(x IN relationships(p) WHERE length(p) = 2)
        | RETURN p
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("Do not plan pruning var expand when path is needed") {
    val query = "MATCH p=(from)-[r*0..1]->(to) WITH nodes(p) AS d RETURN DISTINCT d"
    executeWith(Configs.InterpretedAndSlottedAndPipelined, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should includeSomewhere.aPlan("VarLengthExpand(All)")
      }))
  }

  private def setupEvilCustomerGraph(): Unit = {
    val setup =
      """
        |CREATE (e1:DEPART:ElementHTA {acr:"ACR", dateDebut:0, dateFin:9223372036854775807, type:"DEPART" })<-[:EST_ETAT_DE]-(ee1:EtatElementHTA {acr:"ACR",idSitr:"idSitrDep", codeGdo:"gdoDepart", dateDebut:0, dateFin:9223372036854775807})
        |CREATE (e1)<-[:ETAT_COURANT]-(ee1)
        |
        |CREATE (e2:ElementHTA {acr:"ACR", dateDebut:0, dateFin:9223372036854775807, type:"TRONCON" })<-[:EST_ETAT_DE]-(ee2:EtatElementHTA {acr:"ACR",idSitr:"idSitrTroncon1", codeGdo:"gdoTroncon1", dateDebut:0, dateFin:9223372036854775807})
        |CREATE (e2)<-[:ETAT_COURANT]-(ee2)
        |
        |CREATE (e3:ElementHTA {acr:"ACR", dateDebut:0, dateFin:9223372036854775807, type:"ETOILEMENT" })<-[:EST_ETAT_DE]-(ee3:EtatElementHTA {acr:"ACR",idSitr:"idSitrEtoilement", codeGdo:"gdoEtoilement", dateDebut:0, dateFin:9223372036854775807})
        |CREATE (e3)<-[:ETAT_COURANT]-(ee3)
        |
        |CREATE (e4:ElementHTA {acr:"ACR", dateDebut:0, dateFin:9223372036854775807, type:"COUPUREISOLEE" })<-[:EST_ETAT_DE]-(ee4:EtatElementHTA {acr:"ACR",idSitr:"idSitrCoupure1", codeGdo:"gdoCoupure1", dateDebut:0, dateFin:9223372036854775807, positionSchemaNormal: '0'})
        |CREATE (e4)<-[:ETAT_COURANT]-(ee4)
        |
        |CREATE (e5:ElementHTA {acr:"ACR", dateDebut:0, dateFin:9223372036854775807, type:"JEUDEBARRE" })<-[:EST_ETAT_DE]-(ee5:EtatElementHTA {acr:"ACR",idSitr:"idSitrJdb1", codeGdo:"gdoJdb1", dateDebut:0, dateFin:9223372036854775807})
        |CREATE (e5)<-[:ETAT_COURANT]-(ee5)
        |
        |CREATE (e6:ElementHTA {acr:"ACR", dateDebut:0, dateFin:9223372036854775807, type:"COUPURESIMPLE" })<-[:EST_ETAT_DE]-(ee6:EtatElementHTA {acr:"ACR",idSitr:"idSitrCoupure2", codeGdo:"gdoCoupure2", dateDebut:0, dateFin:9223372036854775807, positionSchemaNormal: '0'})
        |CREATE (e6)<-[:ETAT_COURANT]-(ee6)
        |
        |CREATE (e7:ElementHTA {acr:"ACR", dateDebut:0, dateFin:9223372036854775807, type:"TRONCON" })<-[:EST_ETAT_DE]-(ee7:EtatElementHTA {acr:"ACR",idSitr:"idSitrTroncon2", codeGdo:"gdoTroncon2", dateDebut:0, dateFin:9223372036854775807})
        |CREATE (e7)<-[:ETAT_COURANT]-(ee7)
        |
        |CREATE (e8:ElementHTA {acr:"ACR", dateDebut:0, dateFin:9223372036854775807, type:"JEUDEBARRE" })<-[:EST_ETAT_DE]-(ee8:EtatElementHTA {acr:"ACR",idSitr:"idSitrJdb2", codeGdo:"gdoJdb2", dateDebut:0, dateFin:9223372036854775807})
        |CREATE (e8)<-[:ETAT_COURANT]-(ee8)
        |
        |CREATE (e1)-[:EST_CONNECTE_A { dateDebut:0, dateFin:9223372036854775807 }]->(e2)
        |CREATE (e2)-[:EST_CONNECTE_A { dateDebut:0, dateFin:9223372036854775807 }]->(e3)
        |CREATE (e3)-[:EST_CONNECTE_A { dateDebut:0, dateFin:9223372036854775807 }]->(e4)
        |CREATE (e4)-[:EST_CONNECTE_A { dateDebut:0, dateFin:9223372036854775807 }]->(e5)
        |CREATE (e5)-[:EST_CONNECTE_A { dateDebut:0, dateFin:9223372036854775807 }]->(e6)
        |CREATE (e6)-[:EST_CONNECTE_A { dateDebut:0, dateFin:9223372036854775807 }]->(e7)
        |CREATE (e7)-[:EST_CONNECTE_A { dateDebut:0, dateFin:9223372036854775807 }]->(e8)
        |
        |create (p1:Position {acr:"ACR", dateDebut:0, dateEnregistrement:1562163748909, dateFin:9223372036854775807, passant:true, raison:"1"})-[:EST_POSITION_DE]->(e1)
        |create (p1)-[:EST_POSITION_DE_COURANT]->(e1)
        |
        |create (p6:Position {acr:"ACR", dateDebut:1, dateEnregistrement:1562163748909, dateFin:9223372036854775807, passant:true, raison:"1"})-[:EST_POSITION_DE]->(e6)
        |create (p6)-[:EST_POSITION_DE_COURANT]->(e6)
        |
        |create (ea1:EtatAlimentation {acr:"ACR", dateDebut:1, dateEnregistrement:1562163748909, dateFin:9223372036854775807, estAlimente:true})-[:ETAT_ALIMENTATION]->(e1)
        |create (ea1)-[:ETAT_ALIMENTATION_COURANT]->(e1);
      """.stripMargin

    executeSingle(setup)
  }

  test("Var expand should plan NestedPlanExpressions for inner predicates, not RollUpApply") {
    setupEvilCustomerGraph()

    val query =
      """
        |MATCH (ee1:EtatElementHTA {acr:'ACR', idSitr:'idSitrDep'})-[:EST_ETAT_DE]->(e1:ElementHTA)
        |WITH e1
        |MATCH path = ( (e1) -[r1:EST_CONNECTE_A*0..]- (element:ElementHTA) )
        |WHERE
        |    ALL(noeud in nodes(path) WHERE
        |      head( [ (noeud)<-[r2:EST_POSITION_DE]-(pos:Position) | pos.passant ] ) = true
        |      //noeud.acr = "ACR"
        |    )
        |RETURN path
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined - Configs.PipelinedFused, query,
      planComparisonStrategy =
        ComparePlansWithAssertion( plan => {
          plan shouldNot includeSomewhere.aPlan("RollUpApply")
        }))
    result.toList should have size 1
  }

  test("Var expand should honour the predicate also for the first node: with GetDregree") {
    setupEvilCustomerGraph()
    val query =
      """
        |WITH 'ACR' AS laacr, 'idSitrDep' AS leIdSitr, 1562144482517 AS ladate
        |MATCH (elementDepartPropagation:ElementHTA)
        |WITH elementDepartPropagation, ladate
        |MATCH path = ( (elementDepartPropagation) -[:EST_CONNECTE_A*0..]- (element:ElementHTA) )
        |WHERE
        |    ALL(noeud in nodes(path) WHERE
        |      noeud.dateDebut <= ladate < noeud.dateFin
        |      AND size( (noeud)<--() ) = 1
        |    )
        |AND
        |    ALL(r in relationships(path)
        |        WHERE r.dateDebut <= ladate < r.dateFin)
        |OPTIONAL MATCH (ge:AnnotationProvisoire:GroupeElectrogene) -[:ANNOTE]-> (element)
        |  WHERE ge.dateDebut <= ladate < ge.dateFin
        |WITH element, ge, ladate
        |MATCH (etat:EtatElementHTA) -[:EST_ETAT_DE]-> (element)
        |RETURN element""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined - Configs.PipelinedFused, query)

    result.toList shouldBe empty
  }

  test("Var expand should honour the predicate also for the first node") {
    createLabeledNode(Map("bar" -> 2), "Foo")
    val query =
      """
        |MATCH (a:Foo)
        |MATCH path = ( (a)-[:REL*0..]-() )
        |WHERE ALL(n in nodes(path) WHERE n.bar = 1)
        |RETURN path
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.toList shouldBe empty
  }

  private def setUp(startLabel: String) {
    val a = createLabeledNode(Map("name" -> "Keanu Reeves"), "From")
    val b = createLabeledNode(Map("name" -> "Craig"), "User")
    val c = createLabeledNode(Map("name" -> "Olivia"), "User")
    val d = createLabeledNode(Map("name" -> "Carrie"), "User")
    val e = createLabeledNode(Map("name" -> "Andres"), "To")
    // Ensure compiler prefers to start at low cardinality 'To' node
    Range(0, 100).foreach(i => createLabeledNode(Map("name" -> s"node $i"), startLabel))
    relate(a, b)
    relate(b, c)
    relate(c, d)
    relate(d, e)
  }
}
