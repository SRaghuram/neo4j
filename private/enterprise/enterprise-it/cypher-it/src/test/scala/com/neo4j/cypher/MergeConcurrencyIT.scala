/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.kernel.DeadlockDetectedException

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class MergeConcurrencyIT extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {

  val nodeCount = 100
  val threadCount = 10

  test("should handle ten simultaneous threads") {
    // Given a constraint on :Label(id), create a linked list
    execute("CREATE CONSTRAINT ON (n:Label) ASSERT n.id IS UNIQUE")

    var exceptionsThrown = List.empty[Throwable]
    val q = "MERGE (a:Label {id:$id}) " +
      "MERGE (b:Label {id:$id+1}) " +
      "MERGE (a)-[r:TYPE]->(b) " +
      "RETURN a, b, r"

    val runner = new Runnable {
      def run() {
        try {
          (1 to nodeCount) foreach {
            x => execute(q, "id" -> x)
          }
        }
        catch {
          case _: DeadlockDetectedException =>
          // The enterprise Deadlock Detection can have false positives. Ignore.
          case e: Throwable => exceptionsThrown = exceptionsThrown :+ e
        }
      }
    }

    val threads: Seq[Thread] = 0 until threadCount map (_ => new Thread(runner))

    threads.foreach(_.start())
    threads.foreach(_.join())
    exceptionsThrown.foreach(throw _)

    // Check that we haven't created duplicate nodes or duplicate relationships
    execute("match (a:Label) with a.id as id, count(*) as c where c > 1 return *") shouldBe empty
    execute("match (a)-[r1]->(b)<-[r2]-(a) where r1 <> r2 return *") shouldBe empty

    val details = "\n" + graph.withTx( tx => tx.execute("match (a)-[r]->(b) return a.id, b.id, id(a), id(r), id(b)").resultAsString())

    assert(execute(s"match p=(:Label {id:1})-[*..1000]->({id:$nodeCount}) return 1").size === 1, details)
  }

  test("should handle ten simultaneous threads with only nodes - with constraint") {
    execute("CREATE CONSTRAINT ON (n:Label) ASSERT n.id IS UNIQUE")
    var exceptionsThrown = List.empty[Throwable]

    val runner = new Runnable {
      def run() {
        try {
          (0 until nodeCount) foreach {
            x => execute("MERGE (a:Label {id:$id})", "id" -> x)
          }
        } catch {
          case _: DeadlockDetectedException =>
          // The enterprise Deadlock Detection can have false positives. Ignore.
          case e: Throwable => exceptionsThrown = exceptionsThrown :+ e
        }
      }
    }

    val threads: Seq[Thread] = 0 until threadCount map (_ => new Thread(runner))

    threads.foreach(_.start())
    threads.foreach(_.join())
    exceptionsThrown.foreach(throw _)

    // Check that we haven't created duplicate nodes or duplicate relationships
    execute("match (a:Label) with a.id as id, count(*) as c where c > 1 return *") shouldBe empty
    executeScalar[Int]("match (a:Label) return count(a)") shouldBe nodeCount

    0 until nodeCount foreach { i =>
      withClue(s"did not find node with id $i") {
        execute(s"match (:Label {id:$i}) return 1") should have size 1
      }
    }
  }

  test("merge relationship - one bound end node") {
    val n1 = createNode()
    val n2 = createNode()
    val query = "MATCH (n) WHERE ID(n) = $id1 MERGE (n)-[r:TEST]-(m)"
    execute(s"EXPLAIN $query", "id1" -> n1.getId, "id2" -> n2.getId)
    var exceptionsThrown = List.empty[Throwable]

    def createRunner(n1: Long, n2: Long) = new Runnable {
      def run() {
        try {
          (0 until nodeCount) foreach {
            _ => execute(query, "id1" -> n1, "id2" -> n2)
          }
        } catch {
          case _: DeadlockDetectedException =>
          // The enterprise Deadlock Detection can have false positives. Ignore.
          case e: Throwable => exceptionsThrown = exceptionsThrown :+ e
        }
      }
    }

    val threads: Seq[Thread] = 0 until threadCount map { x =>
      val (from, to) = if(x % 2 == 0) (n1.getId, n2.getId) else (n2.getId, n1.getId)
      new Thread(createRunner(from, to))
    }

    threads.foreach(_.start())
    threads.foreach(_.join())
    exceptionsThrown.foreach(throw _)

    graph.withTx( tx => {
      tx.getNodeById(n1.getId).getRelationships.asScala.size should equal(1)
      tx.getNodeById(n2.getId).getRelationships.asScala.size should equal(1)
    } )
  }

  test("merge relationship - both end nodes matched") {
    val n1 = createNode()
    val n2 = createNode()
    val query = "MATCH (n), (m) WHERE ID(n) = $id1 AND ID(m) = $id2 MERGE (n)-[r:TEST]-(m)"
    execute(s"EXPLAIN $query", "id1" -> n1.getId, "id2" -> n2.getId)

    var exceptionsThrown = List.empty[Throwable]

    def createRunner(n1: Long, n2: Long) = new Runnable {
      def run() {
        try {
          (0 until nodeCount) foreach {
            _ => execute(query, "id1" -> n1, "id2" -> n2)
          }
        } catch {
          case _: DeadlockDetectedException =>
          // The enterprise Deadlock Detection can have false positives. Ignore.
          case e: Throwable => exceptionsThrown = exceptionsThrown :+ e
        }
      }
    }

    val threads: Seq[Thread] = 0 until threadCount map { x =>
      val (from, to) = if(x % 2 == 0) (n1.getId, n2.getId) else (n2.getId, n1.getId)
      new Thread(createRunner(from, to))
    }

    threads.foreach(_.start())
    threads.foreach(_.join())
    exceptionsThrown.foreach(throw _)

    graph.withTx( tx =>  {
      tx.getNodeById(n1.getId).getRelationships.asScala.size should equal(1)
      tx.getNodeById(n2.getId).getRelationships.asScala.size should equal(1)
    } )
  }
}
