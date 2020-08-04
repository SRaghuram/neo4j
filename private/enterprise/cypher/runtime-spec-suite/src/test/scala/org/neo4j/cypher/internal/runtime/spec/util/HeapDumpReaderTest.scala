/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.util

import java.nio.file.Files

import org.neo4j.cypher.internal.runtime.spec.util.HeapDumpTestClasses.MultiDimByteArray
import org.neo4j.cypher.internal.runtime.spec.util.HeapDumpTestClasses.MultiDimLongArray
import org.neo4j.memory.HeapDumper
import org.neo4j.values.storable.Values
import org.netbeans.lib.profiler.heap.Instance
import org.openjdk.jol.info.ClassLayout
import org.scalatest.FunSuite
import org.scalatest.Matchers

sealed trait Nested {
  def inner: Nested
}
case class Nested1(inner: Nested) extends Nested
case class Nested2(inner: Nested) extends Nested

class HeapDumpReaderTest extends FunSuite with Matchers {

  private val dumper = new HeapDumper()

  def withTempHeapDump[T](func: HeapDumpReader.HeapDump => T): T = {
    val path = Files.createTempFile("HeapDumpLoaderTest", ".hprof")
    try {
      path.toFile.delete()
      dumper.createHeapDump(path.toAbsolutePath.toString, true)
      val heap = HeapDumpReader.load(path)
      func(heap)
    } finally {
      path.toFile.delete()
    }
  }

  test("gives transitive size without duplicates") {

    // a -> b -> c
    // x -> y -> c

    val a = Nested1(Nested1(Nested1(null)))
    val x = Nested2(Nested2(a.inner.inner))

    withTempHeapDump { heap =>
      val ai = heap.instancesInStack(Thread.currentThread().getName)
                   .filter(_.getJavaClass.getName == classOf[Nested1].getName)
                   .toSeq.head

      val xi = heap.instancesInStack(Thread.currentThread().getName)
                   .filter(_.getJavaClass.getName == classOf[Nested2].getName)
                   .toSeq.head

      val isolatedSizeA = heap.reachableSize().including(ai).size
      val isolatedSizeX = heap.reachableSize().including(xi).size
      val combinedSize = heap.reachableSize().including(ai).including(xi).size

      isolatedSizeA
        .shouldEqual(Seq(
          ClassLayout.parseInstance(a).instanceSize(),
          ClassLayout.parseInstance(a.inner).instanceSize(),
        ).sum)

      isolatedSizeX
        .shouldEqual(Seq(
          ClassLayout.parseInstance(x).instanceSize(),
          ClassLayout.parseInstance(x.inner).instanceSize(),
        ).sum)

      combinedSize
        .shouldEqual(Seq(
          ClassLayout.parseInstance(a).instanceSize(),
          ClassLayout.parseInstance(a.inner).instanceSize(),
          ClassLayout.parseInstance(x).instanceSize(),
          ClassLayout.parseInstance(x.inner).instanceSize(),
          ClassLayout.parseInstance(x.inner.inner).instanceSize(),
        ).sum)

    }
  }

  test("gives accurate sizes for kernel values") {
    val booleanValue = Values.booleanValue(true)
    val byteValue = Values.byteValue(12)
    val charValue = Values.charValue('c')
    val doubleValue = Values.doubleValue(12.34)
    val intValue = Values.intValue(123)
    val shortValue = Values.shortValue(123)
    val stringValue = Values.stringValue("abcd")

    withTempHeapDump { heap =>

      heap.sizeOfStackInstance(booleanValue).shouldEqual(ClassLayout.parseInstance(booleanValue).instanceSize())
      heap.sizeOfStackInstance(byteValue).shouldEqual(byteValue.estimatedHeapUsage())
      heap.sizeOfStackInstance(charValue).shouldEqual(charValue.estimatedHeapUsage())
      heap.sizeOfStackInstance(doubleValue).shouldEqual(doubleValue.estimatedHeapUsage())
      heap.sizeOfStackInstance(intValue).shouldEqual(intValue.estimatedHeapUsage())
      heap.sizeOfStackInstance(shortValue).shouldEqual(shortValue.estimatedHeapUsage())
      heap.sizeOfStackInstance(stringValue).shouldEqual(stringValue.estimatedHeapUsage())

    }
  }

  test("multi-dimensional object arrays") {
    val root = new HeapDumpTestClasses.MultiDimObjectArray

    withTempHeapDump { heap =>

      heap.sizeOfStackInstance(root)
          .shouldEqual(Seq(
            ClassLayout.parseInstance(root).instanceSize(),
            ClassLayout.parseInstance(root.empties).instanceSize(),
            ClassLayout.parseInstance(root.empties(0)).instanceSize(),
            ClassLayout.parseInstance(root.empties(0)(0)).instanceSize(),
            ClassLayout.parseInstance(root.empties(1)).instanceSize(),
            ClassLayout.parseInstance(root.empties(1)(0)).instanceSize(),
          ).sum)
    }
  }

  test("multi-dimensional long arrays") {
    val root = new MultiDimLongArray

    withTempHeapDump { heap =>

      heap.sizeOfStackInstance(root)
          .shouldEqual(Seq(
            ClassLayout.parseInstance(root).instanceSize(),
            ClassLayout.parseInstance(root.longs).instanceSize(),
            ClassLayout.parseInstance(root.longs(0)).instanceSize(),
            ClassLayout.parseInstance(root.longs(1)).instanceSize(),
          ).sum)
    }
  }


  test("multi-dimensional byte arrays") {
    val root = new MultiDimByteArray

    withTempHeapDump { heap =>

      heap.sizeOfStackInstance(root)
          .shouldEqual(Seq(
            ClassLayout.parseInstance(root).instanceSize(),
            ClassLayout.parseInstance(root.bytes).instanceSize(),
            ClassLayout.parseInstance(root.bytes(0)).instanceSize(),
            ClassLayout.parseInstance(root.bytes(0)(0)).instanceSize(),
            ClassLayout.parseInstance(root.bytes(0)(1)).instanceSize(),
            ClassLayout.parseInstance(root.bytes(1)).instanceSize(),
            ClassLayout.parseInstance(root.bytes(1)(0)).instanceSize(),
          ).sum)
    }
  }

  implicit class HeapDumpExtras(heap: HeapDumpReader.HeapDump) {
    def findStackInstance(obj: AnyRef): Instance =
      heap.instancesInStack().find(_.getJavaClass.getName == obj.getClass.getName).get

    def sizeOfStackInstance(obj: AnyRef): Long =
      heap.reachableSize().including(findStackInstance(obj)).size
  }

}
