/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.util

import java.nio.file.Path

import org.netbeans.lib.profiler.heap.ArrayItemValue
import org.netbeans.lib.profiler.heap.GCRoot
import org.netbeans.lib.profiler.heap.HeapFactory
import org.netbeans.lib.profiler.heap.Instance
import org.netbeans.lib.profiler.heap.JavaClass
import org.netbeans.lib.profiler.heap.JavaFrameGCRoot
import org.netbeans.lib.profiler.heap.ObjectArrayInstance
import org.netbeans.lib.profiler.heap.ObjectFieldValue
import org.netbeans.lib.profiler.heap.PrimitiveArrayInstance
import org.netbeans.lib.profiler.heap.ThreadObjectGCRoot
import org.openjdk.jol.info.ClassData
import org.openjdk.jol.info.ClassLayout
import org.openjdk.jol.layouters.CurrentLayouter

import scala.annotation.tailrec
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable

/**
 * Utility to read and analyze heap dumps
 * uses org.netbeans.lib.profiler internally
 */
object HeapDumpReader {

  def load(heapDumpPath: Path): HeapDump = HeapDump(heapDumpPath)

  case class HeapDump(heapDumpPath: Path) {

    private val heap = HeapFactory.createHeap(heapDumpPath.toFile)

    def threadReachableHeap(threadName: String): Long = {

      val threadId =
        heap.getGCRoots.asScala
            .collectFirst { case thread: ThreadObjectGCRoot if name(thread) == threadName => thread.getInstance().getInstanceId }.get

      val frameInstances = heap.getGCRoots.asScala
          .collect { case frame: JavaFrameGCRoot => frame }
          .filter(frame => threadId == id(frame.getThreadGCRoot))
          .map(frame => frame.getInstance())

      reachableSize(frameInstances.toSet)
        .size
    }

    def instancesInStack(threadName: String = Thread.currentThread().getName): Iterator[Instance] = {
      val threadId =
        heap.getGCRoots.asScala
            .collectFirst { case thread: ThreadObjectGCRoot if name(thread) == threadName => thread.getInstance().getInstanceId }.get

      heap.getGCRoots.asScala
          .collect { case frame: JavaFrameGCRoot if threadId == id(frame.getThreadGCRoot) => frame.getInstance() }
          .toIterator
    }

    def instancesOfType(className: String): Iterator[Instance] =
      heap.getJavaClassByName(className).getInstancesIterator.asScala
          .collect { case i: Instance => i }

    def fieldInstance(instance: Instance, fieldName: String): Instance =
      instance.getFieldValues.asScala
              .collectFirst { case field: ObjectFieldValue if field.getField.getName == fieldName => field.getInstance() }
              .get

    def reachableSize(): ReachableSize = ReachableSize()

    def reachableSize(instances: Set[Instance]): ReachableSize = ReachableSize(instances)
  }

  private def id(root: GCRoot): Long =
    root.getInstance().getInstanceId

  private def name(t: ThreadObjectGCRoot): String =
    materializeStringValue(t.getInstance().getValueOfField("name").asInstanceOf[Instance])

  private def materializeStringValue(instance: Instance): String = {
    instance.getValueOfField("value") match {
      case pi: PrimitiveArrayInstance =>
        val builder = new StringBuilder
        pi.getValues.forEach(value => value match {
          case code: String => builder.append(Integer.parseInt(code).toChar)
        })
        builder.toString()

      case other =>
        other.toString;
    }
  }
}

/**
 * Calculates the transitive reachable heap size given a number of root object instances
 */
case class ReachableSize(
  rootInstances: Set[Instance] = Set.empty,
  ignoredClassNames: Set[String] = ReachableSize.defaultIgnoredClasses
) {
  private val debugPrintWarn = false

  def including(root: Instance): ReachableSize =
    copy(rootInstances + root)

  def size: Long = {
    val seen = mutable.HashSet.empty[Long]
    rootInstances.foldLeft(0L) { case (size, inst) =>
      size + recursiveSize(List(inst), 0, seen)
    }
  }

  @tailrec
  private def recursiveSize(instances: List[Instance], accumulatedSize: Long, seenIds: mutable.HashSet[Long]): Long = {
    val instancesToVisit = instances.filter(shouldVisit(_, seenIds))
    if (instancesToVisit.isEmpty) accumulatedSize
    else {
      val shallowSize = instancesToVisit.map(instanceSize).sum

      val children = instancesToVisit.flatMap {
        case _: PrimitiveArrayInstance => List.empty
        case inst: ObjectArrayInstance => inst.getItems.asScala.collect { case item: ArrayItemValue => item.getInstance() }
        case inst                      => inst.getFieldValues.asScala.collect { case field: ObjectFieldValue => field.getInstance() }
      }

      recursiveSize(children, accumulatedSize + shallowSize, seenIds)
    }
  }

  @tailrec
  private def hasSuperClass(superClassNames: Set[String], cls: JavaClass): Boolean =
    if (cls == null) false
    else if (superClassNames.contains(cls.getName)) true
    else hasSuperClass(superClassNames, cls.getSuperClass)

  private def isIgnored(instance: Instance): Boolean =
    hasSuperClass(ignoredClassNames, instance.getJavaClass)

  private def shouldVisit(instance: Instance, seenIds: mutable.HashSet[Long]): Boolean =
    instance != null && seenIds.add(instance.getInstanceId) && !isIgnored(instance)

  private def instanceSize(instance: Instance): Long = {
    // Prefer using JOL to get shallow sizes - it seems to be more accurate
    // but fall back to getting them from the heap dump with netbeans if that fails (lambdas)
    val name = instance.getJavaClass.getName
    try {
      jolSize(instance)
    } catch {
      case _: Throwable =>
        if (debugPrintWarn) println(s"!!!! falling back to netbeansSize() for $name")
        netbeansSize(instance)
    }
  }


  private def jolSize(instance: Instance) = {
    val name = ClassName.fromHeapName(instance.getJavaClass.getName)
    val layout = instance match {
      case inst: PrimitiveArrayInstance =>
        new CurrentLayouter().layout(new ClassData(name.asCanonicalName, name.unwrapped.asCanonicalName, inst.getLength))
      case inst: ObjectArrayInstance    =>
        new CurrentLayouter().layout(new ClassData(name.asCanonicalName, name.unwrapped.asCanonicalName, inst.getLength))
      case _                            =>
        ClassLayout.parseClass(Class.forName(name.asJvmName))
    }
    layout.instanceSize()
  }

  private def netbeansSize(instance: Instance): Long =
    instance.getSize

  object ClassName {
    def fromHeapName(heapName: String): ClassName = {
      val (baseHeapName, arrayLevels) = stripArrayMarkers(heapName)
      ClassName(baseHeapName, arrayLevels)
    }

    @tailrec
    private def stripArrayMarkers(name: String, levels: Int = 0): (String, Int) =
      if (name.endsWith("[]")) stripArrayMarkers(name.dropRight(2), levels + 1) else (name, levels)
  }
  case class ClassName(baseHeapName: String, arrayLevels: Int) {

    val (baseJvmName: String, primitive: Boolean) =
      baseHeapName match {
        case "boolean" => ("Z", true)
        case "byte"    => ("B", true)
        case "short"   => ("S", true)
        case "int"     => ("I", true)
        case "long"    => ("J", true)
        case "float"   => ("F", true)
        case "double"  => ("D", true)
        case "char"    => ("C", true)
        case fqn       => (fqn, false)
      }

    def asCanonicalName: String =
      baseHeapName + "[]" * arrayLevels

    def asJvmName: String = (arrayLevels, primitive) match {
      case (0, _)     => baseJvmName
      case (l, false) => "[" * l + "L" + baseJvmName + ";"
      case (l, true)  => "[" * l + baseJvmName
    }

    def unwrapped: ClassName = copy(arrayLevels = arrayLevels - 1)
  }
}

object ReachableSize {
  val defaultIgnoredClasses = Set(
    // Filter these out as they seem to create noise in tests, and we assume that in
    // real applications they are stable across query executions
    classOf[java.lang.Class[_]].getName,
    classOf[java.lang.ClassLoader].getName,
    classOf[java.util.jar.JarFile].getName,
    "java.util.zip.ZipFile$Source",
    classOf[java.lang.Thread].getName,
    // Filter these out as they either are assumed not to be present, or
    // to be stable in real applications
    classOf[org.neo4j.internal.collector.RecentQueryBuffer].getName,
    classOf[org.neo4j.internal.id.indexed.IndexedIdGenerator].getName,
    classOf[org.neo4j.kernel.impl.newapi.AllStoreHolder].getName,
    classOf[org.neo4j.io.fs.EphemeralFileSystemAbstraction].getName,
    "org.neo4j.io.fs.EphemeralFileData",
  )
}
