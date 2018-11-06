/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

case class Versions(versions: Version*) {
  def +(other: Version): Versions = {
    val newVersions = if (!versions.contains(other)) versions :+ other else versions
    Versions(newVersions: _*)
  }
}

object Versions {
  implicit def versionToVersions(version: Version): Versions = Versions(version)

  val orderedVersions: Seq[Version] = Seq(V3_4, V4_0)

  val oldest: Version = orderedVersions.head
  val latest: Version = orderedVersions.last
  val all = Versions(orderedVersions: _*)

  def definedBy(preParserArgs: Array[String]): Versions = {
    val versions = all.versions.filter(_.isDefinedBy(preParserArgs))
    if (versions.nonEmpty) Versions(versions: _*) else all
  }

  object V3_4 extends Version("3.4") {
    // 3.4 has 4.0 runtime
    override val acceptedRuntimeVersionNames = Set("4.0")
  }

  object V4_0 extends Version("4.0")

}

case class Version(name: String) {
  // inclusive
  def ->(other: Version): Versions = {
    val fromIndex = Versions.orderedVersions.indexOf(this)
    val toIndex = Versions.orderedVersions.indexOf(other) + 1
    Versions(Versions.orderedVersions.slice(fromIndex, toIndex): _*)
  }

  val acceptedRuntimeVersionNames: Set[String] = Set(name)
  val acceptedPlannerVersionNames: Set[String] = Set(name)

  def isDefinedBy(preParserArgs: Array[String]): Boolean = preParserArgs.contains(name)
}
