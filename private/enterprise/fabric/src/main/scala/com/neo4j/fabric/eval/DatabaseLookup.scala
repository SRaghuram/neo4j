/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.util.function.Supplier

import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseManager
import org.neo4j.kernel.database.NamedDatabaseId

import scala.compat.java8.OptionConverters.RichOptionalGeneric

import scala.collection.JavaConverters.asScalaSetConverter

trait DatabaseLookup {

  def databaseIds: Set[NamedDatabaseId]

  def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId]
}

object DatabaseLookup {

  class Default(
    databaseManager: Supplier[DatabaseManager[DatabaseContext]],
  ) extends DatabaseLookup {

    def databaseIds: Set[NamedDatabaseId] =
      databaseManager.get().registeredDatabases().keySet().asScala.toSet

    def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId] =
      databaseManager.get().databaseIdRepository().getByName(databaseName).asScala
  }
}
