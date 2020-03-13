/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import com.neo4j.fabric.executor.Location

trait CatalogManager {

  def currentCatalog(): Catalog

  def locationOf(graph: Catalog.Graph, requireWritable: Boolean): Location

}
