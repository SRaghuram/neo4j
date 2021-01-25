/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka

import org.scalatest.mock.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.GivenWhenThen
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

trait NeoSuite extends WordSpecLike with Matchers with BeforeAndAfterAll with GivenWhenThen with MockitoSugar
