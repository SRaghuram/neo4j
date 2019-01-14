/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ha.correctness;

/** Something that will cause a state transition. For instance, delivery of a message or an instance crashing. */
interface ClusterAction
{
    Iterable<ClusterAction> perform( ClusterState state ) throws Exception;
}
