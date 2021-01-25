/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Set;
import java.util.function.BiConsumer;

import org.neo4j.kernel.database.DatabaseId;

@FunctionalInterface
public interface RaftListener extends BiConsumer<DatabaseId,Set<RaftMemberId>>
{
}
