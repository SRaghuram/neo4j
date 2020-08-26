/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;

import org.neo4j.kernel.database.DatabaseId;

public interface Topology<T extends DiscoveryServerInfo>
{
    DatabaseId databaseId();

    Map<MemberId, T> servers();
}
