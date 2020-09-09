/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import java.util.Set;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

class DefaultDiscoveryMember implements DiscoveryMember
{
    private final ServerId id;
    private final Set<NamedDatabaseId> startedDatabases;

    DefaultDiscoveryMember( ServerId id, Set<NamedDatabaseId> startedDatabases )
    {
        this.id = id;
        this.startedDatabases = startedDatabases;
    }

    @Override
    public ServerId id()
    {
        return id;
    }

    @Override
    public Set<NamedDatabaseId> startedDatabases()
    {
        return startedDatabases;
    }

    @Override
    public String toString()
    {
        return "DefaultDiscoveryMember{" +
               "id=" + id +
               ", startedDatabases=" + startedDatabases +
               '}';
    }
}
