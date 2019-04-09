/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.unmodifiableSet;

public class DefaultDiscoveryMember implements DiscoveryMember
{
    private final MemberId id;
    private final Supplier<DatabaseManager<?>> databaseManagerSupplier; // todo: remove supplier somehow

    public DefaultDiscoveryMember( MemberId id, Supplier<DatabaseManager<?>> databaseManagerSupplier )
    {
        this.id = id;
        this.databaseManagerSupplier = databaseManagerSupplier;
    }

    @Override
    public MemberId id()
    {
        return id;
    }

    @Override
    public Set<DatabaseId> databaseIds()
    {
        return unmodifiableSet( databaseManagerSupplier.get().registeredDatabases().keySet() );
    }
}
