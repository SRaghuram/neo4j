/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;

import static com.neo4j.configuration.CausalClusteringSettings.server_groups;
import static java.util.Set.copyOf;

public class ServerGroupsSupplier implements Supplier<Set<ServerGroupName>>
{
    private Set<ServerGroupName> current = Set.of();

    public static ServerGroupsSupplier listen( Config config )
    {
        var serverGroupsProvider = new ServerGroupsSupplier();
        serverGroupsProvider.update( config.get( server_groups ) );
        config.addListener( server_groups, ( before, after ) -> serverGroupsProvider.update( after ) );
        return serverGroupsProvider;
    }

    private ServerGroupsSupplier()
    {
    }

    private synchronized void update( List<ServerGroupName> groupNames )
    {
        this.current = copyOf( groupNames );
    }

    @Override
    public Set<ServerGroupName> get()
    {
        return current;
    }
}
