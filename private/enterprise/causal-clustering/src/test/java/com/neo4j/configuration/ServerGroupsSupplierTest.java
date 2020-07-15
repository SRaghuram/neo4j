/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.Config;

import static com.neo4j.configuration.CausalClusteringSettings.server_groups;
import static org.assertj.core.api.Assertions.assertThat;

class ServerGroupsSupplierTest
{
    @Test
    void shouldReactToConfigChanges()
    {
        var config = Config.defaults();
        var groupsSupplier = ServerGroupsSupplier.listen( config );

        assertThat( groupsSupplier.get() ).isEmpty();

        config.set( server_groups, List.of( new ServerGroupName( "foo" ), new ServerGroupName( "bar" ) ) );

        assertThat( groupsSupplier.get() ).contains( new ServerGroupName( "foo" ), new ServerGroupName( "bar" ) );

        config.setDynamic( server_groups, List.of( new ServerGroupName( "baz" ) ), getClass().getSimpleName() );

        assertThat( groupsSupplier.get() ).contains( new ServerGroupName( "baz" ) );
    }
}
