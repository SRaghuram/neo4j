/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.metadata;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatabaseSecurityCommands
{
    public final List<String> roleSetup;
    public final List<String> userSetup;

    public DatabaseSecurityCommands( List<String> roleSetup, List<String> userSetup )
    {
        this.roleSetup = roleSetup;
        this.userSetup = userSetup;
    }

    public List<String> getCommands()
    {
        return Stream.concat( roleSetup.stream(), userSetup.stream() ).collect( Collectors.toList() );
    }

    public boolean isEmpty()
    {
        return roleSetup.isEmpty() && userSetup.isEmpty();
    }
}
