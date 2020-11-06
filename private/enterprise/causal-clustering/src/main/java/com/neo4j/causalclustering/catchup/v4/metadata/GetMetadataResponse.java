/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.metadata;

import java.util.List;
import java.util.Objects;

public class GetMetadataResponse
{
    private final List<String> commands;

    public GetMetadataResponse( List<String> commands )
    {
        this.commands = commands;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        GetMetadataResponse that = (GetMetadataResponse) o;
        return Objects.equals( commands, that.commands );
    }

    public List<String> commands()
    {
        return commands;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( commands );
    }
}
