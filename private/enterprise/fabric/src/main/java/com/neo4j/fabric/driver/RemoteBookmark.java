/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import java.util.Objects;

/**
 * A bookmark received after interacting with a remote graph.
 */
public class RemoteBookmark
{
    private final String serialisedState;

    public RemoteBookmark( String serialisedState )
    {
        this.serialisedState = serialisedState;
    }

    public String getSerialisedState()
    {
        return serialisedState;
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
        RemoteBookmark that = (RemoteBookmark) o;
        return serialisedState.equals( that.serialisedState );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( serialisedState );
    }
}
