/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.status;

import java.util.Objects;

public class Status
{
    public enum Message
    {
        OK
    }
    public final Message message;

    public Status( Message message )
    {
        this.message = message;
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
        Status that = (Status) o;
        return Objects.equals( message, that.message );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( message );
    }

    @Override
    public String toString()
    {
        return "StateStatus{" +
               "statusMessage='" + message + '\'' +
               '}';
    }
}
