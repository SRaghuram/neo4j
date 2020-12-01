/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import java.util.Objects;

public final class DbmsPanicEvent extends AbstractPanicEvent
{
    private final int exitCode;

    public DbmsPanicEvent( DbmsPanicReason reason, Throwable cause )
    {
        super( cause, reason );
        exitCode = reason.exitCode;
    }

    public int getExitCode()
    {
        return exitCode;
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
        if ( !super.equals( o ) )
        {
            return false;
        }
        DbmsPanicEvent event = (DbmsPanicEvent) o;
        return super.equals( event ) && exitCode == event.exitCode;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), exitCode );
    }
}
