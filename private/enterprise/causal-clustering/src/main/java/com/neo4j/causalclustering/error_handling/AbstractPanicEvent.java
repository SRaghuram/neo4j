/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import java.util.Objects;

public abstract class AbstractPanicEvent implements Panicker.PanicEvent
{
    protected final Throwable cause;
    protected final Panicker.Reason reason;

    protected AbstractPanicEvent( Throwable cause, Panicker.Reason reason )
    {
        this.cause = cause;
        this.reason = reason;
    }

    @Override
    public Throwable getCause()
    {
        return cause;
    }

    @Override
    public Panicker.Reason getReason()
    {
        return reason;
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
        AbstractPanicEvent that = (AbstractPanicEvent) o;
        return cause.equals( that.cause ) &&
               reason.equals( that.reason );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( cause, reason );
    }
}
