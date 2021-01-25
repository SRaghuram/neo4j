/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.helper;

import java.nio.channels.ClosedChannelException;
import java.util.function.Predicate;

public class IsChannelClosedException implements Predicate<Throwable>
{
    @Override
    public boolean test( Throwable e )
    {
        if ( e == null )
        {
            return false;
        }

        if ( e instanceof ClosedChannelException )
        {
            return true;
        }

        return test( e.getCause() );
    }
}
