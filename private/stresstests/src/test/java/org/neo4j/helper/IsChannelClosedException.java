/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.helper;

import java.nio.channels.ClosedChannelException;
import java.util.function.Predicate;

import org.neo4j.com.ComException;

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

        if ( e instanceof ComException && e.getMessage() != null &&
                e.getMessage().startsWith( "Channel has been closed" ) )
        {
            return true;
        }

        return test( e.getCause() );
    }
}
