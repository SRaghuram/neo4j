/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.helper;

import java.io.IOException;
import java.util.function.Predicate;

public class IsConnectionResetByPeer implements Predicate<Throwable>
{
    @Override
    public boolean test( Throwable e )
    {
        if ( e == null )
        {
            return false;
        }

        if ( e instanceof IOException && e.getMessage() != null &&
                e.getMessage().startsWith( "Connection reset by peer" ) )
        {
            return true;
        }

        return test( e.getCause() );
    }
}
