/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.neo4j.logging.Log;

public class ComException extends RuntimeException
{
    public static final boolean TRACE_HA_CONNECTIVITY = Boolean.getBoolean( "org.neo4j.com.TRACE_HA_CONNECTIVITY" );

    public ComException()
    {
        super();
    }

    public ComException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public ComException( String message )
    {
        super( message );
    }

    public ComException( Throwable cause )
    {
        super( cause );
    }

    public ComException traceComException( Log log, String tracePoint )
    {
        if ( TRACE_HA_CONNECTIVITY )
        {
            String msg = String.format( "ComException@%x trace from %s: %s",
                    System.identityHashCode( this ), tracePoint, getMessage() );
            log.debug( msg, this );
        }
        return this;
    }
}
