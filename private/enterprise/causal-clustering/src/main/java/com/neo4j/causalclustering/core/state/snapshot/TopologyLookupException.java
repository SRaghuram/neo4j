/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import org.neo4j.dbms.identity.ServerId;

import static java.lang.String.format;

public class TopologyLookupException extends Exception
{
    public TopologyLookupException( Throwable cause )
    {
        super( cause );
    }

    public TopologyLookupException( ServerId serverId )
    {
        super( format( "Cannot find the target server %s socket address", serverId ) );
    }
}
