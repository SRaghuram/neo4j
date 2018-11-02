/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import org.neo4j.causalclustering.identity.MemberId;

import static java.lang.String.format;

public class TopologyLookupException extends Exception
{
    public TopologyLookupException( Throwable cause )
    {
        super( cause );
    }

    public TopologyLookupException( MemberId memberId )
    {
        super( format( "Cannot find the target member %s socket address", memberId ) );
    }
}
