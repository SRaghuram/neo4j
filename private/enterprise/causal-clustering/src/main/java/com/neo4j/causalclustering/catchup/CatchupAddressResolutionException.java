/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;

import org.neo4j.dbms.identity.ServerId;

public class CatchupAddressResolutionException extends TopologyLookupException
{
    public CatchupAddressResolutionException( ServerId serverId )
    {
        super( serverId );
    }

    CatchupAddressResolutionException( Exception e )
    {
        super( e );
    }
}
