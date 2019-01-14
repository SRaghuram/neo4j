/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import org.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import org.neo4j.causalclustering.identity.MemberId;

public class CatchupAddressResolutionException extends TopologyLookupException
{
    public CatchupAddressResolutionException( MemberId memberId )
    {
        super( memberId );
    }

    CatchupAddressResolutionException( Exception e )
    {
        super( e );
    }
}
