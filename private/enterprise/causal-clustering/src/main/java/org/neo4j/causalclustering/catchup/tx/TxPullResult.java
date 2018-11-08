/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.tx;

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients;
import org.neo4j.causalclustering.identity.StoreId;


/**
 * Wrapper type for the outcome of a {@link VersionedCatchupClients.CatchupClientV2#pullTransactions(StoreId, long, String)} call (or V1).
 * TODO: Very similar to TxStreamFinishedResponse and so could potentially be factored away in future, by refactoring the adaptor in TxPullClient
 */
public class TxPullResult
{
    private final CatchupResult catchupResult;
    private final long lastTxId;

    public TxPullResult( CatchupResult catchupResult, long lastTxId )
    {
        this.catchupResult = catchupResult;
        this.lastTxId = lastTxId;
    }

    public CatchupResult status()
    {
        return catchupResult;
    }

    public long lastTxId()
    {
        return lastTxId;
    }
}
