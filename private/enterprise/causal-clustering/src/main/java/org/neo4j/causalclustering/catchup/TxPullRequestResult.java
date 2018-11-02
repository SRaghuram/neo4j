/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

public class TxPullRequestResult
{
    private final CatchupResult catchupResult;
    private final long lastTxId;

    public TxPullRequestResult( CatchupResult catchupResult, long lastTxId )
    {
        this.catchupResult = catchupResult;
        this.lastTxId = lastTxId;
    }

    public CatchupResult catchupResult()
    {
        return catchupResult;
    }

    public long lastTxId()
    {
        return lastTxId;
    }
}
