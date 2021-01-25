/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.util.Optional;

class CommitState
{
    private final long metaDataStoreIndex;
    private final Long transactionLogIndex;

    CommitState( long metaDataStoreIndex )
    {
        this.metaDataStoreIndex = metaDataStoreIndex;
        this.transactionLogIndex = null;
    }

    CommitState( long metaDataStoreIndex, long transactionLogIndex )
    {
        assert transactionLogIndex >= metaDataStoreIndex;

        this.metaDataStoreIndex = metaDataStoreIndex;
        this.transactionLogIndex = transactionLogIndex;
    }

    long metaDataStoreIndex()
    {
        return metaDataStoreIndex;
    }

    Optional<Long> transactionLogIndex()
    {
        return Optional.ofNullable( transactionLogIndex );
    }

    @Override
    public String toString()
    {
        return "CommitState{" + "metaDataStoreIndex=" + metaDataStoreIndex + ", transactionLogIndex=" + transactionLogIndex + '}';
    }
}
