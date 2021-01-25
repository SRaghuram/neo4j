/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

class TxPullRequestTest
{
    @ParameterizedTest
    @ValueSource( longs = {Long.MIN_VALUE, BASE_TX_ID - 999, BASE_TX_ID - 42, BASE_TX_ID - 2, BASE_TX_ID - 1} )
    void shouldFailWhenTransactionIdIsWrong( long wrongTxId )
    {
        assertThrows( IllegalArgumentException.class, () -> newTxPullRequest( wrongTxId ) );
    }

    @ParameterizedTest
    @ValueSource( longs = {BASE_TX_ID, BASE_TX_ID + 1, BASE_TX_ID + 42, BASE_TX_ID + 999, Long.MAX_VALUE} )
    void shouldAllowCorrectTransactionIds( long correctTxId )
    {
        assertDoesNotThrow( () -> newTxPullRequest( correctTxId ) );
    }

    private static TxPullRequest newTxPullRequest( long correctTxId )
    {
        return new TxPullRequest( correctTxId, StoreId.UNKNOWN, randomNamedDatabaseId().databaseId() );
    }
}
