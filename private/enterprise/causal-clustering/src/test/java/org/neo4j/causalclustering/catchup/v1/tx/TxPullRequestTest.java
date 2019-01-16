/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v1.tx;

import com.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import com.neo4j.causalclustering.identity.StoreId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

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
        return new TxPullRequest( correctTxId, StoreId.DEFAULT, DEFAULT_DATABASE_NAME );
    }
}
