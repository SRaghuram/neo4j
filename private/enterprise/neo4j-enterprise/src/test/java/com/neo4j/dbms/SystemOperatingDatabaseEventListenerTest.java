/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SystemOperatingDatabaseEventListenerTest
{
    private SystemGraphDbmsOperator systemOperator = mock( SystemGraphDbmsOperator.class );
    private SystemOperatingDatabaseEventListener listener = new SystemOperatingDatabaseEventListener( systemOperator );

    @Test
    void transactionCommitted()
    {
        listener.transactionCommitted( 17 );
        verify( systemOperator ).transactionCommitted( 17, null );
    }

    @Test
    void storeReplaced()
    {
        listener.storeReplaced( 17 );
        verify( systemOperator ).storeReplaced( 17 );
    }
}
