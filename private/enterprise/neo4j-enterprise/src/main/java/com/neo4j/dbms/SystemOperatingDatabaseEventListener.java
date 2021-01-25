/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventListener;

class SystemOperatingDatabaseEventListener implements ReplicatedDatabaseEventListener
{
    private final SystemGraphDbmsOperator systemOperator;

    SystemOperatingDatabaseEventListener( SystemGraphDbmsOperator systemOperator )
    {
        this.systemOperator = systemOperator;
    }

    @Override
    public void transactionCommitted( long txId )
    {
        systemOperator.transactionCommitted( txId, null );
    }

    @Override
    public void storeReplaced( long txId )
    {
        systemOperator.storeReplaced( txId );
    }
}
