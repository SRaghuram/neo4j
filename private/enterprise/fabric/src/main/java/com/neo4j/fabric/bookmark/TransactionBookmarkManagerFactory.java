/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bookmark;

import com.neo4j.fabric.localdb.FabricDatabaseManager;

/**
 * The main purpose of this factory is to make bookmark logic testable.
 */
public class TransactionBookmarkManagerFactory
{
    private FabricDatabaseManager fabricDatabaseManager;

    public TransactionBookmarkManagerFactory( FabricDatabaseManager fabricDatabaseManager )
    {
        this.fabricDatabaseManager = fabricDatabaseManager;
    }

    public TransactionBookmarkManager createTransactionBookmarkManager( LocalGraphTransactionIdTracker transactionIdTracker )
    {
        // TODO: We will get fabric only bookmarks only when system database supports Fabric
//        if ( fabricDatabaseManager.multiGraphCapabilitiesEnabledForAllDatabases() )
//        {
//            return new FabricOnlyBookmarkManager( transactionIdTracker );
//        }
//        else
//        {
//            return new MixedModeBookmarkManager( transactionIdTracker );
//        }
        return new MixedModeBookmarkManager( transactionIdTracker );
    }
}
