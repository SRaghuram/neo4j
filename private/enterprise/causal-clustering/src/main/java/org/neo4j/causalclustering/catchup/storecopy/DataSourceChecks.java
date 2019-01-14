/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.neo4j.causalclustering.catchup.CheckPointerService;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.kernel.NeoStoreDataSource;

class DataSourceChecks
{
    private DataSourceChecks()
    {
    }

    static boolean hasSameStoreId( StoreId storeId, NeoStoreDataSource dataSource )
    {
        return storeId.equalToKernelStoreId( dataSource.getStoreId() );
    }

    static boolean isTransactionWithinReach( long transactionId, CheckPointerService checkPointerService )
    {
        return checkPointerService.lastCheckPointedTransactionId() >= transactionId;
    }

}
