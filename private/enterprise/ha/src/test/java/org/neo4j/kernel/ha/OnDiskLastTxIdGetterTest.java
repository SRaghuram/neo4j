/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.function.LongSupplier;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;

public class OnDiskLastTxIdGetterTest
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void testGetLastTxIdNoFilePresent()
    {
        // This is a sign that we have some bad coupling on our hands.
        // We currently have to do this because of our lifecycle and construction ordering.
        OnDiskLastTxIdGetter getter = new OnDiskLastTxIdGetter( () -> 13L );
        assertEquals( 13L, getter.getLastTxId() );
    }

    @Test
    public void lastTransactionIdIsBaseTxIdWhileNeoStoresAreStopped()
    {
        final StoreFactory storeFactory = new StoreFactory( DatabaseLayout.of( new File( "store" ) ),
                Config.defaults(), new DefaultIdGeneratorFactory( fs.get() ), pageCacheRule.getPageCache( fs.get() ), fs.get(),
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        final NeoStores neoStores = storeFactory.openAllNeoStores( true );
        neoStores.close();

        LongSupplier supplier = () -> neoStores.getMetaDataStore().getLastCommittedTransactionId();
        OnDiskLastTxIdGetter diskLastTxIdGetter = new OnDiskLastTxIdGetter( supplier );
        assertEquals( TransactionIdStore.BASE_TX_ID, diskLastTxIdGetter.getLastTxId() );
    }
}
