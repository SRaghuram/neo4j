/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.function.LongSupplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityGuardInstaller;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.factory.HighlyAvailableEditionModule;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class UpdatePullerTriggersPageTransactionTrackingIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule();
    private final Label NODE_LABEL = Label.label( "mark" );
    private final TestTransactionVersionContextSupplier contextSupplier = new TestTransactionVersionContextSupplier();
    private ClusterManager.ManagedCluster cluster;

    @Before
    public void setup() throws Exception
    {
        CustomGraphDatabaseFactory customGraphDatabaseFactory = new CustomGraphDatabaseFactory();
        cluster = clusterRule.withSharedSetting( GraphDatabaseSettings.snapshot_query, TRUE )
                .withDbFactory( customGraphDatabaseFactory )
                .startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        for ( int i = 0; i < 3; i++ )
        {
            try ( Transaction tx = master.beginTx() )
            {
                master.createNode( NODE_LABEL );
                tx.success();
            }
        }
        cluster.sync();
    }

    @Test
    public void updatePullerTriggerPageTransactionTracking()
    {
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        TransactionIdStore slaveTransactionIdStore =
                slave.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        assertEquals( 5, slaveTransactionIdStore.getLastClosedTransactionId() );

        ByzantineLongSupplier byzantineIdSupplier = contextSupplier.getByzantineIdSupplier();
        byzantineIdSupplier.useWrongTxId();
        try ( Transaction ignored = slave.beginTx() )
        {
            slave.execute( "match (n) return n" );
        }
        catch ( QueryExecutionException executionException )
        {
            assertEquals( "Unable to get clean data snapshot for query 'match (n) return n' after 5 attempts.", executionException.getMessage());
        }
        byzantineIdSupplier.useCorrectTxId();
        slave.execute( "match (n) return n" ).close();
    }

    private class CustomGraphDatabaseFactory extends TestHighlyAvailableGraphDatabaseFactory
    {
        @Override
        protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator( File storeDir,
                GraphDatabaseFactoryState state )
        {
            return new GraphDatabaseBuilder.DatabaseCreator()
            {
                @Override
                public GraphDatabaseService newDatabase( Config config )
                {
                    config.augment( stringMap( "unsupported.dbms.ephemeral", "false" ) );
                    return new CustomHighlyAvailableGraphDatabase( storeDir,
                            config, state.databaseDependencies() ) ;
                }
            };
        }
    }

    private class CustomHighlyAvailableGraphDatabase extends HighlyAvailableGraphDatabase
    {

        CustomHighlyAvailableGraphDatabase( File storeDir, Config config,
                GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            super( storeDir, config, dependencies );
        }

        @Override
        protected GraphDatabaseFacadeFactory newHighlyAvailableFacadeFactory( AvailabilityGuardInstaller guardInstaller )
        {
            return new CustomFacadeFactory( this, guardInstaller );
        }
    }

    private class CustomFacadeFactory extends GraphDatabaseFacadeFactory
    {
        CustomFacadeFactory( CustomHighlyAvailableGraphDatabase customHighlyAvailableGraphDatabase, AvailabilityGuardInstaller guardInstaller )
        {
            super( DatabaseInfo.HA, platformModule ->
            {
                customHighlyAvailableGraphDatabase.module = new HighlyAvailableEditionModule( platformModule );
                AvailabilityGuard guard = customHighlyAvailableGraphDatabase.module
                        .getGlobalAvailabilityGuard( platformModule.clock, platformModule.logging, platformModule.config );
                guardInstaller.install( guard );
                return customHighlyAvailableGraphDatabase.module;
            } );
        }

        @Override
        public GraphDatabaseFacade newFacade( File storeDir, Config config, Dependencies dependencies )
        {
            return initFacade( storeDir, config, dependencies, new HighlyAvailableGraphDatabase( storeDir, config, dependencies ) );
        }

        @Override
        protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies )
        {
            return new PlatformModule( storeDir, config, databaseInfo, dependencies )
            {
                @Override
                protected VersionContextSupplier createCursorContextSupplier( Config config )
                {
                    return contextSupplier;
                }
            };
        }
    }

    private class TestTransactionVersionContextSupplier extends TransactionVersionContextSupplier
    {

        private volatile ByzantineLongSupplier byzantineLongSupplier;

        @Override
        public void init( LongSupplier lastClosedTransactionIdSupplier )
        {
            byzantineLongSupplier = new ByzantineLongSupplier( lastClosedTransactionIdSupplier );
            super.init( byzantineLongSupplier );
        }

        @Override
        public VersionContext getVersionContext()
        {
            return super.getVersionContext();
        }

        ByzantineLongSupplier getByzantineIdSupplier()
        {
            return byzantineLongSupplier;
        }
    }

    private class ByzantineLongSupplier implements LongSupplier
    {

        private volatile boolean wrongTxId;
        private final LongSupplier originalIdSupplier;

        ByzantineLongSupplier( LongSupplier originalIdSupplier )
        {
            this.originalIdSupplier = originalIdSupplier;
        }

        @Override
        public long getAsLong()
        {
            return wrongTxId ? 1 : originalIdSupplier.getAsLong();
        }

        void useWrongTxId()
        {
            wrongTxId = true;
        }

        void useCorrectTxId()
        {
            wrongTxId = false;
        }
    }
}
