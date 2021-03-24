/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database.controller;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.io.Flushable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.kernel.impl.pagecache.IOControllerService;
import org.neo4j.service.PrioritizedService;
import org.neo4j.service.Services;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.SystemNanoClock;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_iops_limit;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class EnterpriseIoControllerReportingIT
{
    private static final int MARKER = -2;
    @Inject
    private IOController ioController;
    @Inject
    private GraphDatabaseService databaseService;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( check_point_iops_limit, MARKER );
    }

    @Test
    void checkpointObserveOngoingIO()
    {
        assertThat( ioController ).isInstanceOf( MonitoringController.class );

        var controller = MonitoringController.MONITORING_CONTROLLER;
        var completed = new AtomicBoolean();
        while ( !completed.get() )
        {
            try ( Transaction transaction = databaseService.beginTx() )
            {
                Node node = transaction.createNode();
                node.setProperty( "a", randomAlphabetic( (int) ByteUnit.mebiBytes( 1 ) ) );
                transaction.commit();
            }
            if ( controller.getExternalIOs() > 0 )
            {
                completed.set( true );
            }
        }
    }

    @ServiceProvider
    public static class MonitoredIOControllerService implements IOControllerService
    {
        @Override
        public IOController createIOController( Config config, SystemNanoClock clock )
        {
            if ( config.get( check_point_iops_limit ) == MARKER )
            {
                return MonitoringController.MONITORING_CONTROLLER;
            }
            final List<IOControllerService> allServices = (List<IOControllerService>) Services.loadAll( IOControllerService.class );
            allServices.sort( Comparator.comparingInt( PrioritizedService::getPriority ) );
            return allServices.get( 1 ).createIOController( config, clock );
        }

        @Override
        public int getPriority()
        {
            return 5;
        }
    }

    public static class MonitoringController implements IOController
    {
        public static final MonitoringController MONITORING_CONTROLLER = new MonitoringController();
        private final AtomicLong externalIOs = new AtomicLong();

        private MonitoringController()
        {
        }

        @Override
        public void maybeLimitIO( int recentlyCompletedIOs, Flushable flushable, FlushEventOpportunity flushes )
        {

        }

        @Override
        public void reportIO( int completedIOs )
        {
            externalIOs.addAndGet( completedIOs );
        }

        public long getExternalIOs()
        {
            return externalIOs.get();
        }
    }
}
