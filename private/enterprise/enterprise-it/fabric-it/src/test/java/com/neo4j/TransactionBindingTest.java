/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.bolt.BoltFabricDatabaseManagementService;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.transaction.TransactionManager;
import com.neo4j.utils.CustomFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext.AUTH_DISABLED;
import static java.time.Duration.ZERO;
import static org.neo4j.bolt.runtime.AccessMode.WRITE;

class TransactionBindingTest
{
    private TestServer testServer;
    private ExecutorService thread1 = Executors.newSingleThreadExecutor();
    private ExecutorService thread2 = Executors.newSingleThreadExecutor();

    @BeforeEach
    void setUp() throws KernelException
    {

        PortUtils.Ports ports = PortUtils.findFreePorts();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "neo4j://somewhere:1234",
                "fabric.routing.servers", "localhost:" + ports.bolt,
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
                "dbms.connector.bolt.enabled", "true"
        );

        var config = Config.newBuilder()
                .setRaw( configProperties )
                .build();
        testServer = new TestServer( config );

        testServer.start();

        testServer.getDependencies().resolveDependency( GlobalProceduresRegistry.class )
                .registerFunction( CustomFunctions.class );
    }

    @AfterEach
    void tearDown()
    {
        testServer.stop();
        thread1.shutdownNow();
        thread2.shutdownNow();
    }

    @Test
    void testTransactionBinding() throws Exception
    {
        // Each Bolt message (RUN, PULL) can be processed by a different thread and a transaction must be bound to a thread
        // that tries to execute a query in them.
        // So the pattern for using local transaction is like this:
        // <ENTER FROM BOLT>
        // <BIND>
        // <EXECUTE>
        // <EXECUTE>
        // ...
        // <UNBIND>
        // <RETURN TO BOLT>

        // this test checks that local transactions really work like that

        var dependencies = testServer.getDependencies();
        var fabricExecutor = dependencies.resolveDependency( FabricExecutor.class );
        var transactionManager = dependencies.resolveDependency( TransactionManager.class );
        var fabricDatabaseManager = dependencies.resolveDependency( FabricDatabaseManager.class );
        var config = dependencies.resolveDependency( FabricConfig.class );

        var fabricDatabaseManagementService = new BoltFabricDatabaseManagementService( fabricExecutor, config, transactionManager, fabricDatabaseManager );

        var boltDatabase = fabricDatabaseManagementService.database( "mega" );

        var tx = boltDatabase.beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED, null, ZERO, WRITE, Map.of() );

        thread1.submit( () -> executeQuery( tx ) ).get();
        thread2.submit( () -> executeQuery( tx ) ).get();

        // no checks needed, the kernel would throw an exception if we tried to execute a query on a thread with no transaction bound to it.
    }

    private void executeQuery( BoltTransaction tx )
    {
        tx.bindToCurrentThread();

        CompletableFuture<Void> future = new CompletableFuture<>();

        try
        {

            var queryExecution = tx.executeQuery( "Return 1", MapValue.EMPTY, true, new QuerySubscriber()
            {
                @Override
                public void onResult( int numberOfFields )
                {

                }

                @Override
                public void onRecord()
                {

                }

                @Override
                public void onField( AnyValue value )
                {

                }

                @Override
                public void onRecordCompleted()
                {

                }

                @Override
                public void onError( Throwable throwable )
                {
                    future.completeExceptionally( throwable );
                }

                @Override
                public void onResultCompleted( QueryStatistics statistics )
                {
                    future.complete( null );
                }
            } ).getQueryExecution();

            queryExecution.request( Long.MAX_VALUE );
            future.get();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            tx.unbindFromCurrentThread();
        }
    }
}
