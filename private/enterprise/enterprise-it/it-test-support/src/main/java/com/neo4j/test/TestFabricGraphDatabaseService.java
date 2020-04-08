package com.neo4j.test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;

import static java.util.Objects.requireNonNull;

public class TestFabricGraphDatabaseService extends GraphDatabaseFacade
{
    final BoltGraphDatabaseServiceSPI boltFabricDatabaseService;
    final Config config;

    public TestFabricGraphDatabaseService( GraphDatabaseFacade baseDb,
                                           BoltGraphDatabaseServiceSPI boltFabricDatabaseService,
                                           Config config )
    {
        super( baseDb, Function.identity() );
        this.boltFabricDatabaseService = boltFabricDatabaseService;
        this.config = requireNonNull( config );
    }

    @Override
    protected InternalTransaction beginTransactionInternal( KernelTransaction.Type type,
                                                            LoginContext loginContext,
                                                            ClientConnectionInfo connectionInfo,
                                                            long timeoutMillis )
    {
        return new TestFabricTransaction(
                contextFactory,
                boltFabricDatabaseService.beginTransaction( type, loginContext, connectionInfo, List.of(),
                                                            Duration.ofMillis( timeoutMillis ), AccessMode.WRITE, Map.of() )
        );
    }
}
