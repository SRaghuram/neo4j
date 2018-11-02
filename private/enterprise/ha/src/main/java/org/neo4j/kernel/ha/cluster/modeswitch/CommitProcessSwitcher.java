/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster.modeswitch;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.MasterTransactionCommitProcess;
import org.neo4j.kernel.ha.SlaveTransactionCommitProcess;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngine;

public class CommitProcessSwitcher extends AbstractComponentSwitcher<TransactionCommitProcess>
{
    private final TransactionPropagator txPropagator;
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private final DependencyResolver dependencyResolver;
    private final MasterTransactionCommitProcess.Monitor monitor;
    private final String activeDatabaseName;

    public CommitProcessSwitcher( TransactionPropagator txPropagator, Master master, DelegateInvocationHandler<TransactionCommitProcess> delegate,
            RequestContextFactory requestContextFactory, Monitors monitors, DependencyResolver dependencyResolver, Config config )
    {
        super( delegate );
        this.txPropagator = txPropagator;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.dependencyResolver = dependencyResolver;
        this.monitor = monitors.newMonitor( MasterTransactionCommitProcess.Monitor.class );
        this.activeDatabaseName = config.get( GraphDatabaseSettings.active_database );
    }

    @Override
    protected TransactionCommitProcess getSlaveImpl()
    {
        return new SlaveTransactionCommitProcess( master, requestContextFactory );
    }

    @Override
    protected TransactionCommitProcess getMasterImpl()
    {
        GraphDatabaseFacade databaseFacade = this.dependencyResolver.resolveDependency( DatabaseManager.class )
                .getDatabaseFacade( activeDatabaseName ).get();
        DependencyResolver databaseResolver = databaseFacade.getDependencyResolver();
        TransactionCommitProcess commitProcess = new TransactionRepresentationCommitProcess(
                databaseResolver.resolveDependency( TransactionAppender.class ),
                databaseResolver.resolveDependency( StorageEngine.class ) );

        IntegrityValidator validator = databaseResolver.resolveDependency( IntegrityValidator.class );
        return new MasterTransactionCommitProcess( commitProcess, txPropagator, validator, monitor );
    }
}
