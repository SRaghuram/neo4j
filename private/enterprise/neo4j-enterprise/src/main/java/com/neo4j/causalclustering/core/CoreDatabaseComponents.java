/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.state.CoreEditionKernelComponents;

import java.util.function.Function;

import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.token.TokenHolders;

public class CoreDatabaseComponents implements EditionDatabaseComponents
{
    private final AbstractEditionModule editionModule;
    private final CoreEditionKernelComponents kernelComponents;
    private final DatabaseTransactionStats transactionMonitor;

    CoreDatabaseComponents( AbstractEditionModule editionModule, CoreEditionKernelComponents kernelComponents )
    {
        this.editionModule = editionModule;
        this.kernelComponents = kernelComponents;
        this.transactionMonitor = editionModule.createTransactionMonitor();
    }

    @Override
    public DatabaseIdContext getIdContext()
    {
        return kernelComponents.idContext();
    }

    @Override
    public TokenHolders getTokenHolders()
    {
        return kernelComponents.tokenHolders();
    }

    @Override
    public Function<DatabaseLayout,DatabaseLayoutWatcher> getWatcherServiceFactory()
    {
        return editionModule.getWatcherServiceFactory();
    }

    @Override
    public IOLimiter getIoLimiter()
    {
        return editionModule.getIoLimiter();
    }

    @Override
    public ConstraintSemantics getConstraintSemantics()
    {
        return editionModule.getConstraintSemantics();
    }

    @Override
    public CommitProcessFactory getCommitProcessFactory()
    {
        return kernelComponents.commitProcessFactory();
    }

    @Override
    public Locks getLocks()
    {
        return kernelComponents.lockManager();
    }

    @Override
    public DatabaseTransactionStats getTransactionMonitor()
    {
        return transactionMonitor;
    }

    @Override
    public QueryEngineProvider getQueryEngineProvider()
    {
        return editionModule.getQueryEngineProvider();
    }

    @Override
    public AccessCapabilityFactory getAccessCapabilityFactory()
    {
        return kernelComponents.accessCapabilityFactory();
    }

    @Override
    public DatabaseStartupController getStartupController()
    {
        return editionModule.getDatabaseStartupController();
    }
}
