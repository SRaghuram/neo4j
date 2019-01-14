/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.factory;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

import static java.lang.reflect.Proxy.newProxyInstance;

class HighlyAvailableCommitProcessFactory implements CommitProcessFactory
{
    private final DelegateInvocationHandler<TransactionCommitProcess> commitProcessDelegate;

    HighlyAvailableCommitProcessFactory( DelegateInvocationHandler<TransactionCommitProcess> commitProcessDelegate )
    {
        this.commitProcessDelegate = commitProcessDelegate;
    }

    @Override
    public TransactionCommitProcess create( TransactionAppender appender, StorageEngine storageEngine,
            Config config )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTransactionCommitProcess();
        }
        return (TransactionCommitProcess) newProxyInstance( TransactionCommitProcess.class.getClassLoader(),
                new Class[]{TransactionCommitProcess.class}, commitProcessDelegate );
    }
}
