/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import java.util.function.Supplier;

import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * This class is used for breaking up circular dependencies between clustering and kernel components.
 * Having the problem contained in a single class makes it easier to manage, but ideally we would get
 * rid of it entirely.
 *
 * TODO: Try to get rid of this class altogether!
 */
public class CoreKernelResolvers
{
    private Database database;

    public void registerDatabase( Database database )
    {
        this.database = database;
    }

    public Supplier<StorageEngine> storageEngine()
    {
        return () -> database.getDependencyResolver().resolveDependency( StorageEngine.class );
    }

    public Supplier<TransactionIdStore> txIdStore()
    {
        return () -> database.getDependencyResolver().resolveDependency( TransactionIdStore.class );
    }

    public Supplier<LogicalTransactionStore> txStore()
    {
        return () -> database.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
    }

    public Supplier<IdGeneratorFactory> idGeneratorFactory()
    {
        return () -> database.getDependencyResolver().resolveDependency( IdGeneratorFactory.class );
    }
}
