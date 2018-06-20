/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom.surface;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.scheduler.JobScheduler;

/**
 * A {@link KernelExtensionFactory} for the bloom fulltext addon.
 *
 * @see BloomProcedures
 * @see BloomFulltextConfig
 */
@Service.Implementation( KernelExtensionFactory.class )
public class BloomKernelExtensionFactory extends KernelExtensionFactory<BloomKernelExtensionFactory.Dependencies>
{

    public static final String SERVICE_NAME = "bloom";
    public static final String BLOOM_RELATIONSHIPS = "bloomRelationships";
    public static final String BLOOM_NODES = "bloomNodes";

    public interface Dependencies
    {
        Config getConfig();

        GraphDatabaseService db();

        FileSystemAbstraction fileSystem();

        Procedures procedures();

        LogService logService();

        AvailabilityGuard availabilityGuard();

        JobScheduler scheduler();

        NeoStoreDataSource neoStoreDataSource();
    }

    public BloomKernelExtensionFactory()
    {
        super( SERVICE_NAME );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
    {
        FileSystemAbstraction fs = dependencies.fileSystem();
        File storeDir = context.storeDir();
        Config config = dependencies.getConfig();
        GraphDatabaseService db = dependencies.db();
        Procedures procedures = dependencies.procedures();
        LogService logService = dependencies.logService();
        AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
        JobScheduler scheduler = dependencies.scheduler();
        NeoStoreDataSource dataSource = dependencies.neoStoreDataSource();
        Supplier<TransactionIdStore> transactionIdStore = dataSourceDependency( dataSource, TransactionIdStore.class );
        Supplier<NeoStoreFileListing> fileListing = dataSourceDependency( dataSource, NeoStoreFileListing.class );
        return new BloomKernelExtension(
                fs, storeDir, config, db, procedures, logService, availabilityGuard, scheduler, transactionIdStore, fileListing );
    }

    private static <T> Supplier<T> dataSourceDependency( NeoStoreDataSource neoStoreDataSource, Class<T> clazz )
    {
        return () -> neoStoreDataSource.getDependencyResolver().resolveDependency( clazz );
    }
}
