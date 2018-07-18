/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import java.io.File;
import java.util.function.Supplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

@Service.Implementation( KernelExtensionFactory.class )
public class FulltextIndexProviderFactory extends KernelExtensionFactory<FulltextIndexProviderFactory.Dependencies>
{
    private static final String KEY = "fulltext";
    private static final int PRIORITY = 0;
    public static final IndexProvider.Descriptor DESCRIPTOR = new IndexProvider.Descriptor( KEY, "1.0" );

    public interface Dependencies
    {
        Config getConfig();

        FileSystemAbstraction fileSystem();

        NeoStoreDataSource neoStoreDataSource();

        Procedures procedures();

        LogService getLogService();
    }

    public FulltextIndexProviderFactory()
    {
        super( ExtensionType.DATABASE, KEY );
    }

    private static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File storeDir )
    {
        IndexDirectoryStructure parentDirectoryStructure = directoriesByProvider( storeDir ).forProvider( DESCRIPTOR );
        return directoriesBySubProvider( parentDirectoryStructure );
    }

    @Override
    public FulltextIndexProvider newInstance( KernelContext context, Dependencies dependencies )
    {
        Config config = dependencies.getConfig();
        boolean ephemeral = config.get( GraphDatabaseSettings.ephemeral );
        FileSystemAbstraction fileSystemAbstraction = dependencies.fileSystem();
        DirectoryFactory directoryFactory = directoryFactory( ephemeral, fileSystemAbstraction );
        OperationalMode operationalMode = context.databaseInfo().operationalMode;
        NeoStoreDataSource neoStoreDataSource = dependencies.neoStoreDataSource();
        IndexDirectoryStructure.Factory directoryStructureFactory = subProviderDirectoryStructure( context.storeDir() );
        Supplier<TokenHolders> tokenHoldersSupplier = dataSourceDependency( neoStoreDataSource, TokenHolders.class );
        Log log = dependencies.getLogService().getInternalLog( FulltextIndexProvider.class );

        FulltextIndexProvider provider = new FulltextIndexProvider(
                DESCRIPTOR, PRIORITY, directoryStructureFactory, fileSystemAbstraction, config, tokenHoldersSupplier,
                directoryFactory, operationalMode );
        dependencies.procedures().registerComponent( FulltextAdapter.class, procContext -> provider, true );
        try
        {
            dependencies.procedures().registerProcedure( FulltextProcedures.class );
        }
        catch ( KernelException e )
        {
            log.error( "Failed to register the fulltext index procedures. The fulltext index provider will be loaded and updated like normal, " +
                    "but it might not be possible to query any fulltext indexes.", e );
        }

        return provider;
    }

    private static <T> Supplier<T> dataSourceDependency( NeoStoreDataSource neoStoreDataSource, Class<T> clazz )
    {
        return () -> neoStoreDataSource.getDependencyResolver().resolveDependency( clazz );
    }
}
