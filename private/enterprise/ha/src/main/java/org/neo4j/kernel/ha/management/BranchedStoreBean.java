/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.management;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.BranchedStoreInfo;

import static org.neo4j.com.storecopy.StoreUtil.getBranchedDataRootDirectory;

@Service.Implementation( ManagementBeanProvider.class )
public final class BranchedStoreBean extends ManagementBeanProvider
{
    @SuppressWarnings( "WeakerAccess" ) // Bean needs public constructor
    public BranchedStoreBean()
    {
        super( BranchedStore.class );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        if ( !isHA( management ) )
        {
            return null;
        }
        return new BranchedStoreImpl( management, true );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management )
            throws NotCompliantMBeanException
    {
        if ( !isHA( management ) )
        {
            return null;
        }
        return new BranchedStoreImpl( management );
    }

    private static boolean isHA( ManagementData management )
    {
        return OperationalMode.ha == management.resolveDependency( DatabaseInfo.class ).operationalMode;
    }

    private static class BranchedStoreImpl extends Neo4jMBean implements BranchedStore
    {
        private final FileSystemAbstraction fileSystem;
        private final PageCache pageCache;
        private KernelData kernelData;

        BranchedStoreImpl( final ManagementData management )
        {
            this( management, false );
        }

        BranchedStoreImpl( final ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );
            kernelData = management.getKernelData();
            fileSystem = getFilesystem( kernelData );
            pageCache = getPageCache( kernelData );
        }

        @Override
        public BranchedStoreInfo[] getBranchedStores()
        {
            DatabaseLayout databaseLayout = kernelData.getDataSourceManager().getDataSource().getDatabaseLayout();
            File databaseDirectory = databaseLayout.databaseDirectory();
            if ( databaseDirectory == null )
            {
                return new BranchedStoreInfo[0];
            }

            List<BranchedStoreInfo> toReturn = new LinkedList<>();

            File[] files = fileSystem.listFiles( getBranchedDataRootDirectory( databaseDirectory ) );
            if ( files != null )
            {
                for ( File branchDirectory : files )
                {
                    if ( !branchDirectory.isDirectory() )
                    {
                        continue;
                    }
                    toReturn.add( parseBranchedStore( branchDirectory ) );
                }
            }
            return toReturn.toArray( new BranchedStoreInfo[toReturn.size()] );
        }

        private BranchedStoreInfo parseBranchedStore( File branchedDatabase )
        {
            try
            {
                File neoStoreFile = DatabaseLayout.of( branchedDatabase ).metadataStore();
                long txId = MetaDataStore.getRecord( pageCache, neoStoreFile, Position.LAST_TRANSACTION_ID );
                long timestamp = Long.parseLong( branchedDatabase.getName() );
                long branchedStoreSize = FileUtils.size( fileSystem, branchedDatabase );

                return new BranchedStoreInfo( branchedDatabase.getName(), txId, timestamp, branchedStoreSize );
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( "Cannot read branched neostore", e );
            }
        }

        private static PageCache getPageCache( KernelData kernelData )
        {
            return kernelData.getPageCache();
        }

        private static FileSystemAbstraction getFilesystem( KernelData kernelData )
        {
            return kernelData.getFilesystemAbstraction();
        }
    }
}
