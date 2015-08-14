package org.neo4j.store;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

public class StoreFactory extends org.neo4j.kernel.impl.store.StoreFactory
{

    public StoreFactory( Config config, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger, Monitors monitors )
    {
        super( config, idGeneratorFactory, pageCache, fileSystemAbstraction, stringLogger, monitors );
        // TODO Auto-generated constructor stub
    }
    
    public File storeFileName( String toAppend, File dir)
    {
        File returnValue = null;
        if (dir == null)
            returnValue = storeFileName (toAppend );
        else
            returnValue = new File(dir.getPath() + File.separator + "neoStore" + toAppend);
        return returnValue;
    }
    public PropertyStore newPropertyStoreWithNoKey( File dir)
    {
        return newPropertyStore( storeFileName( PROPERTY_STRINGS_STORE_NAME, dir ),
                storeFileName( PROPERTY_ARRAYS_STORE_NAME, dir ), storeFileName( PROPERTY_STORE_NAME, dir ),
                null );
    }
    public void createNodeStore(File dir)
    {
        createNodeLabelsStore(dir);
        createEmptyStore( storeFileName( NODE_STORE_NAME, dir ), buildTypeDescriptorAndVersion( NodeStore.TYPE_DESCRIPTOR
        ) );
    }
    
    @SuppressWarnings( "deprecation" )
    private void createNodeLabelsStore(File dir)
    {
        int labelStoreBlockSize = config.get( Configuration.label_block_size );
        createEmptyDynamicStore( storeFileName( NODE_LABELS_STORE_NAME, dir ), labelStoreBlockSize,
                DynamicArrayStore.VERSION, IdType.NODE_LABELS );
    }
    
    public void createRelationshipStore(File dir)
    {
        createEmptyStore( storeFileName( RELATIONSHIP_STORE_NAME, dir ),
                buildTypeDescriptorAndVersion( RelationshipStore.TYPE_DESCRIPTOR ) );
    }
    
    public void createPropertyStore( File dir, boolean noKeyStore)
    {
        if (dir != null)
        {
            try
            {
                fileSystemAbstraction.mkdirs( dir );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Unable to create directory " +
                                                      dir + " for creating a temp property store in",
                        e );
            }
        }
        createEmptyStore( storeFileName( PROPERTY_STORE_NAME, dir ),
                buildTypeDescriptorAndVersion( PropertyStore.TYPE_DESCRIPTOR ) );
        int stringStoreBlockSize = config.get( Configuration.string_block_size );
        int arrayStoreBlockSize = config.get( Configuration.array_block_size );

        if (!noKeyStore)
            createPropertyKeyTokenStore( dir );
        createDynamicStringStore( storeFileName( PROPERTY_STRINGS_STORE_NAME, dir ), stringStoreBlockSize,
                IdType.STRING_BLOCK );
        createDynamicArrayStore( storeFileName( PROPERTY_ARRAYS_STORE_NAME, dir ), arrayStoreBlockSize );
    }

    public void createPropertyKeyTokenStore( File dir )
    {
        createEmptyStore( storeFileName( PROPERTY_KEY_TOKEN_STORE_NAME, dir ),
                buildTypeDescriptorAndVersion( PropertyKeyTokenStore.TYPE_DESCRIPTOR ) );
        createDynamicStringStore( storeFileName( PROPERTY_KEY_TOKEN_NAMES_STORE_NAME, dir ),
                TokenStore.NAME_STORE_BLOCK_SIZE, IdType.PROPERTY_KEY_TOKEN_NAME );
    }
}
