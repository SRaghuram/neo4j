package org.neo4j.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.kernel.impl.store.id.IdGenerator;

public class StoreFactory extends org.neo4j.kernel.impl.store.StoreFactory
{
	private Config config;
	private IdGeneratorFactory idGeneratorFactory;
    protected FileSystemAbstraction fileSystemAbstraction;
    private LogProvider logProvider;
    protected File neoStoreFileName;
    private PageCache pageCache;
    public StoreFactory( File storeDir, Config config, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction, LogProvider logProvider )
    {
        super( storeDir, config, idGeneratorFactory, pageCache, fileSystemAbstraction, logProvider );
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.neoStoreFileName = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        this.pageCache = pageCache;
        this.logProvider = logProvider;
    }
    
    public File storeFileName( String toAppend, File dir)
    {
        File returnValue = null;
        if (dir == null)
            returnValue = new File( neoStoreFileName.getPath() + toAppend );//storeFileName (toAppend );
        else
            returnValue = new File(dir.getPath() + File.separator + "neoStore" + toAppend);
        return returnValue;
    }
    public File storeFileName( String toAppend)
    {
        File returnValue = new File( neoStoreFileName.getPath() + toAppend );//storeFileName (toAppend );     
        return returnValue;
    }
    /*public PropertyStore newPropertyStoreWithNoKey( File dir)
    {
        return newPropertyStore( storeFileName( PROPERTY_STRINGS_STORE_NAME, dir ),
                storeFileName( PROPERTY_ARRAYS_STORE_NAME, dir ), storeFileName( PROPERTY_STORE_NAME, dir ),
                null );
    }*/
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
                DynamicArrayStore.ALL_STORES_VERSION, IdType.NODE_LABELS );
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
    
    public static String buildTypeDescriptorAndVersion( String typeDescriptor )
    {
        return typeDescriptor + " " + CommonAbstractStore.ALL_STORES_VERSION;
    }
    //-------------------
    public PropertyStore newPropertyStoreWithNoKeys( File dir )
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
        return newPropertyStore( storeFileName( PROPERTY_STRINGS_STORE_NAME, dir ),
        		storeFileName( PROPERTY_ARRAYS_STORE_NAME, dir ), 
                storeFileName( PROPERTY_STORE_NAME, dir ),
                null );
    }

    @SuppressWarnings( "deprecation" )
    protected PropertyStore newPropertyStore( File propertyStringStore, File propertyArrayStore, File propertyStore,
                                            File propertyKeysStore )
    {
        PropertyKeyTokenStore propertyKeyTokenStore = propertyKeysStore == null ? null :newPropertyKeyTokenStore( propertyKeysStore );
        DynamicStringStore stringPropertyStore = newDynamicStringStore( propertyStringStore, IdType.STRING_BLOCK );
        DynamicArrayStore arrayPropertyStore = newDynamicArrayStore( propertyArrayStore, IdType.ARRAY_BLOCK );
        PropertyStore propStore =new PropertyStore( propertyStore, config, idGeneratorFactory,
                pageCache, logProvider, stringPropertyStore, propertyKeyTokenStore,
                arrayPropertyStore );
        propStore.initialise(true);
        return propStore;
    }

    public PropertyKeyTokenStore newPropertyKeyTokenStore()
    {
        return newPropertyKeyTokenStore( storeFileName( PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),
                storeFileName( PROPERTY_KEY_TOKEN_STORE_NAME ) );
    }

    public PropertyKeyTokenStore newPropertyKeyTokenStore( File baseFile )
    {
        return newPropertyKeyTokenStore( new File( baseFile.getPath() + KEYS_PART ), baseFile );
    }

    @SuppressWarnings( "deprecation" )
    private PropertyKeyTokenStore newPropertyKeyTokenStore( File propertyKeyTokenNamesStore,
                                                            File propertyKeyTokenStore )
    {
        DynamicStringStore nameStore = newDynamicStringStore( propertyKeyTokenNamesStore,
                IdType.PROPERTY_KEY_TOKEN_NAME );
        PropertyKeyTokenStore propertyKeyStore = 
        		new PropertyKeyTokenStore( propertyKeyTokenStore, config,
                idGeneratorFactory, pageCache, logProvider, nameStore
                );
        propertyKeyStore.initialise(true);
        return propertyKeyStore;
    }
    public DynamicStringStore newDynamicStringStore( File fileName,
            @SuppressWarnings( "deprecation" ) IdType nameIdType )
    {
    	DynamicStringStore dynamicStringStore = new DynamicStringStore( fileName, config, nameIdType, idGeneratorFactory, pageCache,
    			logProvider, config.get( Configuration.string_block_size ) );
    	dynamicStringStore.initialise(true);
    	return dynamicStringStore;
    }
    public DynamicArrayStore newDynamicArrayStore( File fileName, @SuppressWarnings( "deprecation" ) IdType idType )
    {
    	DynamicArrayStore dynamicArrayStore = new DynamicArrayStore( fileName, config, idType, idGeneratorFactory, pageCache,
                logProvider, config.get( Configuration.array_block_size ) );
    	dynamicArrayStore.initialise(true);
    	return dynamicArrayStore;
    }
    public void createDynamicStringStore( File fileName, int blockSize,
            @SuppressWarnings( "deprecation" ) IdType idType )
    {
    	createEmptyDynamicStore( fileName, blockSize, DynamicStringStore.ALL_STORES_VERSION, idType );
    }
    @SuppressWarnings( "deprecation" )
    public void createDynamicArrayStore( File fileName, int blockSize )
    {
        createEmptyDynamicStore( fileName, blockSize, DynamicArrayStore.ALL_STORES_VERSION, IdType.ARRAY_BLOCK );
    }
    /**
     * Creates a new empty store. A factory method returning an implementation
     * should make use of this method to initialize an empty store. Block size
     * must be greater than zero. Not that the first block will be marked as
     * reserved (contains info about the block size). There will be an overhead
     * for each block of <CODE>AbstractDynamicStore.BLOCK_HEADER_SIZE</CODE>
     * bytes.
     * <p>
     * This method will create a empty store with descriptor returned by the
     * {@link CommonAbstractStore#getTypeDescriptor()}. The internal id generator used by
     * this store will also be created.
     *
     * @param fileName The file name of the store that will be created
     * @param baseBlockSize The number of bytes for each block
     * @param typeAndVersionDescriptor The type and version descriptor that identifies this store
     */
    public void createEmptyDynamicStore( File fileName, int baseBlockSize,
                                         String typeAndVersionDescriptor,
                                         @SuppressWarnings( "deprecation" ) IdType idType )
    {
        int blockSize = baseBlockSize;
        // sanity checks
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        if ( fileSystemAbstraction.fileExists( fileName ) )
        {
            throw new IllegalStateException( "Can't create store[" + fileName
                                             + "], file already exists" );
        }
        if ( blockSize < 1 )
        {
            throw new IllegalArgumentException( "Illegal block size["
                                                + blockSize + "]" );
        }
        if ( blockSize > 0xFFFF )
        {
            throw new IllegalArgumentException( "Illegal block size[" + blockSize + "], limit is 65535" );
        }
        blockSize += AbstractDynamicStore.BLOCK_HEADER_SIZE;

        // write the header
        try
        {
            StoreChannel channel = fileSystemAbstraction.create( fileName );
            int endHeaderSize = blockSize
                                + UTF8.encode( typeAndVersionDescriptor ).length;
            ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
            buffer.putInt( blockSize );
            buffer.position( endHeaderSize - typeAndVersionDescriptor.length() );
            buffer.put( UTF8.encode( typeAndVersionDescriptor ) ).flip();
            channel.write( buffer );
            channel.force( false );
            channel.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to create store "
                                                  + fileName, e );
        }
        idGeneratorFactory.create( new File( fileName.getPath() + ".id" ), 0, true );
        // TODO highestIdInUse = 0 works now, but not when slave can create store files.
        IdGenerator idGenerator = idGeneratorFactory.open( 
                new File( fileName.getPath() + ".id" ),
                idType.getGrabSize(), idType, 0 );
        idGenerator.nextId(); // reserve first for blockSize
        idGenerator.close();
    }
    public void createEmptyStore( File fileName, String typeAndVersionDescriptor )
    {
        createEmptyStore( fileName, typeAndVersionDescriptor, null, null );
    }

    private void createEmptyStore( File fileName, String typeAndVersionDescriptor, ByteBuffer firstRecordData,
                                   @SuppressWarnings( "deprecation" ) IdType idType )
    {
        // sanity checks
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        if ( fileSystemAbstraction.fileExists( fileName ) )
        {
            throw new IllegalStateException( "Can't create store[" + fileName
                                             + "], file already exists" );
        }

        // write the header
        try
        {
            StoreChannel channel = fileSystemAbstraction.create( fileName );
            int endHeaderSize = UTF8.encode( typeAndVersionDescriptor ).length;
            if ( firstRecordData != null )
            {
                endHeaderSize += firstRecordData.limit();
            }
            ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
            if ( firstRecordData != null )
            {
                buffer.put( firstRecordData );
            }
            buffer.put( UTF8.encode( typeAndVersionDescriptor ) ).flip();
            channel.write( buffer );
            channel.force( false );
            channel.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to create store " + fileName, e );
        }
        idGeneratorFactory.create( new File( fileName.getPath() + ".id" ), 0, true );
        if ( firstRecordData != null )
        {
            IdGenerator idGenerator = idGeneratorFactory.open( 
                    new File( fileName.getPath() + ".id" ), 1, idType, 0 );
            idGenerator.nextId(); // reserve first for blockSize
            idGenerator.close();
        }
    }
}
