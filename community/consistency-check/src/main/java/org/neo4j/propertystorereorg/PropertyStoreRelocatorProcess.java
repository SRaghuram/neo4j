package org.neo4j.propertystorereorg;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.full.FullCheckNewUtils;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.store.StoreAccess;
import org.neo4j.store.StoreFactory;
import org.neo4j.utils.PropertyReader;
import org.neo4j.utils.ToolUtils;
import org.neo4j.utils.runutils.RecordProcessor;

public class PropertyStoreRelocatorProcess implements RecordProcessor<Abstract64BitRecord>
{
    PropertyReader propertyReader;
    public List<PropertyRecord>[] propertiesProcessed = null;
    public int[] propertyCheckCount = null;
    StoreAccess storeAccess;
    StoreFactory storeFactory;
    FileSystemAbstraction fileSystemAbstraction;
    NeoStore neoStore = null;
    public static PropertyStore TempPropertyStore = null;
    public static final String TEMPDIR = "tempDir";
    public static final String BACKUPDIR = "backupDir";
    public static int backupDir_version = 0;
    public static long totalPropertiesCopied = 0;

    public PropertyStoreRelocatorProcess( StoreAccess storeAccess, StoreFactory storeFactory,
            FileSystemAbstraction fileSys )
    {
        this.storeAccess = storeAccess;
        this.neoStore = storeAccess.getRawNeoStore();
        this.storeFactory = storeFactory;
        this.fileSystemAbstraction = fileSys;
        this.propertyReader = new PropertyReader( storeAccess );
        createPropertyCache( propertyReader.getPropertyStore().getHighId() );
    }

    public void createPropertyCache( long propHighId )
    {
        if (propertiesProcessed == null)
        {
            propertiesProcessed = new List[ToolUtils.MAX_THREADS];
            propertyCheckCount = new int[(int) (propHighId / ToolUtils.Count16K) + 1];
        }
    }

    public static PropertyStore createTempStore( StoreAccess storeAccess, StoreFactory storeFactory )
    {
        File dir = new File( getTempDirName( storeAccess ) );
        storeFactory.createPropertyStore( dir, true );
        TempPropertyStore = storeFactory.newPropertyStoreWithNoKey( dir );
        return TempPropertyStore;
    }

    @Override
    public void process( Abstract64BitRecord record )
    {
        try
        {
            List<PropertyRecord> properties =
                    (List<PropertyRecord>) propertyReader.getPropertyRecordChain( ((PrimitiveRecord) record)
                            .getNextProp() );
            totalPropertiesCopied += copyProperties( (PrimitiveRecord) record, properties );
        }
        catch ( Exception e )
        {
            System.out.println( "Not able to copy:" + record.toString() );
        }
    }
   
    @Override
    public void close()
    {
        ToolUtils.saveMessage( "Property relocation - Copied [" + totalPropertiesCopied +
                "] properties. ");
        ToolUtils.saveMessage( "HigIds: Temp - PropertyStore ["+ TempPropertyStore.getHighId() 
                +"] String Store [" + TempPropertyStore.getStringStore().getHighId() 
                +"] Array Store [" + TempPropertyStore.getArrayStore().getHighId() +"]");
        ToolUtils.saveMessage( "HigIds: Current - PropertyStore ["+ storeAccess.getPropertyStore().getHighId() 
                +"] String Store [" + storeAccess.getStringStore().getHighId() 
                +"] Array Store [" + storeAccess.getArrayStore().getHighId() +"]");
    }

    public static String getBackupDirName( StoreAccess storeAccess )
    {
        return storeAccess.getRawNeoStore().getStorageFileName().getParentFile().getAbsolutePath() + File.separator
                + BACKUPDIR;
    }

    public static String getTempDirName( StoreAccess storeAccess )
    {
        return storeAccess.getRawNeoStore().getStorageFileName().getParentFile().getAbsolutePath() + File.separator
                + TEMPDIR;
    }

    public static void backupNodeAndRelationshipStore(StoreAccess storeAccess, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction)
    {
        String backupName = getBackupDirName( storeAccess );
        int version = backupDir_version;
        File newLocation = new File( backupName + "_" + version );
        while ( newLocation.exists() )
        {
            newLocation = new File( backupName + "_" + (++version) );
        }
        backupDir_version = version;
        ToolUtils.printMessage( "\tBacking up Node, Relationship, and Property stores at " + newLocation.getAbsolutePath() );
        ToolUtils.printMessage( "\tWill take few minutes." );
        copyNodeStore( newLocation, storeFactory, fileSystemAbstraction );
        copyRelationshipStore( newLocation, storeFactory, fileSystemAbstraction );
        copyPropertyStore( newLocation, storeFactory, fileSystemAbstraction );
        ToolUtils.printMessage( "\tCompleted the copy of store files" );
    }

    public static void switchTempStore(StoreAccess storeAccess, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction)
    {
        TempPropertyStore.close();
        storeAccess.getRawNeoStore().getPropertyStore().close();
        deletePropertyStore(storeAccess.getRawNeoStore().getStorageFileName().getParentFile(), storeFactory);
        movePropertyStore( TempPropertyStore.getStorageFileName().getParentFile(), storeAccess.getRawNeoStore()
                .getStorageFileName().getParentFile(), storeFactory, fileSystemAbstraction );
        TempPropertyStore.getStorageFileName().getParentFile().delete();
        storeAccess.getRawNeoStore().getPropertyStore().close();
    }

    public static void deleteTempStore()
    {
        File dir = TempPropertyStore.getStorageFileName();
        for ( File file : dir.getParentFile().listFiles() )
            file.delete();
        dir.getParentFile().delete();
    }

    public static boolean copyNodeStore( File newLocation, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                fileSystemAbstraction.mkdirs( newLocation );
            }
            catch ( IOException e )
            {
                System.out.println( "Unable to create directory " + newLocation + " for creating a temp node store in"
                        + e.getMessage() );
                return false;
            }
        }
        ToolUtils.printMessage( "\tCopying Node store of size ["
                + storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME ).length() + "] bytes" );
        try
        {
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME ),
                    storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME + ".id" ),
                    storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME + ".id", newLocation ) );
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME ),
                    storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME + ".id" ),
                    storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME + ".id", newLocation ) );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in copying node files" );
            return false;
        }
        return true;
    }

    public static boolean moveNodeStore( File newLocation, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction )
    {
        return moveNodeStore( null, newLocation, storeFactory, fileSystemAbstraction );
    }

    public static boolean moveNodeStore( File currentLocation, File newLocation, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                fileSystemAbstraction.mkdirs( newLocation );
            }
            catch ( IOException e )
            {
                System.out.println( "Unable to create directory " + newLocation + " for creating a temp node store in"
                        + e.getMessage() );
                return false;
            }
        }
        try
        {
            FileUtils.moveFileToDirectory( storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME, currentLocation ),
                    newLocation );
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME + ".id", currentLocation ), newLocation );
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME, currentLocation ), newLocation );
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME + ".id", currentLocation ),
                    newLocation );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in moving node files" );
            return false;
        }
        ToolUtils.printMessage( "Completed the copy of Node store" );
        return true;
    }

    public static boolean copyRelationshipStore( File newLocation, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                fileSystemAbstraction.mkdirs( newLocation );
            }
            catch ( IOException e )
            {
                System.out.println( "Unable to create directory " + newLocation + " for creating a temp node store in"
                        + e.getMessage() );
                return false;
            }
        }
        ToolUtils.printMessage( "\tCopying Relationship store of size ["
                + storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME ).length() + "] bytes" );
        try
        {
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME ),
                    storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME + ".id" ),
                    storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME + ".id", newLocation ) );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in copying node files" );
            return false;
        }
        return true;
    }

    public static boolean moveRelationshipStore( File newLocation, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction )
    {
        return moveNodeStore( null, newLocation, storeFactory, fileSystemAbstraction );
    }

    public boolean moveRelationshipStore( File currentLocation, File newLocation, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                fileSystemAbstraction.mkdirs( newLocation );
            }
            catch ( IOException e )
            {
                System.out.println( "Unable to create directory " + newLocation + " for creating a temp node store in"
                        + e.getMessage() );
                return false;
            }
        }
        try
        {
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME, currentLocation ), newLocation );
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME + ".id", currentLocation ),
                    newLocation );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in moving node files" );
            return false;
        }
        return true;
    }

    public static boolean copyPropertyStore( File newLocation, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                fileSystemAbstraction.mkdirs( newLocation );
            }
            catch ( IOException e )
            {
                System.out.println( "Unable to create directory " + newLocation
                        + " for creating a temp property store in" + e.getMessage() );
                return false;
            }
        }
        ToolUtils.printMessage( "\tCopying property, string, array store of size ["
                + storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME ).length() +", " +
                + storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME ).length() +", " +
                + storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME ).length()  
                + "] bytes" );
        try
        {
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME ),
                    storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME + ".id" ),
                    storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME + ".id", newLocation ) );
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME ),
                    storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME + ".id" ),
                    storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME + ".id", newLocation ) );
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME ),
                    storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME + ".id" ),
                    storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME + ".id", newLocation ) );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in copying property files" );
            return false;
        }
        return true;
    }

    public static boolean movePropertyStore( File newLocation, StoreAccess storeAccess, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction )
    {
        storeAccess.getRawNeoStore().getPropertyStore().close();
        return movePropertyStore( null, newLocation, storeFactory, fileSystemAbstraction );
    }

    public static boolean movePropertyStore( File currentLocation, File newLocation, StoreFactory storeFactory,
            FileSystemAbstraction fileSystemAbstraction )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                fileSystemAbstraction.mkdirs( newLocation );
            }
            catch ( IOException e )
            {
                System.out.println( "Unable to create directory " + newLocation
                        + " for creating a temp property store in" + e.getMessage() );
                return false;
            }
        }
        try
        {
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME, currentLocation ), newLocation );
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME + ".id", currentLocation ),
                    newLocation );
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME, currentLocation ),
                    newLocation );
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME + ".id", currentLocation ),
                    newLocation );
            FileUtils
                    .moveFileToDirectory(
                            storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME, currentLocation ),
                            newLocation );
            FileUtils.moveFileToDirectory(
                    storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME + ".id", currentLocation ),
                    newLocation );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in copying property files" );
            return false;
        }
        return true;
    }
    
    public static boolean deletePropertyStore( File location, StoreFactory storeFactory )
    {
        if ( !location.exists() )
        {          
                System.out.println( "location " + location
                        + " does not exist - nothing to delete" );
                return true;
        }
     
        FileUtils.deleteFile( 
                storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME, location ));
        FileUtils.deleteFile(
                storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME + ".id", location ));
        FileUtils.deleteFile(
                storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME, location ));
        FileUtils.deleteFile(
                storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME + ".id", location ));
        FileUtils
                .deleteFile(storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME, location ));
        FileUtils.deleteFile(
                storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME + ".id", location ));
       
        return true;
    }

    private synchronized long[] reserveIds( PropertyStore store, List<PropertyRecord> properties )
    {
        if ( properties == null || properties.size() == 0 )
            return null;
        long[] ids = new long[3];
        ids[0] = store.getHighId();
        ids[1] = store.getStringStore().getHighId();
        ids[2] = store.getArrayStore().getHighId();
        if ( ids[0] == Record.NO_NEXT_RELATIONSHIP.intValue() )
            ids[0] = 0;
        if ( ids[1] == Record.NO_NEXT_RELATIONSHIP.intValue() )
            ids[1] = 0;
        if ( ids[2] == Record.NO_NEXT_RELATIONSHIP.intValue() )
            ids[2] = 0;
        store.setHighId( ids[0] + properties.size() );
        int strId = 0, arrayId = 0;
        for ( PropertyRecord property : properties )
        {
            for ( PropertyBlock block : (PropertyRecord) property )
            {
                PropertyType type = block.forceGetType();
                if ( type != null )
                {
                    switch ( type )
                    {
                    case STRING:
                        strId++;
                        break;
                    case ARRAY:
                        arrayId++;
                        break;
                    }
                }
            }
        }
        if ( strId > 0 )
            store.getStringStore().setHighId( ids[1] + strId );
        if ( arrayId > 0 )
            store.getArrayStore().setHighId( ids[2] + arrayId );
        return ids;
    }

    private static synchronized long reserveId( PropertyStore store, List<PropertyRecord> properties )
    {
        if ( properties == null || properties.size() == 0 )
            return -1;
        long ids = store.getHighId();
        store.setHighId( ids + properties.size() );
        return ids;
    }

    public int copyProperties( PrimitiveRecord record, List<PropertyRecord> properties )
    {
        int propsCopied = 0;
        if ( properties != null )
        {
            long propId = reserveId( TempPropertyStore, properties );
            record.setNextProp( propId );
            int index = 0;
            for ( PropertyRecord property : properties )
            {
                if ( !property.inUse() )
                {
                    System.out.println( "Not in use [" + property.getId() +"] in property chain" );
                    properties.remove( property );
                    continue;
                }
                property.setId( propId++ );
                property.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
                property.setPrevProp( Record.NO_PREVIOUS_PROPERTY.intValue() );
                PropertyBlock[] blocks = property.getPropertyBlocks();
                property.clearPropertyBlocks();
                for ( PropertyBlock block : blocks )
                {
                    PropertyBlock newBlock = new PropertyBlock();
                    PropertyStore.encodeValue( newBlock, block.getKeyIndexId(),
                            neoStore.getPropertyStore().getValue( block ), TempPropertyStore.getStringStore(),
                            TempPropertyStore.getArrayStore() );
                    property.addPropertyBlock( newBlock );
                }
                //link with previous property
                if ( index++ > 0 )
                {
                    property.setPrevProp( properties.get( index - 1 ).getId() );
                    properties.get( index - 1 ).setNextProp( property.getId() );
                }
            }
           
            for ( PropertyRecord property : properties )
            {
                TempPropertyStore.updateRecord( property );
                propsCopied++;
            }
            if ( record instanceof NodeRecord )
                neoStore.getNodeStore().updateRecord( (NodeRecord) record );
            else if ( record instanceof RelationshipRecord )
                neoStore.getRelationshipStore().updateRecord( (RelationshipRecord) record );
        }
        return propsCopied;
    }
}
