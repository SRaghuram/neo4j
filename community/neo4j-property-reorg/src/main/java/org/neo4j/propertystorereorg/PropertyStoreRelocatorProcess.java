package org.neo4j.propertystorereorg;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.propertystorereorg.PropertystoreReorgTool.StoreDetails;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.store.StoreAccess;
import org.neo4j.store.StoreFactory;
import org.neo4j.utils.PropertyReader;
import org.neo4j.utils.ToolUtils;
import org.neo4j.utils.runutils.RecordProcessor;

public class PropertyStoreRelocatorProcess implements RecordProcessor<Abstract64BitRecord>
{
    public List<PropertyRecord>[] propertiesProcessed = null;
    public int[] propertyCheckCount = null;
    public static PropertyStore TempPropertyStore = null;
    public static final String TEMPDIR = "tempDir";
    public static final String BACKUPDIR = "backupDir";
    public static int backupDir_version = 0;
    public static long totalPropertiesCopied = 0;

    public PropertyStoreRelocatorProcess(  )
    {
    }

    public void createPropertyCache( long propHighId )
    {
        if (propertiesProcessed == null)
        {
            propertiesProcessed = new List[ToolUtils.MAX_THREADS];
            propertyCheckCount = new int[(int) (propHighId / ToolUtils.Count16K) + 1];
        }
    }

    public static PropertyStore createTempStore( StoreDetails storeDetails )
    {
        File dir = new File( getTempDirName( storeDetails.storeAccess ) );
        //storeFactory.createPropertyStore( dir, true );
        TempPropertyStore = storeDetails.storeFactory.newPropertyStoreWithNoKeys(dir);
        		//storeDetails.storeFactory.newPropertyStoreWithNoKey( dir );
        return TempPropertyStore;
    }

    @Override
    public void process( Abstract64BitRecord record, StoreDetails storeDetails )
    {
        try
        {
            PropertyReader propertyReader = new PropertyReader( storeDetails.storeAccess );
            createPropertyCache( propertyReader.getPropertyStore().getHighId() );
            List<PropertyRecord> properties =
                    (List<PropertyRecord>) propertyReader.getPropertyRecordChain( ((PrimitiveRecord) record)
                            .getNextProp() );
            totalPropertiesCopied += copyProperties( (PrimitiveRecord) record, properties, storeDetails );
        }
        catch ( Exception e )
        {
            System.out.println( "Not able to copy:" + record.toString() );
        }
    }
   
    @Override
    public void close(StoreDetails storeDetails)
    {
        ToolUtils.saveMessage( "Property relocation - Copied [" + totalPropertiesCopied +
                "] properties. ");
        ToolUtils.saveMessage( "HigIds: Temp - PropertyStore ["+ TempPropertyStore.getHighId() 
                +"] String Store [" + TempPropertyStore.getStringStore().getHighId() 
                +"] Array Store [" + TempPropertyStore.getArrayStore().getHighId() +"]");
        ToolUtils.saveMessage( "HigIds: Current - PropertyStore ["+ storeDetails.storeAccess.getPropertyStore().getHighId() 
                +"] String Store [" + storeDetails.storeAccess.getStringStore().getHighId() 
                +"] Array Store [" + storeDetails.storeAccess.getArrayStore().getHighId() +"]");
    }

    public static String getBackupDirName( StoreAccess storeAccess )
    {
        return storeAccess.getRawNeoStores().getStoreFileName("").getParentFile().getAbsolutePath() + File.separator
                + BACKUPDIR;
    }

    public static String getTempDirName( StoreAccess storeAccess )
    {
        return storeAccess.getRawNeoStores().getStoreFileName("").getParentFile().getAbsolutePath() + File.separator
                + TEMPDIR;
    }

    public static void backupNodeAndRelationshipStore(StoreDetails storeDetails)
    {
        String backupName = getBackupDirName( storeDetails.storeAccess );
        int version = backupDir_version;
        File newLocation = new File( backupName + "_" + version );
        while ( newLocation.exists() )
        {
            newLocation = new File( backupName + "_" + (++version) );
        }
        backupDir_version = version;
        ToolUtils.printMessage( "\tBacking up Node, Relationship, and Property stores at " + newLocation.getAbsolutePath() );
        ToolUtils.printMessage( "\tWill take few minutes." );
        copyNodeStore( newLocation, storeDetails );
        copyRelationshipStore( newLocation, storeDetails );
        copyPropertyStore( newLocation, storeDetails );
        ToolUtils.printMessage( "\tCompleted the copy of store files" );
    }

    public static void switchTempStore(StoreDetails storeDetails)
    {
        TempPropertyStore.close();
        storeDetails.storeAccess.getRawNeoStores().getPropertyStore().close();
        deletePropertyStore(storeDetails.storeAccess.getRawNeoStores().getStoreFileName("").getParentFile(), storeDetails.storeFactory);
        movePropertyStore( TempPropertyStore.getStorageFileName().getParentFile(), storeDetails.storeAccess.getRawNeoStores()
                .getStoreFileName("").getParentFile(), storeDetails );
        TempPropertyStore.getStorageFileName().getParentFile().delete();
        storeDetails.storeAccess.getRawNeoStores().getPropertyStore().close();
    }

    public static void deleteTempStore()
    {
        File dir = TempPropertyStore.getStorageFileName();
        for ( File file : dir.getParentFile().listFiles() )
            file.delete();
        dir.getParentFile().delete();
    }

    public static boolean copyNodeStore( File newLocation, StoreDetails storeDetails )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                storeDetails.fileSystem.mkdirs( newLocation );
            }
            catch ( IOException e )
            {
                System.out.println( "Unable to create directory " + newLocation + " for creating a temp node store in"
                        + e.getMessage() );
                return false;
            }
        }
        ToolUtils.printMessage( "\tCopying Node store of size ["
                + storeDetails.storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME ).length() + "] bytes" );
        try
        {
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME + ".id" ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME + ".id", newLocation ) );
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME + ".id" ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME + ".id", newLocation ) );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in copying node files" );
            return false;
        }
        return true;
    }

    public static boolean moveNodeStore( File newLocation, StoreDetails storeDetails )
    {
        return moveNodeStore( null, newLocation, storeDetails );
    }

    public static boolean moveNodeStore( File currentLocation, File newLocation, StoreDetails storeDetails )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                storeDetails.fileSystem.mkdirs( newLocation );
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
            FileUtils.moveFileToDirectory( storeDetails.storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME, currentLocation ),
                    newLocation );
            FileUtils.moveFileToDirectory(
                    storeDetails.storeFactory.storeFileName( StoreFactory.NODE_STORE_NAME + ".id", currentLocation ), newLocation );
            FileUtils.moveFileToDirectory(
                    storeDetails.storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME, currentLocation ), newLocation );
            FileUtils.moveFileToDirectory(
                    storeDetails.storeFactory.storeFileName( StoreFactory.NODE_LABELS_STORE_NAME + ".id", currentLocation ),
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

    public static boolean copyRelationshipStore( File newLocation, StoreDetails storeDetails )
    {
        if ( !newLocation.exists() )
        {
            try
            {
            	storeDetails.fileSystem.mkdirs( newLocation );
            }
            catch ( IOException e )
            {
                System.out.println( "Unable to create directory " + newLocation + " for creating a temp node store in"
                        + e.getMessage() );
                return false;
            }
        }
        ToolUtils.printMessage( "\tCopying Relationship store of size ["
                + storeDetails.storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME ).length() + "] bytes" );
        try
        {
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME ),
            		storeDetails.storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME + ".id" ),
            		storeDetails.storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME + ".id", newLocation ) );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in copying node files" );
            return false;
        }
        return true;
    }

    public static boolean moveRelationshipStore( File newLocation, StoreDetails storeDetails )
    {
        return moveNodeStore( null, newLocation, storeDetails );
    }

    public boolean moveRelationshipStore( File currentLocation, File newLocation, StoreDetails storeDetails )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                storeDetails.fileSystem.mkdirs( newLocation );
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
                    storeDetails.storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME, currentLocation ), newLocation );
            FileUtils.moveFileToDirectory(
                    storeDetails.storeFactory.storeFileName( StoreFactory.RELATIONSHIP_STORE_NAME + ".id", currentLocation ),
                    newLocation );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in moving node files" );
            return false;
        }
        return true;
    }

    public static boolean copyPropertyStore( File newLocation, StoreDetails storeDetails )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                storeDetails.fileSystem.mkdirs( newLocation );
            }
            catch ( IOException e )
            {
                System.out.println( "Unable to create directory " + newLocation
                        + " for creating a temp property store in" + e.getMessage() );
                return false;
            }
        }
        ToolUtils.printMessage( "\tCopying property, string, array store of size ["
                + storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME ).length() +", " +
                + storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME ).length() +", " +
                + storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME ).length()  
                + "] bytes" );
        try
        {
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME + ".id" ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME + ".id", newLocation ) );
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME + ".id" ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME + ".id", newLocation ) );
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME, newLocation ) );
            FileUtils.copyFile( storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME + ".id" ),
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME + ".id", newLocation ) );
        }
        catch ( IOException ie )
        {
            System.out.println( "Error in copying property files" );
            return false;
        }
        return true;
    }

    public static boolean movePropertyStore( File newLocation, StoreDetails storeDetails )
    {
        storeDetails.storeAccess.getRawNeoStores().getPropertyStore().close();
        return movePropertyStore( null, newLocation, storeDetails );
    }

    public static boolean movePropertyStore( File currentLocation, File newLocation, StoreDetails storeDetails )
    {
        if ( !newLocation.exists() )
        {
            try
            {
                storeDetails.fileSystem.mkdirs( newLocation );
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
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME, currentLocation ), newLocation );
            FileUtils.moveFileToDirectory(
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STORE_NAME + ".id", currentLocation ),
                    newLocation );
            FileUtils.moveFileToDirectory(
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME, currentLocation ),
                    newLocation );
            FileUtils.moveFileToDirectory(
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME + ".id", currentLocation ),
                    newLocation );
            FileUtils
                    .moveFileToDirectory(
                            storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME, currentLocation ),
                            newLocation );
            FileUtils.moveFileToDirectory(
                    storeDetails.storeFactory.storeFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME + ".id", currentLocation ),
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

    public int copyProperties( PrimitiveRecord record, List<PropertyRecord> properties, StoreDetails storeDetails )
    {
        int propsCopied = 0;
        NeoStores neoStore = storeDetails.storeAccess.getRawNeoStores();
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
                PropertyBlock[] blocks = new PropertyBlock[property.numberOfProperties()];
                int i = 0;
                while (property.hasNext())
                	blocks[i++] = property.next();
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
                if ( index > 0 )
                {
                    property.setPrevProp( properties.get( index - 1 ).getId() );
                    properties.get( index - 1 ).setNextProp( property.getId() );                   
                }
                index++;
            }
           
            if (properties.get( 0 ).getPrevProp() != Record.NO_PREVIOUS_PROPERTY.intValue() ||
                    properties.get( 0 ).getId() != record.getNextProp() )
                System.out.println("Error in linkages");
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
