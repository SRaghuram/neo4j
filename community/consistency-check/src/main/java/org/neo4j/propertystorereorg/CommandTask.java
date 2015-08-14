package org.neo4j.propertystorereorg;

import java.util.ArrayList;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.store.StoreAccess;
import org.neo4j.store.StoreFactory;
import org.neo4j.utils.runutils.CommandProcessor;
import org.neo4j.utils.runutils.StoppableRunnable;


public class CommandTask implements CommandProcessor
{
    CCCommandType[] commands;
    StoreAccess storeAccess;
    StoreFactory storeFactory;
    FileSystemAbstraction fileSystemAbstraction;

    public CommandTask( StoreAccess storeAccess, StoreFactory storeFactory,
            FileSystemAbstraction fileSys, CCCommandType... commands )
    {
        this.commands = commands;
        this.storeAccess = storeAccess;
        this.storeFactory = storeFactory;
        this.fileSystemAbstraction = fileSys;
    }

    public boolean runCommand(StoppableRunnable runProcess)
    {
        for (CCCommandType command : commands)
        {
            try
            {
                if ( command == CCCommandType.CCNew_BackupNodeAndRelationshipStore )
                {
                    PropertyStoreRelocatorProcess.backupNodeAndRelationshipStore(storeAccess,  storeFactory,
                            fileSystemAbstraction);
                }
                else if ( command == CCCommandType.CCNew_SwapTempStore )
                {
                    PropertyStoreRelocatorProcess.switchTempStore(storeAccess,  storeFactory,
                            fileSystemAbstraction);
                }
                else if ( command == CCCommandType.CCNew_CreateTempStore)
                {
                    PropertyStoreRelocatorProcess.createTempStore( storeAccess, storeFactory );
                }
            } catch (Exception e)
            {
                System.out.println("Error in runCommand:"+e.getMessage());
                return false;
            }
        }
        return true;
    }


    private static ArrayList<CCCommandType> CCCommand = new ArrayList<CCCommandType>();

    static public boolean setCCCommand( String command )
    {
        if ( command.equalsIgnoreCase( PropertyReorgSettings.GET_PROPERTY_STATS ) )
            CCCommand.add( CCCommandType.CCNew_GetStats);
        else if ( command.equalsIgnoreCase( PropertyReorgSettings.REORG_PROPERTYSTORE ) )
            CCCommand.add( CCCommandType.CCNew_ReorgPropertyStore);
        else if ( command.equalsIgnoreCase( PropertyReorgSettings.REORG_PROPERTYSTORE_LABEL ) )
            CCCommand.add(CCCommandType.CCNew_ReorgPropertyStoreLabelBased);
        else
            return false;
        return true;
    }

    static public CCCommandType[] getCommand()
    {
        if (CCCommand.isEmpty())
            return new CCCommandType[]{CCCommandType.CCNew_GetStats};
        return CCCommand.toArray( new CCCommandType[0] );
    }

}
