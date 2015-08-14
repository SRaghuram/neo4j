package org.neo4j.utils.runutils;

public interface CommandProcessor
{
    public boolean runCommand(StoppableRunnable runProcess);
    public interface ICCCommandType
    {
        public String getName();
    }

    public static enum CCCommandType implements ICCCommandType
    {
        CCNew_Check
        {
            public String getName()
            {
                return "Full consistency check - Fast";
            }
        },
        CCNew_GetStats
        {
            public String getName()
            {
                return "Property Store scatter statistics";
            }
        },
        CCNew_ReorgPropertyStore
        {
            public String getName()
            {
                return "Reorganizing Property, String, and Array Store - on node id order";
            }
        },
        CCNew_BackupNodeAndRelationshipStore
        {
            public String getName()
            {
                return "Backing up Node and Realtionship files";
            }
        },
        CCNew_SwapTempStore
        {
            public String getName()
            {
                return "Swaping the property store files from temp";
            }
        },
        CCNew_CreateTempStore
        {

            @Override
            public String getName()
            {
                return "Creating temp store";
            }
            
        },
        CCNew_ReorgPropertyStoreLabelBased
        {
            public String getName()
            {
                return "Reorganizing Property, String, and Array Store - based on label order";
            }
        };
    }
}
