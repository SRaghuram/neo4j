package org.neo4j.consistency;


public class FullCheckType
{
    public static final String COMMAND = "command";
    public static final String GET_STATS = "get-propstore-Stats";
    public static final String REORG = "reorg-propertyStore";
    public static final String LEGACY = "legacy";
    
    static public enum ToolTypes
    {
        Legacy_Consistency_Check,
        New_Fast_Consistency_Check,
        PropertyStore_Reorg
    }
    static public ToolTypes toolType = ToolTypes.New_Fast_Consistency_Check;
}
