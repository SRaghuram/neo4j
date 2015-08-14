package org.neo4j.consistency.checking.full;

import java.util.List;

import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

public class PropertyCache
{
    public static List<PropertyRecord>[] propertiesProcessed;
    public static int[] propertyCheckCount = null;

    public static void createPropertyCache( long propHighId )
    {
        propertiesProcessed = new List[FullCheckNewUtils.MAX_THREADS];
        propertyCheckCount = new int[(int) (propHighId / FullCheckNewUtils.Count16K) + 1];
    }

    public static boolean hasCachedProperties()
    {
        return propertyCheckCount == null ? false : true;
    }

    public static void saveProperties( List<PropertyRecord> properties )
    {
        if ( propertiesProcessed != null )
        {
            propertiesProcessed[FullCheckNewUtils.getThreadIndex()] = properties;
        }
    }

    public static void countProperties( List<PropertyRecord> properties )
    {
        if ( propertyCheckCount == null )
            return;
        for ( PropertyRecord property : properties )
            propertyCheckCount[(int) (property.getId() / FullCheckNewUtils.Count16K)]++;
    }

    public static PropertyRecord getFromPropertyCache( long id )
    {
        List<PropertyRecord> propList = propertiesProcessed[FullCheckNewUtils.getThreadIndex()];
        if ( propList == null )
            return null;
        for ( PropertyRecord property : propList )
        {
            if ( property.getId() == id )
                return property;
        }
        return null;
    }

    public static void processProperties( NodeRecord node,
            RecordCheck<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> propertyCheck,
            ConsistencyReporter reporter )
    {
        int threadIndex = FullCheckNewUtils.getThreadIndex();
        if ( propertiesProcessed[threadIndex] != null )
        {
            for ( PropertyRecord property : propertiesProcessed[threadIndex] )
            {
                reporter.forProperty( property, propertyCheck );
                propertyCheckCount[(int) (property.getId() / FullCheckNewUtils.Count16K)]++;
            }
            propertiesProcessed[threadIndex] = null;
        }
    }
}
