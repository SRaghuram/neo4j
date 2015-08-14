package org.neo4j.consistency.checking.full;

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.neo4j.consistency.FullCheckType;
import org.neo4j.consistency.store.StoreAccess;
import org.neo4j.consistency.store.StoreFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.utils.PackedMultiFieldCache;

public class FullCheckNewUtils
{
   
    public static StoreFactory storeFactory = null;
    public static NeoStore neoStore = null;
    public static FileSystemAbstraction fileSystemAbstraction = null;
    public static StoreAccess storeAccess = null;
    public FullCheckNewUtils()
    {
        // TODO Auto-generated constructor stub
    }
    public static void setFactory(StoreFactory factory, NeoStore neo, FileSystemAbstraction fileSys, StoreAccess stAccess)
    {
        storeFactory = factory;
        neoStore = neo;
        fileSystemAbstraction = fileSys;
        storeAccess = stAccess;
    }
    static public int Count16K = (int)Math.pow(2,14);
    static public int Count32K = (int)Math.pow(2,15);
    static public int Count64K = (int)Math.pow(2,16);
    static public int LOCALITY = 500;

    // Cache related utilities and status
    public static void Init()
    {
        NewCCCache = new NewCC_Cache( 1, 63 );
    }

    static public int QSize = 1000;
    static public String[] TaskTimes = new String[50];
    static public boolean forwardScan = true;

    static public void setForward( boolean forward )
    {
        forwardScan = forward;
    }

    static public boolean isForward()
    {
        return forwardScan;
    }

    static public NewCC_Cache NewCCCache = null;

    public static class NewCC_Cache extends PackedMultiFieldCache
    {
        public NewCC_Cache()
        {
            super();
        }

        public NewCC_Cache( int... fields )
        {
            super( fields );
        }
    }

    public static void initCache( long index )
    {
        FullCheckNewUtils.NewCCCache.initCache( index );
        FullCheckNewUtils.Counts.clearCacheCount[FullCheckNewUtils.getThreadIndex()]++;
        FullCheckNewUtils.Counts.activeCacheCount[FullCheckNewUtils.getThreadIndex()]--;
    }
    
    public static void saveToCache( RelationshipRecord relationship )
    {
        if ( !relationship.inUse() )
            return;
        long[] cache1 = FullCheckNewUtils.NewCCCache.getFromCache( relationship.getFirstNode() );
        long[] cache2 = FullCheckNewUtils.NewCCCache.getFromCache( relationship.getSecondNode() );
        long id = relationship.getId();
        long fN = relationship.getFirstNextRel();
        long fP = relationship.getFirstPrevRel();
        long sN = relationship.getSecondNextRel();
        long sP = relationship.getSecondPrevRel();
        /*
         * save to cache the information in this relationship that will be
         * referred in future, i.e., in the forward scan, these are the link ids
         * having value greater than current one and in the backward scan, these
         * are the link ids having value lower than the current one. Choose the
         * lower of two in case of forward scan, and greater of two in backward
         * scan. Save ONLY when the cache slot is available. Otherwise, i.e., it
         * contains cached information, check if the information from current
         * relationship will be utilized sooner. If so, update the cached
         * information.
         */
        boolean cache1Free = cache1[2] == 0x7FFFFFFFFl ? true : false;
        boolean cache2Free = cache2[2] == 0x7FFFFFFFFl ? true : false;
        if ( FullCheckNewUtils.forwardScan )
        {
            if ( StoreProcessorTask.withinBounds( relationship.getFirstNode() ) )
            {
                FullCheckNewUtils.NewCCCache.putToCache( relationship.getFirstNode(), 0, 0, id, fP );
                if ( !cache1Free )
                    FullCheckNewUtils.Counts.overwriteCount[FullCheckNewUtils.getThreadIndex()]++;
                else
                    updateActiveCount();
            }
            if ( StoreProcessorTask.withinBounds( relationship.getSecondNode() ) )
            {
                FullCheckNewUtils.NewCCCache.putToCache( relationship.getSecondNode(), 1, 0, id, sP );
                if ( !cache2Free )
                    FullCheckNewUtils.Counts.overwriteCount[FullCheckNewUtils.getThreadIndex()]++;
                else
                    updateActiveCount();
            }
        }
        else
        {
            if ( StoreProcessorTask.withinBounds( relationship.getFirstNode() ) )
            {
                FullCheckNewUtils.NewCCCache.putToCache( relationship.getFirstNode(), 0, 1, id, fN );
                if ( !cache1Free )
                    FullCheckNewUtils.Counts.overwriteCount[FullCheckNewUtils.getThreadIndex()]++;
                else
                    updateActiveCount();
            }
            if ( StoreProcessorTask.withinBounds( relationship.getSecondNode() ) )
            {
                FullCheckNewUtils.NewCCCache.putToCache( relationship.getSecondNode(), 1, 1, id, sN );
                if ( !cache2Free )
                    FullCheckNewUtils.Counts.overwriteCount[FullCheckNewUtils.getThreadIndex()]++;
                else
                    updateActiveCount();
            }
        }
    }

    public static int saveToCacheInit( RelationshipRecord relationship )
    {
        if ( !relationship.inUse() )
            return 0;
        boolean cacheFirst = FullCheckNewUtils.NewCCCache.getFromCache( relationship.getFirstNode() )[0] == 0 ? false : true;
        boolean cacheSecond = FullCheckNewUtils.NewCCCache.getFromCache( relationship.getSecondNode() )[0] == 0 ? false : true;
        int count = 0;
        if ( !cacheFirst )
        {
            if ( FullCheckNewUtils.forwardScan )
                FullCheckNewUtils.NewCCCache.putToCache( relationship.getFirstNode(), (long) 1, (long) 0, relationship.getId(),
                        relationship.getFirstPrevRel() );
            else
                FullCheckNewUtils.NewCCCache.putToCache( relationship.getFirstNode(), 1, 0, relationship.getFirstNextRel(),
                        relationship.getId() );
            count++;
        }
        if ( !cacheSecond )
        {
            if ( FullCheckNewUtils.forwardScan )
                FullCheckNewUtils.NewCCCache.putToCache( relationship.getSecondNode(), 1, 1, relationship.getId(),
                        relationship.getSecondPrevRel() );
            else
                FullCheckNewUtils.NewCCCache.putToCache( relationship.getSecondNode(), 1, 1, relationship.getSecondNextRel(),
                        relationship.getId() );
            count++;
        }
        return count;
    }
    
    //Stage related utilities and status 
    public interface StageFunctions
    {
        public boolean isParallel( Stages stage );
        public String getPurpose();
    }

    public static enum Stages implements StageFunctions
    {
        Stage1_NS_PropsLabels
        {
            @Override
            public String getPurpose()
            {
                return "NodeStore pass - check its properties, check labels and cache them, skip relationships";
            }
        },
        Stage2_RS_Labels{
            @Override
            public boolean isParallel( Stages stage )
            {
                // TODO Auto-generated method stub
                return false;
            }
            @Override
            public String getPurpose()
            {
                return "ReltionshipStore pass - check label counts using cached labels, check properties, skip nodes and relationships";
            }
        }, 
        Stage3_NS_NextRel {
            @Override
            public String getPurpose()
            {
                return "NodeStore pass - just cache nextRel and inUse";
            }
        }, 
        Stage4_RS_NextRel{
            @Override
            public String getPurpose()
            {
                return "RelationshipStore pass - check nodes inUse, FirstInFirst, FirstInSecond using cached info";
            }
            public boolean isParallel( Stages stage )
            {
                return true;
            }
        }, 
        Stage5_Check_NextRel{
            @Override
            public String getPurpose()
            {
                return "NodeRelationship cache pass - check nextRel";
            }
        }, Stage6_RS_Forward
        {
            @Override
            public String getPurpose()
            {
                return "RelationshipStore pass - forward scan of source chain using the cache";
            }
        },
        Stage7_RS_Backward
        {
            @Override
            public String getPurpose()
            {
                return "RelationshipStore pass - reverse scan of source chain using the cache";
            }
        },
        Stage8_PS_Props{
            @Override
            public String getPurpose()
            {
                return "PropertyStore and Node to Index check pass";
            }
        },
        Stage9_NS_LabelCounts
        {
            @Override
            public String getPurpose()
            {
                return "NodeStore pass - Label counts";
            }
        },
        Stage10_NS_PropertyRelocator{
            @Override
            public String getPurpose()
            {
                return "Property store relocation";
            }
        };
        static boolean[] isParallel = new boolean[] {false, false, false, true, 
                                                     false, true, true, false, 
                                                     true, true};
       
        static Long[] times = new Long[Stages.values().length];
        static ArrayList<Long[]> timings = new ArrayList();

        @Override
        public boolean isParallel( Stages stage )
        {
            // TODO Auto-generated method stub
            // return false;
            return isParallel[stage.ordinal()];
        }

        public static void setParallel( Stages stage, boolean value )
        {
            isParallel[stage.ordinal()] = value;
        }

        public static void setSerial()
        {
            for ( int i = 0; i < isParallel.length; i++ )
                isParallel[i] = false;
        }

        public static void addNewTimingsSet()
        {
            timings.add( times );
            times = new Long[Stages.values().length];
        }

        public static void setTimeTaken( Stages stage, Long time )
        {
            if ( stage != null )
                times[stage.ordinal()] = time;
        }

        public static Long[] getTimes( int index )
        {
            return timings.get( index );
        }
    }
    
    // Debugging related utilities and status (for tuning) 

    public static class Counts
    {
        static public long[] skipCheckCount = null;
        static public long[] missCheckCount = null;
        static public long[] checkedCount = null;
        static public long[] checkErrors = null;
        static public long[] legacySkipCount = null;
        static public long[] correctSkipCheckCount = null;
        static public long[] skipBackupCount = null;
        static public long[] overwriteCount = null;
        static public long forwardlinks = 0;
        static public long backlinks = 0;
        static public long nulllinks = 0;
        static public long[] noCacheSkipCount = null;
        static public long[] activeCacheCount = null;
        static public long[] maxCacheCount = null;
        static public long[] clearCacheCount = null;
        static public long[] noCacheCount = null;
        
        static public long[][] linksChecked = null;

        static public void initCount( int threads )
        {
            skipCheckCount = new long[threads];
            missCheckCount = new long[threads];
            checkedCount = new long[threads];
            checkErrors = new long[threads];
            legacySkipCount = new long[threads];
            correctSkipCheckCount = new long[threads];

            skipBackupCount = new long[threads];
            overwriteCount = new long[threads];          
            forwardlinks = 0;
            backlinks = 0;
            nulllinks = 0;
            linksChecked = new long[4][threads];
            clearCacheCount = new long[threads];
            noCacheCount = new long[threads];
            noCacheSkipCount = new long[threads];
            activeCacheCount = new long[threads];
            maxCacheCount = new long[threads];
        }
    }

    public static void checkRel( Stages stage, RelationshipRecord rel )
    {
        if ( stage != null && (stage == Stages.Stage6_RS_Forward || stage == Stages.Stage7_RS_Backward) )
        {
            long id = rel.getId();
            countLinks( id, rel.getFirstNextRel() );
            countLinks( id, rel.getFirstPrevRel() );
            countLinks( id, rel.getSecondNextRel() );
            countLinks( id, rel.getSecondPrevRel() );
        }
    }

    private static void countLinks( long id1, long id2 )
    {
        if ( id2 == -1 )
            FullCheckNewUtils.Counts.nulllinks++;
        else if ( id2 > id1 )
            FullCheckNewUtils.Counts.forwardlinks++;
        else
            FullCheckNewUtils.Counts.backlinks++;
    }

    public static class Stats
    {
        public static long[][] totalTimeTaken = new long[3][50];
        public static int numRuns = 0;
    }
    
    private static void updateActiveCount()
    {
        if ( ++FullCheckNewUtils.Counts.activeCacheCount[FullCheckNewUtils.getThreadIndex()] > FullCheckNewUtils.Counts.maxCacheCount[FullCheckNewUtils.getThreadIndex()
                ] )
            FullCheckNewUtils.Counts.maxCacheCount[FullCheckNewUtils.getThreadIndex()] =
                    FullCheckNewUtils.Counts.activeCacheCount[FullCheckNewUtils.getThreadIndex()];
    }
    

    public static void saveAccessData(long timeTaken, String storeName, long storeHighId)
    {
        String accessStr = null;
        if (neoStore != null)
        {
            accessStr = storeAccess.getAccessStatsStr();
            if (timeTaken > 0 && accessStr.length() > 2)
            {
                StringBuffer str = new StringBuffer();
                str.append(storeName+": Time["+timeTaken+"] I/Os"+accessStr);
                str.append(" Cache "+FullCheckNewUtils.NewCCCache.getTimes());
                FullCheckNewUtils.NewCCCache.resetTimes();
                FullCheckNewUtils.saveMessage(str.toString());
                FullCheckNewUtils.saveMessage(FullCheckNewUtils.memoryStats());
            }
        }
        if (timeTaken > 0)        
            FullCheckNewUtils.saveMessage(storeName+"["+storeHighId+"]:"+" = "+timeTaken);
        storeAccess.resetStats(FullCheckNewUtils.LOCALITY);
    }
    
    // Multi threading related utilities and status

    public static boolean processor = true;
    public static ThreadLocal<Integer> threadIndex = StoreProcessorTask.threadIndex;
    public static int MAX_THREADS = Runtime.getRuntime().availableProcessors() - 1;

    public static int getThreadIndex()
    {
        if (processor)
        	return StoreProcessorTask.threadIndex.get();
        return RecordScanner.threadIndex.get();
    }
    

    // log/console and print related utilities and status
    static public long startTime = System.currentTimeMillis();
    public static String HeaderMessage = "";
    private static String LogFileName = null;
    private static boolean NoConsoleMsgs = false;
    private static PrintWriter LogFile = null;

    static public void setLogFile( String command, String fileName ) throws IOException
    {
        LogFileName = fileName;
        if ( LogFileName != null )
        {
            LogFile = new PrintWriter( LogFileName );
            NoConsoleMsgs = true;
        }
        if ( command.contains( ":" ) )
        {
            String[] parts = command.split( ":" );
            if ( parts[0].equalsIgnoreCase( "-CCLog" ) && parts[1].equalsIgnoreCase( "console" ) )
                NoConsoleMsgs = false;
        }
    }
    
    public static void printMessage( String message )
    {
        printMessage( message, false);
    }

    public static void printMessage( String message, boolean newLine )
    {
        String msg = (newLine ? "\n" : "") +
                "[" + FullCheckNewUtils.getCurrentTimeStamp() + "][" + (System.currentTimeMillis() - startTime) / 1000 + "] "
                        + message;
        if ( LogFile != null )
        {
            LogFile.println( msg );
            LogFile.flush();
        }
        if ( !NoConsoleMsgs )
            System.out.println( msg );
    }

    public static String buildMessage( String message )
    {
        return "[" + FullCheckNewUtils.getCurrentTimeStamp() + "][" + (System.currentTimeMillis() - startTime) / 1000 + "] "
                + message + "\n";
    }

    private static StringBuilder cachedMsg = new StringBuilder();

    public static void saveMessage( String msg )
    {
        saveMessage(msg, false);
    }
    public static void saveMessage( String msg, boolean force )
    {
        String[] lines = msg.split( "\n" );
        for (String line : lines)
        {
            if (!msg.startsWith( "[" ))
                cachedMsg.append( buildMessage( line ) );
            else
                cachedMsg.append( line +"\n" );
        }
    }

    public static void printSavedMessage()
    {
        printSavedMessage(false);
    }
    public static void printSavedMessage(boolean forcePrint)
    {
        if (!Verbose && !forcePrint)
            return;
        if ( LogFile != null )
        {
            LogFile.print( cachedMsg.toString() );
            LogFile.flush();
        }
        if ( !NoConsoleMsgs )
            System.out.print( "\n" + cachedMsg.toString() );
        cachedMsg.setLength( 0 );
    }

    public static void clearMessage()
    {
        cachedMsg.setLength( 0 );
    }
    
    public static void printProgressMsg(String[] graphDBs, long[] totalTimeTaken, int current)
    {
        if (FullCheckNewUtils.isVerbose())
        {
            FullCheckNewUtils.Stages.addNewTimingsSet();
            StringBuilder progressMsg = new StringBuilder();
            progressMsg.append("-------Summary so far: ["+FullCheckNewUtils.HeaderMessage + "]----------\n");
            for (int j = 0; j <= current; j++)
            {               
                progressMsg.append((j)+"-["+graphDBs[j]+"]\t Time "+
                        FullCheckType.ToolTypes.New_Fast_Consistency_Check.name()+"["+totalTimeTaken[j]+"] ms\n");
            }
            progressMsg.append("\t\t");
            for (int j = 0; j <= current; j++)
                progressMsg.append("\t"+(j));
            progressMsg.append("\n");
            for (int k = 0; k < FullCheckNewUtils.Stages.values().length; k++)
            {                                       
                progressMsg.append(FullCheckNewUtils.Stages.values()[k] + ":\t");
                for (int j = 0; j <= current; j++)
                {
                    Long[] times = FullCheckNewUtils.Stages.getTimes(j);                        
                    if (times[k] != null)
                        progressMsg.append(times[k].longValue());
                    progressMsg.append(",\t");
                }
                progressMsg.append("\n");
            }
            FullCheckNewUtils.printMessage(progressMsg.toString());
        }
    }

    public static void printTimes(long time)
    {
        if (!FullCheckNewUtils.isVerbose())
            return;
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < FullCheckNewUtils.TaskTimes.length; i++)
        {
            if (FullCheckNewUtils.TaskTimes[i] != null)
            {
                str.append(FullCheckNewUtils.TaskTimes[i]+"\n");
                FullCheckNewUtils.TaskTimes[i] = null;
            }
        }
        str.append("Total time taken ["+time+"]\n");
        FullCheckNewUtils.saveMessage( str.toString(), true );
        FullCheckNewUtils.printSavedMessage(true);
    }
    
    private static long getSum( long[] counts )
    {
        long sum = 0;
        for ( int i = 0; i < counts.length; i++ )
            sum += counts[i];
        return sum;
    }

    public static void setVerbose(boolean value)
    {
        Verbose = value;
    }
    public static boolean isVerbose()
    {
        return Verbose;
    }
    private static boolean Verbose = false;
    public static void printPostMessage( Stages stage )
    {
        if ( stage != null && (stage == Stages.Stage6_RS_Forward || stage == Stages.Stage7_RS_Backward) )
        {
            saveMessage( "Stats - Check [" + getSum( FullCheckNewUtils.Counts.checkedCount ) + "] Skip ["
                    + getSum( FullCheckNewUtils.Counts.skipCheckCount ) + "] Miss ["
                    + getSum( FullCheckNewUtils.Counts.missCheckCount ) + "] CSkip ["
                    + getSum( FullCheckNewUtils.Counts.correctSkipCheckCount ) + "] NoCache ["
                    + getSum( FullCheckNewUtils.Counts.noCacheSkipCount ) + "] Forced ["
                    + getSum( FullCheckNewUtils.Counts.skipBackupCount ) + "] Error ["
                    + getSum( FullCheckNewUtils.Counts.checkErrors ) + "]" );
            StringBuffer str = new StringBuffer();
            str.append( "Link Stats- forward[" + FullCheckNewUtils.Counts.forwardlinks + "] back["
                    + FullCheckNewUtils.Counts.backlinks + "] Null[" + FullCheckNewUtils.Counts.nulllinks + "] Checked " );
            for ( int i = 0; i < 4; i++ )
                str.append( "[" + i + ":" + getSum( FullCheckNewUtils.Counts.linksChecked[i] ) + "]" );
            saveMessage( str.toString() );
            saveMessage( "Cache Stats - Max [" + getSum( FullCheckNewUtils.Counts.maxCacheCount ) + "] NoCache ["
                    + getSum( FullCheckNewUtils.Counts.noCacheCount ) + "] Clear ["
                    + getSum( FullCheckNewUtils.Counts.clearCacheCount ) + "] Overwrite ["
                    + getSum( FullCheckNewUtils.Counts.overwriteCount ) + "]" );
            FullCheckNewUtils.Counts.initCount( FullCheckNewUtils.MAX_THREADS );
        }
    }

    // general utilities
    
    public static String getMaxIds( )
    {
        long[] highIds = new long[4];

        highIds[0] = neoStore.getPropertyStore().getHighId();
        highIds[1] = neoStore.getNodeStore().getHighId();
        highIds[2] = neoStore.getRelationshipStore().getHighId();
        highIds[3] = 0;//neoStore.getLabelTokenStore().getHighId();
       
        return ("HighIDs - Property[" + highIds[0] + "] Node[" + highIds[1] + "] Relationship[" + highIds[2] + "] Label["
                + highIds[3] + "]");
    }

    public static String getCurrentTimeStamp()
    {
        Calendar timeStamp = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault() );
        return timeStamp.getTime().toString() ;
    }
    
    
    public static final int KB = (int)Math.pow( 2, 10 );
    public static final int MB = (int)Math.pow( 2, 20 );
    public static final int GB = (int)Math.pow( 2, 30 );
    public static String memoryStats()
    {
        Runtime runtime = Runtime.getRuntime();
        StringBuilder str = new StringBuilder();
        str.append( "Memory stats[MB]" );
        str.append( "[Used:" + (runtime.totalMemory() - runtime.freeMemory()) / MB );
        str.append( "][Free:" + runtime.freeMemory() / MB );
        str.append( "][Total:" + runtime.totalMemory() / MB );
        str.append( "][Max:" + runtime.maxMemory() / MB + "]" );
        return str.toString();
    }
}
