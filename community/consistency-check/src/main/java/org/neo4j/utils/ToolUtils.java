package org.neo4j.utils;

import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;

import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.unsafe.impl.batchimport.Utils;
import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;

public class ToolUtils
{
    static public int Count16K = (int)Math.pow(2,14);
    static public int Count32K = (int)Math.pow(2,15);
    static public int Count64K = (int)Math.pow(2,16);
    public static int MAX_THREADS = Runtime.getRuntime().availableProcessors() - 1;
    static public long startTime = System.currentTimeMillis();
    
    public static void printMessage(String msg )
    {
        printMessage( null, msg, true, false);
    }
    public static void printMessage( PrintWriter logFile, String message, boolean noConsoleMsgs )
    {
        printMessage( logFile, message, false);
    }

    public static void printMessage( PrintWriter logFile, String message, boolean newLine, boolean noConsoleMsgs )
    {
        String msg = (newLine ? "\n" : "") +
                "[" + getCurrentTimeStamp() + "][" + (System.currentTimeMillis() - startTime) / 1000 + "] "
                        + message;
        if ( logFile != null )
        {
            logFile.println( msg );
            logFile.flush();
        }
        if ( !noConsoleMsgs )
            System.out.println( msg );
    }

    public static String buildMessage( String message )
    {
        return "[" + getCurrentTimeStamp() + "][" + (System.currentTimeMillis() - startTime) / 1000 + "] "
                + message + "\n";
    }

    private static StringBuilder cachedMsg = new StringBuilder();

    public static void saveMessage( String msg  )
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
    
    public static void printSavedMessage( boolean forcePrint )
    {
        printSavedMessage(null, forcePrint, false, true );
    }

    public static void printSavedMessage(PrintWriter logFile, boolean noConsoleMsgs, boolean verbose)
    {
        printSavedMessage(logFile, false, noConsoleMsgs, verbose);
    }
    public static void printSavedMessage(PrintWriter logFile, boolean forcePrint, boolean noConsoleMsgs, boolean verbose)
    {
        if (!verbose && !forcePrint)
            return;
        if ( logFile != null )
        {
            logFile.print( cachedMsg.toString() );
            logFile.flush();
        }
        if ( !noConsoleMsgs )
            System.out.print( "\n" + cachedMsg.toString() );
        cachedMsg.setLength( 0 );
    }

    public static void clearMessage()
    {
        cachedMsg.setLength( 0 );
    }
    
    public static String getCurrentTimeStamp()
    {
        Calendar timeStamp = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault() );
        return timeStamp.getTime().toString() ;
    }
    public static String getMaxIds( NeoStore neoStore )
    {
        long[] highIds = new long[4];

        highIds[0] = neoStore.getPropertyStore().getHighId();
        highIds[1] = neoStore.getNodeStore().getHighId();
        highIds[2] = neoStore.getRelationshipStore().getHighId();
        highIds[3] = 0;//neoStore.getLabelTokenStore().getHighId();
       
        return ("HighIDs - Property[" + highIds[0] + "] Node[" + highIds[1] + "] Relationship[" + highIds[2] + "] Label["
                + highIds[3] + "]");
    }
    
    //------sort----------
    private static final long firstIntMask = 0xFFFF_FFFF_0000_0000l;
    private static final long secondIntMask = ~firstIntMask;
    public static void setIndex(long[] indexValueArray)
    {
        for (int i = 0; i < indexValueArray.length; i++)
        {
            long i1 = (long)i << 32;
            indexValueArray[i] = i1 | indexValueArray[i];
        }
    }
    
    public static int getIndex(long val)
    {
        return (int)((val & firstIntMask) >> 32);
    }
    public static int getValue(long val)
    {
        return (int)(val & secondIntMask);
    }
    
    public static long setIndex(long value, int index)
    {
        value = value & secondIntMask;
        return value | ((long)index << 32);
    }
    public static long setvalue(long value, int val)
    {
        value = value & firstIntMask;
        return value | val;
    }
    public static boolean lt( long left, long pivot )
    {
        return Utils.unsignedCompare( left, pivot, CompareType.LT );
    }
    public static boolean ge( long right, long pivot )
    {
        return Utils.unsignedCompare( right, pivot, CompareType.GE );
    }
    
    public static void swap(long[] array, int i1, int i2)
    {
        long temp = array[i1];
        array[i1] = array[i2];
        array[i2] = temp; 
    }
    
    private static int partition( long[] indexValueArray, int leftIndex, int rightIndex, int pivotIndex )
    {
        int li = leftIndex, ri = rightIndex - 2, pi = pivotIndex;
        long pivot = getValue(indexValueArray[pi]);
        // save pivot in last index
        swap( indexValueArray, pi, rightIndex - 1 );
        long left = getValue(indexValueArray[li]);
        long right = getValue(indexValueArray[ri]);
        while ( li < ri )
        {
            if ( lt( left, pivot ) )
            {   // this value is on the correct side of the pivot, moving on
                left = getValue(indexValueArray[++li]);
            }
            else if ( ge( right, pivot ) )
            {   // this value is on the correct side of the pivot, moving on
                right = getValue(indexValueArray[--ri]);
            }
            else
            {   // this value is on the wrong side of the pivot, swapping
                swap( indexValueArray, li, ri );
                long temp = left;
                left = right;
                right = temp;
            }
        }
        int partingIndex = ri;
        if (lt( right, pivot ) )
        {
            partingIndex++;
        }
        // restore pivot
        swap( indexValueArray, rightIndex - 1, partingIndex );
        return partingIndex;
    }
    
    public static void sortIndexValueArray( long[] indexValueArray, int startValue, int endValue)
    {
        sortIndexValueArray( indexValueArray, startValue, endValue, true );
    }

    public static void sortIndexValueArray( long[] indexValueArray, int startValue, int endValue, boolean setIndex)
    {
        if (setIndex)
            setIndex(indexValueArray);
        PrimitiveIntStack stack = new PrimitiveIntStack( 100 );
        Random random = new Random(System.currentTimeMillis());
        stack.push( startValue );
        stack.push( endValue );
        while ( !stack.isEmpty() )
        {
            int end = stack.poll();
            int start = stack.poll();
            int diff = end - start;
            if ( diff < 2 )
            {
                continue;
            }
            // choose a random pivot between start and end
            int pivot = start + random.nextInt( diff );

            // partition, given that pivot
            pivot = partition( indexValueArray, start, end, pivot );
            if ( pivot > start )
            {   // there are elements to left of pivot
                stack.push( start );
                stack.push( pivot );
            }
            if ( pivot + 1 < end )
            {   // there are elements to right of pivot
                stack.push( pivot + 1 );
                stack.push( end );
            }
        }
    }
    //------------sort------------------
}
