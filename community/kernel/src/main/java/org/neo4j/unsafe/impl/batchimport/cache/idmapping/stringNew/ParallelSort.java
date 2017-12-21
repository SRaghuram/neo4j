/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.PrimitiveLongStack;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.Utils;
import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.stringNew.EncodingIdMapper.ByteArrayCache;

import static org.neo4j.helpers.Numbers.safeCastLongToInt;

/**
 * Sorts input data by dividing up into chunks and sort each chunk in parallel. Each chunk is sorted using a quick sort method, whereas the dividing of the data
 * is first sorted using radix sort.
 */
public class ParallelSort
{
    private final int PARALLEL_SORT_THRESHOLD = 0;
    private final int[] radixIndexCount;
    private final RadixCalculator radixCalculator;
    private final ByteArrayCache dataCache;
    private final long highestSetIndex;
    private final int threads;
    private final ProgressListener progress;
    private final Comparator comparator;

    public ParallelSort( Radix radix, ByteArrayCache dataCache, long highestSetIndex, int threads, ProgressListener progress, Comparator comparator )
    {
        this.progress = progress;
        this.comparator = comparator;
        this.radixIndexCount = radix.getRadixIndexCounts();
        this.radixCalculator = radix.calculator();
        this.dataCache = dataCache;
        this.highestSetIndex = highestSetIndex;
        this.threads = threads;
    }

    public synchronized long[][] run() throws InterruptedException
    {
        long[][] sortParams;
        long[][] rangeDetails = null;
        if ( highestSetIndex < PARALLEL_SORT_THRESHOLD )
        {
            sortParams = new long[][] { { 0, highestSetIndex } };
        }
        else
        {
            rangeDetails = getRadixParams( threads  );
            sortParams = sortRadix( rangeDetails );
        }
        int threadsNeeded = 0;
        for ( int i = 0; i < sortParams.length && i < threads; i++ )
        {
            if ( sortParams[i][1] == 0 )
            {
                break;
            }
            threadsNeeded++;
        }
        Workers<SortWorker> sortWorkers = new Workers<>( "SortWorker" );
        progress.started( "SORT"  );
        for ( int i = 0; i < threadsNeeded; i++ )
        {
            if ( sortParams[i][1] == 0 )
            {
                break;
            }
            sortWorkers.start( new SortWorker( dataCache, comparator, sortParams[i][0], sortParams[i][1], progress ) );
        }
        try
        {
            sortWorkers.awaitAndThrowOnError( RuntimeException.class );
        }
        finally
        {
            progress.done();
        }
        return rangeDetails;
    }

    private long[][] getRadixParams( int bucketCount )
    {
        long[][] rangeParams = new long[bucketCount][4];
        long dataSize = highestSetIndex + 1;
        long bucketSize = dataSize / bucketCount;
        long count = 0, fullCount = 0;

        for ( int i = 0, threadIndex = 0; i < radixIndexCount.length && threadIndex < bucketCount; i++ )
        {
            if ( ( count + radixIndexCount[i] ) > bucketSize )
            {
                rangeParams[threadIndex][2] = count == 0 ? i : i - 1;
                rangeParams[threadIndex][0] = fullCount;
                rangeParams[threadIndex][3] = fullCount;
                if ( count != 0 )
                {
                    rangeParams[threadIndex][1] = count;
                    fullCount += count;
                    count = radixIndexCount[i];
                }
                else
                {
                    rangeParams[threadIndex][1] = radixIndexCount[i];
                    fullCount += radixIndexCount[i];
                }
                threadIndex++;
            }
            else
            {
                count += radixIndexCount[i];
            }
            if ( threadIndex == bucketCount - 1 || i == radixIndexCount.length - 1 )
            {
                rangeParams[threadIndex][2] = radixIndexCount.length;
                rangeParams[threadIndex][0] = fullCount;
                rangeParams[threadIndex][1] = dataSize - fullCount;
                break;
            }
        }
        return rangeParams;
    }

    private long[][] sortRadix( long[][] rangeDetails ) throws InterruptedException
    {
        progress.started( "SPLIT" );
        Workers<TrackerInitializer> initializers = new Workers<>( "TrackerInitializer" );
        int rangeSize = threads;

        buckets = new Bucket[threads];
        int[] prevPartSizes = null, partSizes = new int[] { threads };
        for ( int parts = 2; parts < threads * 2; parts = parts * 2 )
        {
            // initialize the buckets
            for ( int i = 0; i < threads; i++ )
            {
                buckets[i] = new Bucket( rangeDetails[i][0], rangeDetails[i][1] );
            }
            prevPartSizes = partSizes;
            partSizes = new int[parts];
            for ( int i = 0; i < parts; )
            {
                partSizes[i] = prevPartSizes[i / 2] % 2 == 0 ? prevPartSizes[i / 2] / 2 : prevPartSizes[i / 2] / 2 + 1;
                partSizes[i + 1] = prevPartSizes[i / 2] / 2;
                i = i + 2;
            }

            int offset = 0;
            for ( int i = 0; i < parts / 2; i++ )
            {
                if ( partSizes[2 * i] + partSizes[2 * i + 1] <= 1 )
                {
                    offset += partSizes[2 * i] + partSizes[2 * i + 1];
                    continue;
                }
                int[] params = new int[7];
                params[0] = offset;
                params[1] = offset + partSizes[2 * i] - 1;
                params[2] = params[1] + 1;
                params[3] = params[2] + partSizes[2 * i + 1] - 1;
                params[6] = i * rangeSize;
                for ( int threadIndex = 0; threadIndex < partSizes[2 * i] + partSizes[2 * i + 1]; threadIndex++ )
                {
                    params[4] = params[0] + threadIndex / 2;
                    params[5] = ( params[4] + partSizes[2 * i] >= params[0] + partSizes[2 * i] + partSizes[2 * i + 1] ) ? params[4] + partSizes[2 * i] - 1
                            : params[4] + partSizes[2 * i];
                    initializers.start( new TrackerInitializer( params, rangeDetails, buckets ) );
                }
                offset = params[3] + 1;
            }
            Throwable error = initializers.await();
            rangeSize = rangeSize / 2 + rangeSize % 2;
        }
        progress.done();
        return rangeDetails;
    }

    /**
     * Pluggable comparator for the comparisons that quick-sort needs in order to function.
     */
    public interface Comparator
    {
        /**
         * @return {@code true} if {@code left} is less than {@code pivot}.
         */
        boolean lt( long left, long pivot );

        /**
         * @return {@code true} if {@code right} is greater than or equal to {@code pivot}.
         */
        boolean ge( long right, long pivot );

        /**
         * @param dataValue
         *            the data value in the used dataCache for a given tracker index.
         * @return actual data value given the data value retrieved from the dataCache at a given index. This is exposed to be able to introduce an indirection
         *         while preparing the tracker indexes just like the other methods on this interface does.
         */
        long dataValue( long dataValue );
    }

    public static final Comparator DEFAULT = new Comparator()
    {
        @Override
        public boolean lt( long left, long pivot )
        {
            return Utils.unsignedCompare( left, pivot, CompareType.LT );
        }

        @Override
        public boolean ge( long right, long pivot )
        {
            return Utils.unsignedCompare( right, pivot, CompareType.GE );
        }

        @Override
        public long dataValue( long dataValue )
        {
            return dataValue;
        }
    };

    /**
     * Sorts a part of data in dataCache covered by trackerCache. Values in data cache doesn't change location, instead trackerCache is updated to point to the
     * right indexes. Only touches a designated part of trackerCache so that many can run in parallel on their own part without synchronization.
     */
    public static class SortWorker implements Runnable
    {
        private final long start, size;
        private int threadLocalProgress;
        private final long[] pivotChoice = new long[10];
        private final ThreadLocalRandom random = ThreadLocalRandom.current();
        private ByteArrayCache dataCache;
        private Comparator comparator;
        private ProgressListener progress;
        private int valueLength = 8;
        private int offset;

        SortWorker( ByteArrayCache dataCache, int valueLength, int offset, Comparator comparator, long startRange, long size, ProgressListener progress )
        {
            this.start = startRange;
            this.size = size;
            this.dataCache = dataCache;
            this.comparator = comparator;
            this.progress = progress;
            this.valueLength = valueLength;
            this.offset = offset;
        }

        SortWorker( ByteArrayCache dataCache, Comparator comparator, long startRange, long size, ProgressListener progress )
        {
            this.start = startRange;
            this.size = size;
            this.dataCache = dataCache;
            this.comparator = comparator;
            this.progress = progress;
        }

        void incrementProgress( long diff )
        {
            threadLocalProgress += diff;
            if ( threadLocalProgress >= 10_000 /* reasonably big to dwarf passing a memory barrier */ )
            { // Update the
              // total
              // progress
                reportProgress();
            }
        }

        private void reportProgress()
        {
            progress.add( threadLocalProgress );
            threadLocalProgress = 0;
        }

        @Override
        public void run()
        {
            qsort( start, start + size - 1 );
            reportProgress();
        }

        private long partition( long leftIndex, long rightIndex, long pivotIndex )
        {
            long li = leftIndex, ri = rightIndex - 1, pi = pivotIndex;
            long pivot = dataCache.getValue( valueLength, pi, offset );
            // save pivot in last index
            dataCache.byteArray.swap( pi, rightIndex );

            long left = dataCache.getValue( valueLength, li, offset );
            long right = dataCache.getValue( valueLength, ri, offset );
            while ( li < ri )
            {
                if ( comparator.lt( left, pivot ) )
                { // this value is on the correct side of the pivot, moving on
                    left = dataCache.getValue( valueLength, ++li, offset );
                }
                else if ( comparator.ge( right, pivot ) )
                { // this value is on the correct side of the pivot, moving on
                    right = dataCache.getValue( valueLength, --ri, offset );
                }
                else
                { // this value is on the wrong side of the pivot, swapping
                    dataCache.byteArray.swap( li, ri );// dataCache.swap( li, ri );
                    long temp = left;
                    left = right;
                    right = temp;
                }
            }
            long partingIndex = ri;
            if ( comparator.lt( right, pivot ) )
            {
                partingIndex++;
            }
            // restore pivot
            dataCache.byteArray.swap( rightIndex, partingIndex );
            return partingIndex;
        }

        private void qsort( long initialStart, long initialEnd )
        {
            PrimitiveLongStack stack = new PrimitiveLongStack( 100 );
            stack.push( initialStart );
            stack.push( initialEnd );
            while ( !stack.isEmpty() )
            {
                long end = stack.poll();
                long start = stack.poll();
                long diff = end - start + 1;
                if ( diff < 2 )
                {
                    incrementProgress( 2 );
                    long left = dataCache.getValue( valueLength, start, offset );
                    long right = dataCache.getValue( valueLength, end, offset );
                    if ( left > right && right != 0 )
                     {
                        dataCache.byteArray.swap( start, end );// EncodingIdMapper.dataCacheSwap(dataCache, start, end);
                    }
                    continue;
                }

                incrementProgress( 1 );

                // choose a random pivot between start and end
                long pivot = start + random.nextLong( diff );
                pivot = informedPivot( start, end, pivot );

                // partition, given that pivot
                pivot = partition( start, end, pivot );
                if ( pivot > start )
                { // there are elements to left of pivot
                    stack.push( start );
                    stack.push( pivot );
                }
                if ( pivot + 1 < end )
                { // there are elements to right of pivot
                    stack.push( pivot + 1 );
                    stack.push( end );
                }
            }
        }

        private long informedPivot( long start, long end, long randomIndex )
        {
            if ( end - start < pivotChoice.length )
            {
                return randomIndex;
            }

            long low = Math.max( start, randomIndex - 5 );
            long high = Math.min( low + 10, end );
            int length = safeCastLongToInt( high - low );

            int j = 0;
            for ( long i = low; i < high; i++, j++ )
            {
                pivotChoice[j] = dataCache.getValue( valueLength, i, offset );
            }
            Arrays.sort( pivotChoice, 0, length );

            long middle = pivotChoice[length / 2];
            for ( long i = low; i <= high; i++ )
            {
                if ( dataCache.getValue( valueLength, i, offset ) == middle )
                {
                    return i;
                }
            }
            throw new IllegalStateException( "The middle value somehow disappeared in front of our eyes" );
        }
    }

    private class Bucket
    {
        long index;

        Bucket( long start, long size )
        {
            this.index = start;
        }
    }

    Bucket[] buckets;

    private class TrackerInitializer implements Runnable
    {
        long[][] rangeParams;
        long breakValue;
        int leftFirst, leftLast, rightFirst, rightLast;
        int[] currentBucket = new int[2], startBucket = new int[2];
        long[] currentIndex = new long[2];
        Bucket[] bucketArray;

        TrackerInitializer( int[] bucketParams, long[][] rangeParams, Bucket[] buckets )
        {

            leftFirst = bucketParams[0];
            leftLast = bucketParams[1];
            rightFirst = bucketParams[2];
            rightLast = bucketParams[3];
            startBucket[0] = bucketParams[4];
            startBucket[1] = bucketParams[5];
            currentBucket[0] = startBucket[0];
            currentBucket[1] = startBucket[1];
            currentIndex[0] = rangeParams[startBucket[0]][0];
            currentIndex[1] = rangeParams[startBucket[1]][0];
            this.breakValue = rangeParams[leftLast][2];
            this.bucketArray = buckets;
            this.rangeParams = rangeParams;
        }

        private long ownIndex( int bucket )
        {
            synchronized ( bucketArray[bucket] )
            {
                long value = bucketArray[bucket].index++;
                return value;
            }
        }

        private long getNextIndex( boolean isLeft )
        {
            int index = isLeft ? 0 : 1;
            while ( true )
            {
                // first try to own an index value in the current bucket
                currentIndex[index] = ownIndex( currentBucket[index] );
                // if it is out of range for the bucket then go to next bucket
                if ( currentIndex[index] < rangeParams[currentBucket[index]][0] + rangeParams[currentBucket[index]][1] )
                {
                    // get the radix
                    int radix = radixCalculator.radixOf( dataCache.getValue( currentIndex[index] ) );
                    // if it is candidate then return value else continue to next
                    if ( ( index == 0 && radix > breakValue) || (index == 1 && radix <= breakValue ) )
                    {
                        return currentIndex[index];
                    }
                }
                else
                {
                    // go to next bucket
                    if ( index == 0 )
                    {
                        if ( ++currentBucket[index] > leftLast )
                        {
                            currentBucket[index] = leftFirst;
                        }
                    }
                    else
                    {
                        if ( ++currentBucket[index] > rightLast )
                        {
                            currentBucket[index] = rightFirst;
                        }
                    }
                    // currentIndex[index] = bucketArray[currentBucket[index]].index;
                    if ( ( index == 0 && currentBucket[0] == startBucket[0]) || (index == 1 && currentBucket[1] == startBucket[1] ) )
                    {
                        break;
                    }
                }
            }
            return -1;
        }

        private long verify()
        {
            long startLeft = rangeParams[leftFirst][0];
            long endLeft = rangeParams[rightFirst][0];
            int errLeft = 0, errRight = 0;
            for ( long i = startLeft; i < endLeft; i++ )
            {
                int radix = radixCalculator.radixOf( dataCache.getValue( i ) );
                // if it is candidate then return value else continue to next
                if ( radix > breakValue )
                {
                    errLeft++;
                }
            }
            long startRight = rangeParams[rightFirst][0];
            long endRight = rangeParams[rightLast][0] + rangeParams[rightLast][1];
            for ( long i = startRight; i < endRight; i++ )
            {
                int radix = radixCalculator.radixOf( dataCache.getValue( i ) );
                // if it is candidate then return value else continue to next
                if ( radix <= breakValue )
                {
                    errRight++;
                }
            }
            return errLeft + errRight;
        }

        @Override
        public void run()
        {
            int count = 0;
            while ( true )
            {
                long leftIndex = getNextIndex( true );
                if ( leftIndex == -1 )
                {
                    break;
                }
                long rightIndex = getNextIndex( false );
                if ( rightIndex == -1 )
                {
                    System.out.println( "Orphaned Left index [" + leftIndex + "] Count [" + count + "]" );
                    break;
                }
                dataCache.byteArray.swap( leftIndex, rightIndex );
                count++;
            }
            /*long err = verify();
            if ( err != 0 )
            {
                verify();
                String errStr = "Error[" + err + "]";
                System.out.println( "Exiting thread[" + startBucket[0] + "," + startBucket[1] + "][" + count + "]   " + errStr );
            }*/
        }
    }
}
