/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.utils.runutils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.utils.ToolUtils;

public class RecordScanner<RECORD> implements StoppableRunnable
{
    private final ProgressListener progress;
    private final BoundedIterable<RECORD> store;
    private final RecordProcessor<Abstract64BitRecord> processor;
    private final CommandProcessor[] commands;
    private final String taskName;
    private boolean parallel = false;
    private int iteration = 1;

    private volatile boolean continueScanning = true;

    public RecordScanner( BoundedIterable<RECORD> store,
                          String taskName,
                          ProgressMonitorFactory.MultiPartBuilder builder,
                          RecordProcessor<Abstract64BitRecord> processor,
                          CommandProcessor... commands)
    {
        this.store = store;
        this.processor = processor;
        this.commands = commands;
        this.progress = builder.progressForPart( taskName, store.maxCount() );
        this.taskName = taskName;
    }
    
    public void run()
    {
        run( parallel );
    }
    
    public void setParallel(boolean parallel){
        this.parallel = parallel;
    }
    
    public void setIteration(int iteration)
    {
        this.iteration = iteration;
    }

    public void run(boolean parallel)
    {
        long start = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++)
        {
            preProcess();
        	if (!parallel)
        		runSequential();
        	else
        		runParallel();
        	postProcess();
        }
    }
    
    public void runSequential()
    {
    	threadIndex.set(0);
        try
        {
            int entryCount = 0;
            for ( RECORD record : store )
            {
                if ( !continueScanning )
                {
                    return;
                }
                processor.process( (Abstract64BitRecord)record );
                progress.set( entryCount++ );
            }
        }
        finally
        {
            try
            {
                store.close();
            }
            catch ( Exception e )
            {
                progress.failed( e );
            }
            processor.close();
            progress.done();
        }
    }

	public static int MAX_THREADS = Runtime.getRuntime().availableProcessors() - 1;
	public static int QSIZE = 1000;
	private static long recordsPerCPU = 0;
    public void runParallel()
    {
		RecordCheckWorker<PrimitiveRecord>[] worker = new RecordCheckWorker[MAX_THREADS];
		ArrayBlockingQueue<RECORD>[] recordQ = new ArrayBlockingQueue[MAX_THREADS];
		CountDownLatch startLatch = new CountDownLatch( 1 );
		CountDownLatch endLatch = new CountDownLatch( MAX_THREADS );
		for (int threadId = 0; threadId < MAX_THREADS; threadId++)
		{
			recordQ[threadId] = new ArrayBlockingQueue<RECORD>(QSIZE);
			worker[threadId] = new RecordCheckWorker(threadId, startLatch, endLatch,
					recordQ, store, processor);
			worker[threadId].start();
		}
		try
        {
			startLatch.countDown();
			recordsPerCPU = (store.maxCount()/MAX_THREADS) + 1;
	    	ToolUtils.saveMessage("Max Threads["+MAX_THREADS+"] Recs/CPU["+recordsPerCPU+"]");
	    	int[] recsProcessed = new int[MAX_THREADS];
	    	int total = 0, qIndex = 0;
	    	int entryCount = 0;
	    	long previousID = -1;
	    	int notInUse = 0;
	    	for ( RECORD record : store )
	        {
	    		if (record instanceof PrimitiveRecord)
	    		{
	    			long id = ((PrimitiveRecord)record).getId();
	    			notInUse += id - (previousID + 1);
	    			previousID = id;
	    		}
	        	total++;
	        	try 
	        	{
	        		// do a round robin distribution to maintain physical locality
	        		recordQ[qIndex++].put(record);
	        		qIndex %= MAX_THREADS;
	        		recsProcessed[qIndex]++; 		
	        	} catch (Exception e)
	        	{
	        		System.out.println("ERROR:"+e.getMessage());
	        		break;
	        	}
	            progress.set( entryCount++ );
	        }
	        StringBuffer strBuf = new StringBuffer();
	        for (int i = 0; i < MAX_THREADS; i++)
	        	strBuf.append("Q"+i+"["+recsProcessed[i]+"] ");
	        ToolUtils.saveMessage("Total["+total+"] NotInUse["+notInUse+"] "+strBuf.toString());
	        ToolUtils.saveMessage("FinalID:"+previousID);
	        progress.done();
			//--
            for (int threadId = 0; threadId < MAX_THREADS; threadId++)
            	worker[threadId].isDone();
            endLatch.await();
            processor.close();
        }
        catch ( Exception e )
        {
            System.out.println("runParallel:"+e.getMessage());
        }
    }
    
	public static final ThreadLocal<Integer> threadIndex = new ThreadLocal<>();
    private class RecordCheckWorker<PrimitiveRecord> extends java.lang.Thread
    {
        private final int threadId;
        private final CountDownLatch waitSignal;
        private final CountDownLatch waitEndSignal;
        public boolean done = false;
        private int threadLocalProgress;
        public ArrayBlockingQueue<RECORD>[] recordsQ =null;

        BoundedIterable<RECORD> store;
        RecordProcessor<PrimitiveRecord> processor;
        RecordCheckWorker( int threadId,  CountDownLatch wait, CountDownLatch waitEnd, ArrayBlockingQueue<RECORD>[] recordsQ,
        		BoundedIterable<RECORD> store,
                RecordProcessor<PrimitiveRecord> processor )
        {
        	this.threadId = threadId;
            this.recordsQ = recordsQ;
            this.waitSignal = wait;
            this.store = store;
            this.processor = processor;
            this.waitEndSignal = waitEnd;
        }

        void incrementProgress( int diff )
        {
            threadLocalProgress += diff;
            if ( threadLocalProgress == 10_000 /*reasonably big to dwarf passing a memory barrier*/ )
            {   // Update the total progress
                reportProgress();
            }
        }

        private void reportProgress()
        {
            //progress.add( threadLocalProgress );
            threadLocalProgress = 0;
        }
        
        public void isDone()
        {
        	done = true;
        }

        @Override
        public void run()
        {
            try
            {
            	threadIndex.set(threadId);
                waitSignal.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            //recursiveQsort( start, start + size, random, this );
            while (!done || !recordsQ[threadId].isEmpty())
            {
            	try {
            		RECORD record = recordsQ[threadId].poll(2000, TimeUnit.MILLISECONDS);
            		if (record != null)
            			processor.process( (PrimitiveRecord)record );
            	} catch (Exception ie)
            	{
            		System.out.println("Record Scanner:"+ie.getMessage());
            	}
            }
            reportProgress();
            waitEndSignal.countDown();
        }
    }
    
    @Override
    public void stopScanning()
    {
        continueScanning = false;
    }
    
    @Override
    public String getName()
    {
        // TODO Auto-generated method stub
        return taskName;
    }

    @Override
    public void preProcess()
    {
        if (commands.length > 0 && commands[0] != null)
            commands[0].runCommand(this);      
    }

    @Override
    public void postProcess()
    {
        if (commands.length > 1 && commands[1] != null)
            commands[1].runCommand(this);
        
    }
}
