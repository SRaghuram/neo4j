/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.io.pagecache.randomharness;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.io.pagecache.PagedFile;

class Plan
{
    private final Action[] plan;
    private final Map<Path,PagedFile> fileMap;
    private final List<Path> mappedFiles;
    private final Set<Path> filesTouched;
    private final long[] executedByThread;
    private final AtomicInteger actionCounter;
    private final CountDownLatch startLatch;

    Plan( Action[] plan, Map<Path,PagedFile> fileMap, List<Path> mappedFiles, Set<Path> filesTouched )
    {
        this.plan = plan;
        this.fileMap = fileMap;
        this.mappedFiles = mappedFiles;
        this.filesTouched = filesTouched;
        executedByThread = new long[plan.length];
        Arrays.fill( executedByThread, -1 );
        actionCounter = new AtomicInteger();
        startLatch = new CountDownLatch( 1 );
    }

    public void start()
    {
        startLatch.countDown();
    }

    public Action next() throws InterruptedException
    {
        startLatch.await();
        int index = actionCounter.getAndIncrement();
        if ( index < plan.length )
        {
            executedByThread[index] = Thread.currentThread().getId();
            return plan[index];
        }
        return null;
    }

    public void close()
    {
        for ( Path mappedFile : mappedFiles )
        {
            PagedFile pagedFile = fileMap.get( mappedFile );
            if ( pagedFile != null )
            {
                pagedFile.close();
            }
        }
    }

    public void print( PrintStream out )
    {
        out.println( "Plan: [thread; action]" );
        for ( int i = 0; i < plan.length; i++ )
        {
            long threadId = executedByThread[i];
            out.printf( "  % 3d : %s%n", threadId, plan[i] );
            if ( threadId == -1 )
            {
                break;
            }
        }
    }

    public Set<Path> getFilesTouched()
    {
        return filesTouched;
    }
}
