/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;

import static org.neo4j.io.IOUtils.closeAll;

public abstract class CloseableFactory implements Closeable
{
    private final LinkedList<AutoCloseable> closeableCollection = new LinkedList<>();

    protected AutoCloseable[] getCloseablesInReverseOrder()
    {
        synchronized ( closeableCollection )
        {
            var output = new AutoCloseable[closeableCollection.size()];
            int i = 0;
            while ( !closeableCollection.isEmpty() )
            {
                output[i++] = closeableCollection.removeLast();
            }
            return output;
        }
    }

    protected <T extends AutoCloseable> T addCloseable( T driver )
    {
        synchronized ( closeableCollection )
        {
            closeableCollection.add( driver );
        }
        return driver;
    }

    @Override
    public void close() throws IOException
    {
        // Shut down AutoCloseables in reverse order from the order they were created.
        //
        // Why? Here's a specific example. We do this:
        //
        //        addCloseable(new Driver(logFile=addCloseable(new FileOutputStream("my.log")))
        //
        // the List<AutoCloseable> looks like:
        //
        //        1: FileOutputStream@my.log
        //        2: Driver(log=FileOutputStream@my.log)
        //
        // if we close the FileOutputStream *first* when the driver tries to write to the log during its own close() method an error is thrown :-(

        closeAll( getCloseablesInReverseOrder() );
    }
}
