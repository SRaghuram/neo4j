/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.org.neo4j.index;

import com.neo4j.tools.input.Command;
import com.neo4j.tools.input.ConsoleInput;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.index.internal.gbptree.SimpleLongLayout;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;

import static com.neo4j.tools.input.ConsoleUtil.staticPrompt;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class GBPTreePlayground
{
    private final File indexFile;
    private GBPTree<MutableLong,MutableLong> tree;

    private final int pageSize = 256;
    private final PageCache pageCache;
    private final SimpleLongLayout layout;
    private final MutableBoolean autoPrint = new MutableBoolean( true );

    private GBPTreePlayground( FileSystemAbstraction fs, File indexFile )
    {
        this.indexFile = indexFile;
        this.layout = SimpleLongLayout.longLayout().build();
        this.pageCache = StandalonePageCacheFactory.createPageCache( fs, createInitialisedScheduler(), pageSize );
    }

    private void setupIndex()
    {
        tree = new GBPTreeBuilder<>( pageCache, indexFile, layout ).build();
    }

    private void run() throws InterruptedException
    {
        System.out.println( "Working on: " + indexFile.getAbsolutePath() );
        setupIndex();

        LifeSupport life = new LifeSupport();
        ConsoleInput consoleInput = life.add( new ConsoleInput( System.in, System.out, staticPrompt( "# " ) ) );
        consoleInput.add( "print", new Print() );
        consoleInput.add( "add", new AddCommand() );
        consoleInput.add( "remove", new RemoveCommand() );
        consoleInput.add( "cp", new Checkpoint() );
        consoleInput.add( "autoprint", new ToggleAutoPrintCommand() );
        consoleInput.add( "restart", new RestartCommand() );
        consoleInput.add( "state", new PrintStateCommand() );
        consoleInput.add( "cc", new ConsistencyCheckCommand() );

        life.start();
        try
        {
            consoleInput.waitFor();
        }
        finally
        {
            life.shutdown();
        }
    }

    private class Print implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            tree.printTree( NULL );
        }

        @Override
        public String toString()
        {
            return "Print tree";
        }
    }

    private class PrintStateCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            tree.printState( NULL );
        }

        @Override
        public String toString()
        {
            return "Print state of tree";
        }
    }

    private class Checkpoint implements Command
    {
        @Override
        public void run( String[] args, PrintStream out )
        {
            tree.checkpoint( IOLimiter.UNLIMITED, NULL );
        }
        @Override
        public String toString()
        {
            return "Checkpoint tree";
        }
    }

    private class AddCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            Long[] longValues = new Long[args.length];
            for ( int i = 0; i < args.length; i++ )
            {
                longValues[i] = Long.valueOf( args[i] );
            }
            MutableLong key = new MutableLong();
            MutableLong value = new MutableLong();
            try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
            {
                for ( Long longValue : longValues )
                {
                    key.setValue( longValue );
                    value.setValue( longValue );
                    writer.put( key, value );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            maybePrint();
        }
        @Override
        public String toString()
        {
            return "N [N ...] (add key N)";
        }
    }

    private class RemoveCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            Long[] longValues = new Long[args.length];
            for ( int i = 0; i < args.length; i++ )
            {
                longValues[i] = Long.valueOf( args[i] );
            }
            MutableLong key = new MutableLong();
            try ( Writer<MutableLong,MutableLong> writer = tree.writer( NULL ) )
            {
                for ( Long longValue : longValues )
                {
                    key.setValue( longValue );
                    writer.remove( key );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            maybePrint();
        }
        @Override
        public String toString()
        {
            return "N [N ...] (remove key N)";
        }
    }

    private class ToggleAutoPrintCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out )
        {
            if ( autoPrint.isTrue() )
            {
                autoPrint.setFalse();
            }
            else
            {
                autoPrint.setTrue();
            }
        }
        @Override
        public String toString()
        {
            return "Toggle auto print after modifications (ON by default)";
        }
    }

    private class RestartCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            System.out.println( "Closing tree..." );
            tree.close();
            System.out.println( "Starting tree..." );
            setupIndex();
            System.out.println( "Tree started!" );
        }
        @Override
        public String toString()
        {
            return "Close and open gbptree. No checkpoint is performed.";
        }
    }

    private class ConsistencyCheckCommand implements Command
    {
        @Override
        public void run( String[] args, PrintStream out ) throws Exception
        {
            System.out.println( "Checking consistency..." );
            tree.consistencyCheck( NULL );
            System.out.println( "Consistency check finished!");
        }

        @Override
        public String toString()
        {
            return "Check consistency of GBPTree";
        }
    }

    private void maybePrint() throws IOException
    {
        if ( autoPrint.getValue() )
        {
            print();
        }
    }

    private void print() throws IOException
    {
        tree.printTree( NULL );
    }

    public static void main( String[] args ) throws InterruptedException
    {
        File indexFile;
        if ( args.length > 0 )
        {
            indexFile = new File( args[0] );
        }
        else
        {
            indexFile = new File( "index" );
        }

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        new GBPTreePlayground( fs, indexFile ).run();
    }
}
