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
package org.neo4j.tooling;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Deserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;

import static org.neo4j.io.ByteUnit.mebiBytes;

public class CsvOutput implements BatchImporter
{
    private final File targetDirectory;
    private final Header nodeHeader;
    private final Header relationshipHeader;
    private final Configuration config;

    public CsvOutput( File targetDirectory, Header nodeHeader, Header relationshipHeader, Configuration config )
    {
        this.targetDirectory = targetDirectory;
        this.config = config;
        assert targetDirectory.isDirectory();
        this.nodeHeader = nodeHeader;
        this.relationshipHeader = relationshipHeader;
        targetDirectory.mkdirs();
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
        consume( "nodes", input.nodes(), nodeHeader, ( node, deserialization ) ->
        {
            deserialization.clear();
            for ( Header.Entry entry : nodeHeader.entries() )
            {
                switch ( entry.type() )
                {
                case ID:
                    deserialization.handle( entry, node.id() );
                    break;
                case PROPERTY:
                    deserialization.handle( entry, property( node, entry.name() ) );
                    break;
                case LABEL:
                    deserialization.handle( entry, node.labels() );
                    break;
                default: // ignore other types
                }
            }
            return deserialization.materialize();
        } );
        consume( "relationships", input.relationships(), relationshipHeader, ( relationship, deserialization ) ->
        {
            deserialization.clear();
            for ( Header.Entry entry : relationshipHeader.entries() )
            {
                switch ( entry.type() )
                {
                case PROPERTY:
                    deserialization.handle( entry, property( relationship, entry.name() ) );
                    break;
                case TYPE:
                    deserialization.handle( entry, relationship.type() );
                    break;
                case START_ID:
                    deserialization.handle( entry, relationship.startNode() );
                    break;
                case END_ID:
                    deserialization.handle( entry, relationship.endNode() );
                    break;
                default: // ignore other types
                }
            }
            return deserialization.materialize();
        } );
    }

    private Object property( InputEntity entity, String key )
    {
        Object[] properties = entity.properties();
        for ( int i = 0; i < properties.length; i += 2 )
        {
            if ( properties[i].equals( key ) )
            {
                return properties[i + 1];
            }
        }
        return null;
    }

    private <ENTITY extends InputEntity> void consume( String name, InputIterable<ENTITY> entities, Header header,
            BiFunction<ENTITY,Deserialization<String>,String> deserializer ) throws IOException
    {
        try ( PrintStream out = file( name + "-header.csv" ) )
        {
            serialize( out, header );
        }

        try ( InputIterator<ENTITY> iterator = entities.iterator() )
        {
            int threads = Runtime.getRuntime().availableProcessors() / 2;
            iterator.hasNext();
            iterator.processors( threads );
            ExecutorService executor = Executors.newFixedThreadPool( threads );
            AtomicBoolean end = new AtomicBoolean();
            BlockingQueue<List<ENTITY>> queue = new ArrayBlockingQueue<>( threads * 10 );
            for ( int i = 0; i < threads; i++ )
            {
                int id = i;
                executor.submit( new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        try ( PrintStream out = file( name + "-" + id + ".csv" ) )
                        {
                            StringDeserialization deserialization = new StringDeserialization( config );
                            while ( true )
                            {
                                List<ENTITY> batch = queue.poll( 100, TimeUnit.MILLISECONDS );
                                if ( batch != null )
                                {
                                    for ( ENTITY entity : batch )
                                    {
                                        out.println( deserializer.apply( entity, deserialization ) );
                                    }
                                }
                                else if ( end.get() )
                                {
                                    break;
                                }
                            }
                        }
                        return null;
                    }
                } );
            }

            while ( iterator.hasNext() )
            {
                List<ENTITY> batch = grabBatch( iterator, 10_000 );
                while ( !queue.offer( batch, 100, TimeUnit.MILLISECONDS ) )
                {
                    // wait
                }
            }
            executor.shutdown();
            end.set( true );
            executor.awaitTermination( 10, TimeUnit.MINUTES );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new IOException( e );
        }
    }

    private static <T> List<T> grabBatch( InputIterator<T> iterator, int count )
    {
        List<T> list = new ArrayList<>( count );
        for ( int i = 0; i < count && iterator.hasNext(); i++ )
        {
            list.add( iterator.next() );
        }
        return list;
    }

    private void serialize( PrintStream out, Header header )
    {
        StringDeserialization deserialization = new StringDeserialization( config );
        deserialization.clear();
        for ( Header.Entry entry : header.entries() )
        {
            deserialization.handle( entry, entry.toString() );
        }
        out.println( deserialization.materialize() );
    }

    private PrintStream file( String name ) throws IOException
    {
        return new PrintStream( new BufferedOutputStream( new FileOutputStream( new File( targetDirectory, name ) ),
                (int) mebiBytes( 4 ) ) );
    }
}
