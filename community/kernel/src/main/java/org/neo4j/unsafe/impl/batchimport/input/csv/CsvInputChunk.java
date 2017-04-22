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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.IOException;
import java.lang.reflect.Array;

import org.neo4j.csv.reader.CharReadableChunker.ChunkImpl;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Extractors.LongExtractor;
import org.neo4j.csv.reader.Mark;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.helpers.Exceptions;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.UnexpectedEndOfInputException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

import static java.lang.String.format;

/**
 * Knows how to interpret raw character data into entities according to a {@link Header}.
 */
public class CsvInputChunk implements InputChunk
{
    private final Mark mark = new Mark();
    private final IdType idType;
    private final int delimiter;
    private final Collector badCollector;
    private final Extractor<String> stringExtractor;
    private final Chunk processingChunk;

    // Set in #initialize
    private CharSeeker seeker;
    private Header header;
    private Entry[] entries;
    private Decorator decorator;

    // Set as #next is called
    private long lineNumber;
    private InputEntityVisitor previousVisitor;
    private InputEntityVisitor visitor;

    public CsvInputChunk( IdType idType, int delimiter, Collector badCollector, Extractors extractors,
            ChunkImpl processingChunk )
    {
        this.idType = idType;
        this.delimiter = delimiter;
        this.badCollector = badCollector;
        this.processingChunk = processingChunk;
        this.stringExtractor = extractors.string();
    }

    /**
     * Called every time this chunk is updated with new data. Potentially this data is from a different
     * stream of data than the previous, therefore the header and decorator is also updated.
     *  @param seeker {@link CharSeeker} able to seek through the data.
     * @param header {@link Header} spec to read data according to.
     * @param decorator additional decoration of the {@link InputEntityVisitor} coming into
 * {@link #next(InputEntityVisitor)}.
     */
    boolean initialize( CharSeeker seeker, Header header, Decorator decorator )
    {
        this.seeker = seeker;
        this.header = header;
        this.entries = header.entries();
        this.decorator = decorator;
        this.visitor = null;
        if ( header.entries().length == 0 )
        {
            return false;
        }
        return true;
    }

    @Override
    public boolean next( InputEntityVisitor nakedVisitor ) throws IOException
    {
        // TODO lazy decorator initialization here, is it a hack?
        if ( visitor == null || nakedVisitor != previousVisitor )
        {
            decorateVisitor( nakedVisitor );
        }

        int i = 0;
        Entry entry = null;
        lineNumber++;
        try
        {
            boolean doContinue = true;
            for ( i = 0; i < entries.length && doContinue; i++ )
            {
                entry = entries[i];
                if ( !seeker.seek( mark, delimiter ) )
                {
                    if ( i > 0 )
                    {
                        throw new UnexpectedEndOfInputException( "Near " + mark );
                    }
                    // We're just at the end
                    return false;
                }

                switch ( entry.type() )
                {
                case ID:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        switch ( idType )
                        {
                        case STRING:
                        case INTEGER:
                            // TODO what about the configured name?
                            Object idValue = entry.extractor().value();
                            doContinue = visitor.id( idValue, entry.group() );
                            if ( entry.name() != null )
                            {
                                doContinue = visitor.property( entry.name(), idValue );
                            }
                            break;
                        case ACTUAL:
                            doContinue = visitor.id( ((LongExtractor) entry.extractor()).longValue() );
                            break;
                        default: throw new IllegalArgumentException( idType.name() );
                        }
                    }
                    break;
                case START_ID:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        switch ( idType )
                        {
                        case STRING:
                            doContinue = visitor.startId( ((StringExtractor) entry.extractor()).flyweightExtract(), entry.group() );
                            break;
                        case INTEGER:
                            doContinue = visitor.startId( entry.extractor().value(), entry.group() );
                            break;
                        case ACTUAL:
                            doContinue = visitor.startId( ((LongExtractor) entry.extractor()).longValue() );
                            break;
                        default: throw new IllegalArgumentException( idType.name() );
                        }
                    }
                    break;
                case END_ID:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        switch ( idType )
                        {
                        case STRING:
                            doContinue = visitor.startId( ((StringExtractor) entry.extractor()).flyweightExtract(), entry.group() );
                            break;
                        case INTEGER:
                            doContinue = visitor.endId( entry.extractor().value(), entry.group() );
                            break;
                        case ACTUAL:
                            doContinue = visitor.endId( ((LongExtractor) entry.extractor()).longValue() );
                            break;
                        default: throw new IllegalArgumentException( idType.name() );
                        }
                    }
                    break;
                 case TYPE:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        doContinue = visitor.type( (String) entry.extractor().value() );
                    }
                    break;
                case PROPERTY:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        // TODO since PropertyStore#encodeValue takes Object there's no point splitting up
                        // into different primitive types
                        Object value = entry.extractor().value();
                        if ( !isEmptyArray( value ) )
                        {
                            doContinue = visitor.property( entry.name(), value );
                        }
                    }
                    break;
                case LABEL:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        Object labelsValue = entry.extractor().value();
                        if ( labelsValue.getClass().isArray() )
                        {
                            doContinue = visitor.labels( (String[]) labelsValue );
                        }
                        else
                        {
                            doContinue = visitor.labels( new String[] {(String) labelsValue} );
                        }
                    }
                    break;
                case IGNORE:
                    break;
                default:
                    throw new IllegalArgumentException( entry.type().toString() );
                }

                if ( mark.isEndOfLine() )
                {
                 // We're at the end of the line, break and return an entity with what we have.
                    break;
                }
            }

            while ( !mark.isEndOfLine() )
            {
                seeker.seek( mark, delimiter );
                if ( doContinue )
                {
                    seeker.tryExtract( mark, stringExtractor );
                    badCollector.collectExtraColumns(
                            seeker.sourceDescription(), lineNumber, stringExtractor.value() );
                }
            }
            visitor.endOfEntity();
        }
        catch ( final RuntimeException e )
        {
            String stringValue = null;
            try
            {
                Extractors extractors = new Extractors( '?' );
                if ( seeker.tryExtract( mark, extractors.string() ) )
                {
                    stringValue = extractors.string().value();
                }
            }
            catch ( Exception e1 )
            {   // OK
            }

            String message = format( "ERROR in input" +
                    "%n  data source: %s" +
                    "%n  in field: %s" +
                    "%n  for header: %s" +
                    "%n  raw field value: %s" +
                    "%n  original error: %s",
                    seeker, entry + ":" + (i + 1), header,
                    stringValue != null ? stringValue : "??",
                    e.getMessage() );

            if ( e instanceof InputException )
            {
                throw Exceptions.withMessage( e, message );
            }
            throw new InputException( message, e );
        }

        return true;
    }

    private static boolean isEmptyArray( Object value )
    {
        return value.getClass().isArray() && Array.getLength( value ) == 0;
    }

    private void decorateVisitor( InputEntityVisitor nakedVisitor )
    {
        visitor = decorator.apply( nakedVisitor );
        previousVisitor = nakedVisitor;
    }

    protected Chunk processingChunk()
    {
        return processingChunk;
    }

    @Override
    public void close() throws IOException
    {
    }
}
