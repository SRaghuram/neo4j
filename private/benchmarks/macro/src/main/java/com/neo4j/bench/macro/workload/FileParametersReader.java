/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class FileParametersReader implements ParametersReader
{
    enum ColumnType
    {
        String( "String",
                s -> s ),
        Integer( "Integer",
                 s -> java.lang.Integer.parseInt( s.trim() ) ),
        Long( "Long",
              s -> java.lang.Long.parseLong( s.trim() ) ),
        Float( "Float",
               java.lang.Float::parseFloat ),
        Double( "Double",
                s -> java.lang.Double.parseDouble( s.trim() ) ),
        StringArray( "String[]",
                     s -> s.isEmpty()
                          ? Collections.EMPTY_LIST
                          : newArrayList( s.split( "," ) ) ),
        IntegerArray( "Integer[]",
                      s -> s.isEmpty()
                           ? Collections.EMPTY_LIST
                           : Arrays.stream( s.split( "," ) ).map( n -> java.lang.Integer.parseInt( n.trim() ) ).collect( toList() ) ),
        LongArray( "Long[]",
                   s -> s.isEmpty()
                        ? Collections.EMPTY_LIST
                        : Arrays.stream( s.split( "," ) ).map( n -> java.lang.Long.parseLong( n.trim() ) ).collect( toList() ) ),
        FloatArray( "Float[]",
                    s -> s.isEmpty()
                         ? Collections.EMPTY_LIST
                         : Arrays.stream( s.split( "," ) ).map( n -> java.lang.Float.parseFloat( n.trim() ) ).collect( toList() ) ),
        DoubleArray( "Double[]",
                     s -> s.isEmpty()
                          ? Collections.EMPTY_LIST
                          : Arrays.stream( s.split( "," ) ).map( n -> java.lang.Double.parseDouble( n.trim() ) ).collect( toList() ) ),
        Date( "Date", LocalDate::parse );

        private final String id;
        private final Function<String,Object> parseFunction;

        ColumnType( String id, Function<String,Object> parseFunction )
        {
            this.id = id;
            this.parseFunction = parseFunction;
        }
    }

    private static final String DEFAULT_COLUMN_SEPARATOR_REGEX_STRING = "\\|";
    private final Pattern separator;
    private final BufferedReader reader;
    private final ColumnType[] columnTypes;
    private final String[] columns;

    private String[] next;
    private boolean closed;

    FileParametersReader( Path csvFile )
    {
        this( csvFile, DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
    }

    private FileParametersReader( Path csvFile, String separatorRegexString )
    {
        this.reader = readerFor( csvFile );
        this.separator = Pattern.compile( separatorRegexString );
        String[] headers = readHeaders( reader, separator );
        if ( null == headers )
        {
            throw new RuntimeException( "Could not retrieve headers, file may be empty" );
        }
        this.columnTypes = columnTypesFor( headers );
        this.columns = stripColumnTypesFrom( headers );
    }

    private static BufferedReader readerFor( Path csvFile )
    {
        if ( !Files.exists( csvFile ) )
        {
            throw new WorkloadConfigException( "File not found: " + csvFile.toAbsolutePath(), WorkloadConfigError.PARAM_FILE_NOT_FOUND );
        }
        try
        {
            return new BufferedReader( new InputStreamReader( Files.newInputStream( csvFile ), UTF_8 ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error accessing file", e );
        }
    }

    private static String[] stripColumnTypesFrom( String[] headers )
    {
        return Arrays.stream( headers )
                     .map( column -> column.contains( ":" )
                                     ? column.substring( 0, column.indexOf( ':' ) )
                                     : column )
                     .toArray( String[]::new );
    }

    private static ColumnType[] columnTypesFor( String[] headers )
    {
        return Arrays.stream( headers )
                     .map( column -> column.contains( ":" )
                                     ? column.substring( column.indexOf( ':' ) + 1 )
                                     : "String" )
                     .map( FileParametersReader::columnTypesForId )
                     .toArray( ColumnType[]::new );
    }

    private static ColumnType columnTypesForId( String columnTypeId )
    {
        return Arrays.stream( ColumnType.values() )
                     .filter( columnType -> columnType.id.equals( columnTypeId ) )
                     .findFirst()
                     .orElseThrow( () -> new RuntimeException( "Invalid column type: " + columnTypeId ) );
    }

    @Override
    public boolean hasNext()
    {
        if ( closed )
        {
            throw new RuntimeException( "Reader already closed" );
        }
        if ( null == next )
        {
            next = read( reader, separator, columnTypes.length );
        }
        return null != next;
    }

    @Override
    public Map<String,Object> next()
    {
        if ( !hasNext() )
        {
            throw new RuntimeException( "Nothing left to read" );
        }
        String[] tempNext = next;
        next = null;
        return IntStream.range( 0, tempNext.length )
                        .boxed()
                        .collect( toMap(
                                i -> columns[i],
                                i -> columnTypes[i].parseFunction.apply( tempNext[i] ) ) );
    }

    private static String[] readHeaders( BufferedReader reader, Pattern separator )
    {
        try
        {
            String csvLine = reader.readLine();
            return (null == csvLine) ? null : separator.split( csvLine );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error reading from file", e );
        }
    }

    private static String[] read( BufferedReader reader, Pattern separator, int columnCount )
    {
        try
        {
            String csvLine = reader.readLine();
            String[] columns = (null == csvLine) ? null : separator.split( csvLine, -1 );
            if ( csvLine != null && columns.length != columnCount )
            {
                throw new RuntimeException( String.format( "Row has unexpected column count, expected %s got %s", columnCount, columns.length ) );
            }
            return columns;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error reading from file", e );
        }
    }

    @Override
    public void close()
    {
        if ( closed )
        {
            throw new RuntimeException( "Reader is already closed, can not close again" );
        }
        try
        {
            reader.close();
            closed = true;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( format( "Error closing file [%s]", reader ), e );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        FileParametersReader that = (FileParametersReader) o;
        return closed == that.closed &&
               Objects.equals( separator, that.separator ) &&
               Objects.equals( reader, that.reader ) &&
               Arrays.equals( columnTypes, that.columnTypes ) &&
               Arrays.equals( columns, that.columns ) &&
               Arrays.equals( next, that.next );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( separator, reader, Arrays.hashCode( columnTypes ), Arrays.hashCode( columns ),
                Arrays.hashCode( next ), closed );
    }
}
