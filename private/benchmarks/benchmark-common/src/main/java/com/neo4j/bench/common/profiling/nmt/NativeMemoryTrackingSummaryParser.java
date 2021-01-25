/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.nmt;

import java.io.IOError;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class NativeMemoryTrackingSummaryParser
{
    private static final Pattern CATEGORY_PATTERN =
            Pattern.compile( "([A-Z].*)\\s\\(reserved=(\\d*)KB,\\scommitted=(\\d*)KB\\)" );

    public static NativeMemoryTrackingSummary parse( Path path )
    {
        try ( Stream<String> stream = Files.lines( path ) )
        {
            Map<String,NativeMemoryTrackingCategory> map = stream.flatMap( s -> {
                Matcher matcher = CATEGORY_PATTERN.matcher( s );
                if ( matcher.find() )
                {
                    return Stream.of( new NativeMemoryTrackingCategory(
                            matcher.group( 1 ),
                            Long.parseLong( matcher.group( 2 ) ),
                            Long.parseLong( matcher.group( 3 ) ) ) );
                }
                else
                {
                    return Stream.empty();
                }
            } )
            .collect( toMap( NativeMemoryTrackingCategory::getCategory, Function.identity() ) );
            return new NativeMemoryTrackingSummary( map );
        }
        catch ( IOException e )
        {
            throw new IOError( e );
        }
    }
}
