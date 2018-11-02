/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public class ExceptionMatchers
{
    public static TypeSafeMatcher<Throwable> exceptionContainsSuppressedThrowable( Throwable expectedSuppressed )
    {
        return new TypeSafeMatcher<Throwable>()
        {
            @Override
            protected boolean matchesSafely( Throwable item )
            {
                List<Throwable> suppress = Arrays.asList( item.getSuppressed() );
                return suppress.contains( expectedSuppressed );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "contains suppressed exception " ).appendValue( expectedSuppressed );
            }

            @Override
            protected void describeMismatchSafely( Throwable item, Description mismatchDescription )
            {
                List<String> suppressedExceptionStrings = Stream
                        .of( item.getSuppressed() )
                        .map( ExceptionMatchers::exceptionWithMessageToString )
                        .collect( Collectors.toList() );
                mismatchDescription
                        .appendText( "exception " )
                        .appendValue( item )
                        .appendText( " with suppressed " )
                        .appendValueList( "[", ", ", "]", suppressedExceptionStrings )
                        .appendText( " does not contain " )
                        .appendValue( exceptionWithMessageToString( expectedSuppressed ) );
            }
        };
    }

    private static String exceptionWithMessageToString( Throwable throwable )
    {
        return format( "<%s:%s>", throwable.getClass(), throwable.getMessage() );
    }
}
