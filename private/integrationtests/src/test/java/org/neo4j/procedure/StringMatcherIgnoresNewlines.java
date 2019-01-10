/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure;

import org.hamcrest.Matcher;
import org.hamcrest.core.StringContains;

import java.util.regex.Pattern;

public class StringMatcherIgnoresNewlines
{
    private StringMatcherIgnoresNewlines()
    {
    }

    public static Matcher<String> containsStringIgnoreNewlines( String substring )
    {
        return new StringContains( false, substring )
        {
            Pattern newLines = Pattern.compile( "\\s*[\\r\\n]+\\s*" );

            private String clean( String string )
            {
                return newLines.matcher( string ).replaceAll( "" );
            }

            @Override
            protected boolean evalSubstringOf( String s )
            {
                return clean( s ).contains( clean( substring ) );
            }
        };
    }
}
