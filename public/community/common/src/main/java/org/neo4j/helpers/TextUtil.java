/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.helpers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * @deprecated This class will be removed from public API in 4.0.
 */
@Deprecated
public class TextUtil
{
    private TextUtil()
    {
    }

    public static String templateString( String templateString, Map<String, ?> data )
    {
        return templateString( templateString, "\\$", data );
    }

    public static String templateString( String templateString,
            String variablePrefix, Map<String, ?> data )
    {
        // Sort data strings on length.
        Map<Integer, List<String>> lengthMap =
            new HashMap<>();
        int longest = 0;
        for ( String key : data.keySet() )
        {
            int length = key.length();
            if ( length > longest )
            {
                longest = length;
            }

            List<String> innerList;
            Integer innerKey = length;
            if ( lengthMap.containsKey( innerKey ) )
            {
                innerList = lengthMap.get( innerKey );
            }
            else
            {
                innerList = new ArrayList<>();
                lengthMap.put( innerKey, innerList );
            }
            innerList.add( key );
        }

        // Replace it.
        String result = templateString;
        for ( int i = longest; i >= 0; i-- )
        {
            Integer lengthKey = i;
            if ( !lengthMap.containsKey( lengthKey ) )
            {
                continue;
            }

            List<String> list = lengthMap.get( lengthKey );
            for ( String key : list )
            {
                Object value = data.get( key );
                if ( value != null )
                {
                    String replacement = data.get( key ).toString();
                    String regExpMatchString = variablePrefix + key;
                    result = result.replaceAll( regExpMatchString, replacement );
                }
            }
        }

        return result;
    }

    private static String[] splitAndKeepEscapedSpaces( String string, boolean preserveEscapes, boolean preserveSpaceEscapes )
    {
        Collection<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for ( int i = 0; i < string.length(); i++ )
        {
            char ch = string.charAt( i );
            if ( ch == ' ' )
            {
                boolean isEscapedSpace = i > 0 && string.charAt( i - 1 ) == '\\';
                if ( !isEscapedSpace )
                {
                    result.add( current.toString() );
                    current = new StringBuilder();
                    continue;
                }
                if ( preserveEscapes && !preserveSpaceEscapes )
                {
                    current.setLength( current.length() - 1 );
                }
            }

            if ( preserveEscapes || ch != '\\' )
            {
                current.append( ch );
            }
        }
        if ( current.length() > 0 )
        {
            result.add( current.toString() );
        }
        return result.toArray( new String[result.size()] );
    }

    /**
     * Tokenizes a string, regarding quotes.
     *
     * @param string the string to tokenize.
     * @return the tokens from the line.
     */
    public static String[] tokenizeStringWithQuotes( String string )
    {
        return tokenizeStringWithQuotes( string, true, false );
    }

    /**
     * Tokenizes a string, regarding quotes. Examples:
     *
     * o '"One two"'              ==&gt; [ "One two" ]
     * o 'One two'                ==&gt; [ "One", "two" ]
     * o 'One "two three" four'   ==&gt; [ "One", "two three", "four" ]
     *
     * @param string the string to tokenize.
     * @param trim  whether or not to trim each token.
     * @param preserveEscapeCharacters whether or not to preserve escape characters '\', otherwise skip them.
     * @return the tokens from the line.
     */
    public static String[] tokenizeStringWithQuotes( String string, boolean trim, boolean preserveEscapeCharacters )
    {
        return tokenizeStringWithQuotes( string, trim, preserveEscapeCharacters, preserveEscapeCharacters );
    }

    /**
     * Tokenizes a string, regarding quotes with a possibility to keep escaping characters but removing them when they used for space escaping.
     */
    public static String[] tokenizeStringWithQuotes( String string, boolean trim, boolean preserveEscapeCharacters, boolean preserveSpaceEscapeCharacters )
    {
        if ( trim )
        {
            string = string.trim();
        }
        ArrayList<String> result = new ArrayList<>();
        string = string.trim();
        boolean inside = string.startsWith( "\"" );
        StringTokenizer quoteTokenizer = new StringTokenizer( string, "\"" );
        while ( quoteTokenizer.hasMoreTokens() )
        {
            String token = quoteTokenizer.nextToken();
            if ( trim )
            {
                token = token.trim();
            }
            if ( token.length() != 0 )
            {
                if ( inside )
                {
                    // Don't split
                    result.add( token );
                }
                else
                {
                    Collections.addAll( result, TextUtil.splitAndKeepEscapedSpaces( token, preserveEscapeCharacters, preserveSpaceEscapeCharacters ) );
                }
            }
            inside = !inside;
        }
        return result.toArray( new String[result.size()] );
    }
}
