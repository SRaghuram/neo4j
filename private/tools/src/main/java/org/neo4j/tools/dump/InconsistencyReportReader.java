/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.neo4j.consistency.RecordType;
import org.neo4j.tools.dump.inconsistency.Inconsistencies;

/**
 * Reads CC inconsistency reports. Example of entry:
 * <p>
 * <pre>
 * ERROR: The referenced relationship record is not in use.
 *     Node[3496089,used=true,rel=14833798,prop=13305361,labels=Inline(0x1000000006:[6]),light,secondaryUnitId=-1]
 *     Inconsistent with: Relationship[14833798,used=false,source=0,target=0,type=0,sPrev=0,sNext=0,tPrev=0,tNext=0,
 *     prop=0,secondaryUnitId=-1,!sFirst,!tFirst]
 * </pre>
 * <p>
 * Another example entry:
 * <p>
 * <pre>
 * ERROR: The first outgoing relationship is not the first in its chain.
 *     RelationshipGroup[12144403,type=9,out=2988709379,in=-1,loop=-1,prev=-1,next=40467195,used=true,owner=635306933,secondaryUnitId=-1]
 * </pre>
 */
public class InconsistencyReportReader
{
    private static final String INCONSISTENT_WITH = "Inconsistent with: ";
    private final Inconsistencies inconsistencies;

    public InconsistencyReportReader( Inconsistencies inconsistencies )
    {
        this.inconsistencies = inconsistencies;
    }

    public void read( File file ) throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( new FileReader( file ) ) )
        {
            read( reader );
        }
    }

    public void read( BufferedReader bufferedReader ) throws IOException
    {
        String line = bufferedReader.readLine();
        RecordType inconsistentRecordType;
        RecordType inconsistentWithRecordType;
        long inconsistentRecordId;
        long inconsistentWithRecordId;

        while ( line != null )
        {
            if ( line.contains( "ERROR" ) || line.contains( "WARNING" ) )
            {
                // The current line is the inconsistency description line.
                // Get the inconsistent entity line:
                line = bufferedReader.readLine();
                if ( line == null )
                {
                    return; // Unexpected end of report.
                }
                line = line.trim();
                inconsistentRecordType = toRecordType( entityType( line ) );
                inconsistentRecordId = id( line );

                // Then get the Inconsistent With line:
                line = bufferedReader.readLine();
                if ( line == null || !line.contains( INCONSISTENT_WITH ) )
                {
                    // There's no Inconsistent With line, so we report what we have.
                    inconsistencies.reportInconsistency( inconsistentRecordType, inconsistentRecordId );
                    // Leave the current line for the next iteration of the loop.
                }
                else
                {
                    line = line.substring( INCONSISTENT_WITH.length() ).trim();
                    inconsistentWithRecordType = toRecordType( entityType( line ) );
                    inconsistentWithRecordId = id( line );
                    inconsistencies.reportInconsistency(
                            inconsistentRecordType, inconsistentRecordId,
                            inconsistentWithRecordType, inconsistentWithRecordId );
                    line = bufferedReader.readLine(); // Prepare a line for the next iteration of the loop.
                }
            }
            else
            {
                // The current line doesn't fit with anything we were expecting to see, so we skip it and try the
                // next line.
                line = bufferedReader.readLine();
            }
        }
    }

    private RecordType toRecordType( String entityType )
    {
        if ( entityType == null )
        {
            // Skip unrecognizable lines.
            return null;
        }

        switch ( entityType )
        {
        case "Relationship":
            return RecordType.RELATIONSHIP;
        case "Node":
            return RecordType.NODE;
        case "Property":
            return RecordType.PROPERTY;
        case "RelationshipGroup":
            return RecordType.RELATIONSHIP_GROUP;
        case "IndexRule":
            return RecordType.SCHEMA;
        case "IndexEntry":
            return RecordType.NODE;
        default:
            // it's OK, we just haven't implemented support for this yet
            return null;
        }
    }

    private long id( String line )
    {
        int bracket = line.indexOf( '[' );
        if ( bracket > -1 )
        {
            int separator = min( getSeparatorIndex( ',', line, bracket ),
                    getSeparatorIndex( ';', line, bracket ),
                    getSeparatorIndex( ']', line, bracket ) );
            int equally = line.indexOf( '=', bracket );
            int startPosition = (isNotPlainId( bracket, separator, equally ) ? equally : bracket) + 1;
            if ( separator > -1 )
            {
                return Long.parseLong( line.substring( startPosition, separator ) );
            }
        }
        return -1;
    }

    private static int min( int... values )
    {
        int min = Integer.MAX_VALUE;
        for ( int value : values )
        {
            min = Math.min( min, value );
        }
        return min;
    }

    private int getSeparatorIndex( char character, String line, int bracket )
    {
        int index = line.indexOf( character, bracket );
        return index >= 0 ? index : Integer.MAX_VALUE;
    }

    private boolean isNotPlainId( int bracket, int comma, int equally )
    {
        return (equally > bracket) && (equally < comma);
    }

    private String entityType( String line )
    {
        int bracket = line.indexOf( '[' );
        return bracket == -1 ? null : line.substring( 0, bracket );
    }
}
