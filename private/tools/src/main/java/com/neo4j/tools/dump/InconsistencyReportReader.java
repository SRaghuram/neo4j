/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
    private final InconsistentRecords inconsistencies;

    public InconsistencyReportReader( InconsistentRecords inconsistencies )
    {
        this.inconsistencies = inconsistencies;
    }

    public void read( Path file ) throws IOException
    {
        try ( BufferedReader reader = Files.newBufferedReader( file ) )
        {
            read( reader );
        }
    }

    public void read( BufferedReader bufferedReader ) throws IOException
    {
        String line = bufferedReader.readLine();
        InconsistentRecords.Type inconsistentRecordType;
        InconsistentRecords.Type inconsistentWithRecordType;
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
                if ( inconsistentRecordType == null )
                {
                    continue;
                }

                inconsistentRecordId = inconsistentRecordType.extractId( line );
                inconsistencies.reportInconsistency( inconsistentRecordType, inconsistentRecordId );

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
                    if ( inconsistentWithRecordType != null )
                    {
                        inconsistentWithRecordId = inconsistentWithRecordType.extractId( line );
                        inconsistencies.reportInconsistency( inconsistentWithRecordType, inconsistentWithRecordId );
                    }
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

    private InconsistentRecords.Type toRecordType( String entityType )
    {
        if ( entityType == null )
        {
            // Skip unrecognizable lines.
            return null;
        }

        switch ( entityType )
        {
        case "Relationship":
            return InconsistentRecords.Type.RELATIONSHIP;
        case "Node":
            return InconsistentRecords.Type.NODE;
        case "Property":
            return InconsistentRecords.Type.PROPERTY;
        case "RelationshipGroup":
            return InconsistentRecords.Type.RELATIONSHIP_GROUP;
        case "Index":
        case "IndexRule": // "IndexRule" may show up in older reports.
            return InconsistentRecords.Type.SCHEMA_INDEX;
        case "IndexEntry":
            return InconsistentRecords.Type.NODE;
        case "NodeLabelRange":
        case "RelationshipTypeRange":
            return InconsistentRecords.Type.ENTITY_TOKEN_RANGE;
        default:
            // it's OK, we just haven't implemented support for this yet
            return null;
        }
    }

    private String entityType( String line )
    {
        int bracket = indexOfBracket( line );
        return bracket == -1 ? null : line.substring( 0, bracket );
    }

    static int indexOfBracket( String line )
    {
        int bracket = -1;
        int len = line.length();
        for ( int i = 0; i < len; i++ )
        {
            char ch = line.charAt( i );
            if ( ch == '[' || ch == '(' )
            {
                bracket = i;
                break;
            }
        }
        return bracket;
    }
}
