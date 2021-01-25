/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import static java.lang.Float.floatToRawIntBits;

public class GraphChecksum
{
    public String checksum( GraphDatabaseService db )
    {
        try
        {
            MessageDigest md5 = MessageDigest.getInstance( "MD5" );
            try ( Transaction tx = db.beginTx() )
            {
                for ( Node node : tx.getAllNodes() )
                {
                    md5.update( toBytes( node.getId() ) );

                    for ( Label label : node.getLabels() )
                    {
                        md5.update( label.name().getBytes() );
                    }

                    digestProperties( md5, node );

                    for ( Relationship relationship : node.getRelationships( Direction.INCOMING ) )
                    {
                        md5.update( toBytes( relationship.getId() ) );
                        md5.update( relationship.getType().name().getBytes() );
                        md5.update( toBytes( relationship.getStartNode().getId() ) );
                        md5.update( toBytes( relationship.getEndNode().getId() ) );

                        digestProperties( md5, relationship );
                    }
                }
                tx.commit();
                return new BigInteger( 1, md5.digest() ).toString( 16 );
            }
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void digestProperties( MessageDigest md5, Entity propContainer )
    {
        for ( String key : propContainer.getPropertyKeys() )
        {
            Object property = propContainer.getProperty( key );
            if ( property instanceof String )
            {
                md5.update( ((String) property).getBytes() );
            }
            else if ( property instanceof Number )
            {
                md5.update( toBytes( ((Number) property).longValue() ) );
            }
            else if ( property.getClass().isArray() )
            {
                Class<?> type = property.getClass().getComponentType();
                if ( type.equals( Integer.TYPE ) )
                {
                    for ( int i : (int[]) property )
                    {
                        md5.update( toBytes( i ) );
                    }
                }
                else if ( type.equals( Long.TYPE ) )
                {
                    for ( long i : (long[]) property )
                    {
                        md5.update( toBytes( i ) );
                    }
                }
                else if ( type.equals( Short.TYPE ) )
                {
                    for ( short i : (short[]) property )
                    {
                        md5.update( toBytes( i ) );
                    }
                }
                else if ( type.equals( Byte.TYPE ) )
                {
                    for ( byte i : (byte[]) property )
                    {
                        md5.update( i );
                    }
                }
                else if ( type.equals( Boolean.TYPE ) )
                {
                    for ( boolean i : (boolean[]) property )
                    {
                        md5.update( i ? (byte) 1 : (byte) 0 );
                    }
                }
                else if ( type.equals( Float.TYPE ) )
                {
                    for ( float i : (float[]) property )
                    {
                        md5.update( toBytes( floatToRawIntBits( i ) ) );
                    }
                }
                else if ( type.equals( Double.TYPE ) )
                {
                    for ( double i : (double[]) property )
                    {
                        md5.update( toBytes( Double.doubleToRawLongBits( i ) ) );
                    }
                }
                else if ( type.equals( Character.TYPE ) )
                {
                    for ( char i : (char[]) property )
                    {
                        md5.update( toBytes( i ) );
                    }
                }
                else if ( type.equals( String.class ) )
                {
                    for ( String i : (String[]) property )
                    {
                        md5.update( i.getBytes() );
                    }
                }
            }
        }
    }

    private byte[] toBytes( long value )
    {
        return ByteBuffer.allocate( 8 ).putLong( value ).flip().array();
    }

    private byte[] toBytes( int value )
    {
        return ByteBuffer.allocate( 4 ).putInt( value ).flip().array();
    }

    private byte[] toBytes( short value )
    {
        return ByteBuffer.allocate( 2 ).putShort( value ).flip().array();
    }

    private byte[] toBytes( char value )
    {
        return ByteBuffer.allocate( 2 ).putChar( value ).flip().array();
    }
}
