/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LenientObjectInputStreamTest
{
    @Test
    public void shouldStoreTheSerialVersionIdOfAClassTheFirstTimeItsDeserialised() throws IOException,
            ClassNotFoundException
    {
        // given
        MemberIsAvailable memberIsAvailable = memberIsAvailable();
        Payload payload = payloadFor( memberIsAvailable );
        VersionMapper versionMapper = new VersionMapper();

        // when
        new LenientObjectInputStream( inputStreamFor( payload ), versionMapper ).readObject();

        // then
        assertTrue( versionMapper.hasMappingFor( memberIsAvailable.getClass().getName() ) );
        assertEquals( serialVersionUIDFor( memberIsAvailable ),
                versionMapper.mappingFor( memberIsAvailable.getClass().getName() ) );
    }

    private long serialVersionUIDFor( MemberIsAvailable memberIsAvailable )
    {
        return ObjectStreamClass.lookup( memberIsAvailable.getClass() ).getSerialVersionUID();
    }

    private ByteArrayInputStream inputStreamFor( Payload payload )
    {
        return new ByteArrayInputStream( payload.getBuf(), 0, payload.getLen() );
    }

    private MemberIsAvailable memberIsAvailable()
    {
        return new MemberIsAvailable( "r1", new InstanceId( 1 ), URI.create( "http://me" ),
                URI.create( "http://me?something" ), StoreId.DEFAULT );
    }

    private Payload payloadFor( Object value ) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream(  );
        ObjectOutputStream oout = new ObjectOutputStream( bout );
        oout.writeObject( value );
        oout.close();
        byte[] bytes = bout.toByteArray();
        return new Payload( bytes, bytes.length );
    }
}
