/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;

import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.server.rest.MasterInfoService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MasterInfoServiceTest
{
    @Test
    void masterShouldRespond200AndTrueWhenMaster() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "master" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isMaster();

        // then
        assertEquals( 200, response.getStatus() );
        assertEquals( "true", String.valueOf( response.getEntity() ) );
    }

    @Test
    void masterShouldRespond404AndFalseWhenSlave() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "slave" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isMaster();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "false", String.valueOf( response.getEntity() ) );
    }

    @Test
    void masterShouldRespond404AndUNKNOWNWhenUnknown() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "unknown" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isMaster();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "UNKNOWN", String.valueOf( response.getEntity() ) );
    }

    @Test
    void slaveShouldRespond200AndTrueWhenSlave() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "slave" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isSlave();

        // then
        assertEquals( 200, response.getStatus() );
        assertEquals( "true", String.valueOf( response.getEntity() ) );
    }

    @Test
    void slaveShouldRespond404AndFalseWhenMaster() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "master" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isSlave();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "false", String.valueOf( response.getEntity() ) );
    }

    @Test
    void slaveShouldRespond404AndUNKNOWNWhenUnknown() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "unknown" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isSlave();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "UNKNOWN", String.valueOf( response.getEntity() ) );
    }

    @Test
    void shouldReportMasterAsGenerallyAvailableForTransactionProcessing() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "master" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isAvailable();

        // then
        assertEquals( 200, response.getStatus() );
        assertEquals( "master", String.valueOf( response.getEntity() ) );
    }

    @Test
    void shouldReportSlaveAsGenerallyAvailableForTransactionProcessing() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "slave" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isAvailable();

        // then
        assertEquals( 200, response.getStatus() );
        assertEquals( "slave", String.valueOf( response.getEntity() ) );
    }

    @Test
    void shouldReportNonMasterOrSlaveAsUnavailableForTransactionProcessing() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "unknown" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isAvailable();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "UNKNOWN", String.valueOf( response.getEntity() ) );
    }
}
