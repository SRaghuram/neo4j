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
package org.neo4j.server.rest.web;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status.Request;
import org.neo4j.kernel.api.exceptions.Status.Schema;
import org.neo4j.kernel.api.exceptions.Status.Statement;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappedDatabase;
import org.neo4j.server.plugins.ConfigAdapter;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.RelationshipRepresentationTest;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.rest.web.DatabaseActions.RelationshipDirection;
import org.neo4j.server.rest.web.RestfulGraphDatabase.AmpersandSeparatedCollection;
import org.neo4j.string.UTF8;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.server.EntityOutputFormat;

import static java.lang.Long.parseLong;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.exceptions.Status.Request.InvalidFormat;

public class RestfulGraphDatabaseTest
{
    private static final String BASE_URI = "http://neo4j.org/";
    private static final String NODE_SUBPATH = "node/";
    private static RestfulGraphDatabase service;
    private static Database database;
    private static GraphDbHelper helper;
    private static EntityOutputFormat output;
    private static GraphDatabaseFacade graph;

    @BeforeClass
    public static void doBefore()
    {
        graph = (GraphDatabaseFacade) new TestGraphDatabaseFactory().newImpermanentDatabase();
        database = new WrappedDatabase( graph );
        helper = new GraphDbHelper( database );
        output = new EntityOutputFormat( new JsonFormat(), URI.create( BASE_URI ), null );
        DatabaseActions databaseActions = new DatabaseActions( database.getGraph() );
        service = new TransactionWrappingRestfulGraphDatabase(
                graph,
                new RestfulGraphDatabase(
                        new JsonFormat(),
                        output,
                        databaseActions,
                        new ConfigAdapter( Config.defaults() )
                )
        );
    }

    @AfterClass
    public static void shutdownDatabase()
    {
        graph.shutdown();
    }

    private static String entityAsString( Response response )
    {
        byte[] bytes = (byte[]) response.getEntity();
        return UTF8.decode( bytes );
    }

    @SuppressWarnings( "unchecked" )
    private static List<String> entityAsList( Response response )
            throws JsonParseException
    {
        String entity = entityAsString( response );
        return (List<String>) JsonHelper.readJson( entity );
    }

    @Test
    public void shouldFailGracefullyWhenViolatingConstraintOnPropertyUpdate() throws Exception
    {
        Response response = service.createPropertyUniquenessConstraint( "Person", "{\"property_keys\":[\"name\"]}" );
        assertEquals( 200, response.getStatus() );

        createPerson( "Fred" );
        String wilma = createPerson( "Wilma" );

        Response setAllNodePropertiesResponse = service.setAllNodeProperties( parseLong( wilma ), "{\"name\":\"Fred\"}" );
        assertEquals( 409, setAllNodePropertiesResponse.getStatus() );
        assertEquals( Schema.ConstraintValidationFailed.code().serialize(), singleErrorCode( setAllNodePropertiesResponse ) );

        Response singleNodePropertyResponse = service.setNodeProperty( parseLong( wilma ), "name", "\"Fred\"" );
        assertEquals( 409, singleNodePropertyResponse.getStatus() );
        assertEquals( Schema.ConstraintValidationFailed.code().serialize(), singleErrorCode( singleNodePropertyResponse ) );
    }

    private String createPerson( final String name ) throws JsonParseException
    {
        Response response = service.createNode( "{\"name\" : \"" + name + "\"}" );
        assertEquals( 201, response.getStatus() );
        String self = (String) JsonHelper.jsonToMap( entityAsString( response ) ).get( "self" );
        String nodeId = self.substring( self.indexOf( NODE_SUBPATH ) + NODE_SUBPATH.length() );
        response = service.addNodeLabel( parseLong( nodeId ), "\"Person\"" );
        assertEquals( 204, response.getStatus() );
        return nodeId;
    }

    @Test
    public void shouldRespondWith201LocationHeaderAndNodeRepresentationInJSONWhenEmptyNodeCreated() throws Exception
    {
        Response response = service.createNode( null );

        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );

        checkContentTypeCharsetUtf8( response );
        String json = entityAsString( response );

        Map<String, Object> map = JsonHelper.jsonToMap( json );

        assertNotNull( map );

        assertTrue( map.containsKey( "self" ) );
    }

    @Test
    public void shouldRespondWith201LocationHeaderAndNodeRepresentationInJSONWhenPopulatedNodeCreated()
            throws Exception
    {
        Response response = service.createNode( "{\"foo\" : \"bar\"}" );

        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );

        checkContentTypeCharsetUtf8(response);
        String json = entityAsString( response );

        Map<String, Object> map = JsonHelper.jsonToMap( json );

        assertNotNull( map );

        assertTrue( map.containsKey( "self" ) );

        @SuppressWarnings( "unchecked" )
        Map<String, Object> data = (Map<String, Object>) map.get( "data" );

        assertEquals( "bar", data.get( "foo" ) );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldRespondWith201LocationHeaderAndNodeRepresentationInJSONWhenPopulatedNodeCreatedWithArrays()
            throws Exception
    {
        Response response = service.createNode( "{\"foo\" : [\"bar\", \"baz\"] }" );

        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );
        String json = entityAsString( response );

        Map<String, Object> map = JsonHelper.jsonToMap( json );

        assertNotNull( map );

        Map<String, Object> data = (Map<String, Object>) map.get( "data" );

        List<String> foo = (List<String>) data.get( "foo" );
        assertNotNull( foo );
        assertEquals( 2, foo.size() );
    }

    @Test
    public void shouldRespondWith400WhenNodeCreatedWithUnsupportedPropertyData() throws Exception
    {
        Response response = service.createNode( "{\"foo\" : {\"bar\" : \"baz\"}}" );

        assertEquals( 400, response.getStatus() );
        assertEquals( Statement.ArgumentError.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenNodeCreatedWithInvalidJSON() throws Exception
    {
        Response response = service.createNode( "this:::isNot::JSON}" );

        assertEquals( 400, response.getStatus() );
        assertEquals( InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200AndNodeRepresentationInJSONWhenNodeRequested() throws Exception
    {
        Response response = service.getNode( helper.createNode() );
        assertEquals( 200, response.getStatus() );
        String json = entityAsString( response );
        Map<String, Object> map = JsonHelper.jsonToMap( json );
        assertNotNull( map );
        assertTrue( map.containsKey( "self" ) );
    }

    @Test
    public void shouldRespondWith404WhenRequestedNodeDoesNotExist() throws Exception
    {
        Response response = service.getNode( 9000000000000L );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204AfterSettingPropertiesOnExistingNode()
    {
        Response response = service.setAllNodeProperties( helper.createNode(),
                "{\"foo\" : \"bar\", \"a-boolean\": true, \"boolean-array\": [true, false, false]}" );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith404WhenSettingPropertiesOnNodeThatDoesNotExist() throws Exception
    {
        Response response = service.setAllNodeProperties( 9000000000000L, "{\"foo\" : \"bar\"}" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenTransferringCorruptJsonPayload() throws Exception
    {
        Response response = service.setAllNodeProperties( helper.createNode(),
                "{\"foo\" : bad-json-here \"bar\"}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenTransferringIncompatibleJsonPayload() throws Exception
    {
        Response response = service.setAllNodeProperties( helper.createNode(),
                "{\"foo\" : {\"bar\" : \"baz\"}}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Statement.ArgumentError.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200ForGetNodeProperties()
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        helper.setNodeProperties( nodeId, properties );
        Response response = service.getAllNodeProperties( nodeId );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldGetPropertiesForGetNodeProperties() throws Exception
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        properties.put( "double", 15.7 );
        helper.setNodeProperties( nodeId, properties );
        Response response = service.getAllNodeProperties( nodeId );
        String jsonBody = entityAsString( response );
        Map<String, Object> readProperties = JsonHelper.jsonToMap( jsonBody );
        assertEquals( properties, readProperties );
    }

    @Test
    public void shouldRespondWith204OnSuccessfulDelete()
    {
        long id = helper.createNode();

        Response response = service.deleteNode( id );

        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith409IfNodeCannotBeDeleted() throws Exception
    {
        long id = helper.createNode();
        helper.createRelationship( "LOVES", id, helper.createNode() );

        Response response = service.deleteNode( id );

        assertEquals( 409, response.getStatus() );
        assertEquals( Schema.ConstraintValidationFailed.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404IfNodeToBeDeletedDoesNotExist() throws Exception
    {
        long nonExistentId = 999999;
        Response response = service.deleteNode( nonExistentId );

        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204ForSetNodeProperty()
    {
        long nodeId = helper.createNode();
        String key = "foo";
        String json = "\"bar\"";
        Response response = service.setNodeProperty( nodeId, key, json );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldSetRightValueForSetNodeProperty()
    {
        long nodeId = helper.createNode();
        String key = "foo";
        Object value = "bar";
        String json = "\"" + value + "\"";
        service.setNodeProperty( nodeId, key, json );
        Map<String, Object> readProperties = helper.getNodeProperties( nodeId );
        assertEquals( Collections.singletonMap( key, value ), readProperties );
    }

    @Test
    public void shouldRespondWith404ForSetNodePropertyOnNonExistingNode() throws Exception
    {
        String key = "foo";
        String json = "\"bar\"";
        Response response = service.setNodeProperty( 999999, key, json );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400ForSetNodePropertyWithInvalidJson() throws Exception
    {
        String key = "foo";
        String json = "{invalid json";
        Response response = service.setNodeProperty( 999999, key, json );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404ForGetNonExistentNodeProperty() throws Exception
    {
        long nodeId = helper.createNode();
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.PropertyNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404ForGetNodePropertyOnNonExistentNode() throws Exception
    {
        long nodeId = 999999;
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200ForGetNodeProperty()
    {
        long nodeId = helper.createNode();
        String key = "foo";
        Object value = "bar";
        helper.setNodeProperties( nodeId, Collections.singletonMap( key, value ) );
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldReturnCorrectValueForGetNodeProperty()
    {
        long nodeId = helper.createNode();
        String key = "foo";
        Object value = "bar";
        helper.setNodeProperties( nodeId, Collections.singletonMap( key, value ) );
        Response response = service.getNodeProperty( nodeId, "foo" );
        assertEquals( JsonHelper.createJsonFrom( value ), entityAsString( response ) );
    }

    @Test
    public void shouldRespondWith201AndLocationWhenRelationshipIsCreatedWithoutProperties()

    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode, "{\"to\" : \"" + BASE_URI + endNode
                                                                   + "\", \"type\" : \"LOVES\"}" );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );
    }

    @Test
    public void shouldRespondWith201AndLocationWhenRelationshipIsCreatedWithProperties()

    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode + "\", \"type\" : \"LOVES\", " +
                        "\"properties\" : {\"foo\" : \"bar\"}}" );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getMetadata()
                .get( "Location" )
                .get( 0 ) );
    }

    @Test
    public void shouldReturnRelationshipRepresentationWhenCreatingRelationship() throws Exception
    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}" );
        Map<String, Object> map = JsonHelper.jsonToMap( entityAsString( response ) );
        assertNotNull( map );
        assertTrue( map.containsKey( "self" ) );

        checkContentTypeCharsetUtf8( response );

        @SuppressWarnings( "unchecked" )
        Map<String, Object> data = (Map<String, Object>) map.get( "data" );

        assertEquals( "bar", data.get( "foo" ) );
    }

    @Test
    public void shouldRespondWith404WhenTryingToCreateRelationshipFromNonExistentNode() throws Exception
    {
        long nodeId = helper.createNode();
        Response response = service.createRelationship( nodeId + 1000, "{\"to\" : \"" + BASE_URI + nodeId
                + "\", \"type\" : \"LOVES\"}" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipToNonExistentNode() throws Exception
    {
        long nodeId = helper.createNode();
        Response response = service.createRelationship( nodeId, "{\"to\" : \"" + BASE_URI + (nodeId + 1000)
                + "\", \"type\" : \"LOVES\"}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith201WhenTryingToCreateRelationshipBackToSelf()
    {
        long nodeId = helper.createNode();
        Response response = service.createRelationship( nodeId, "{\"to\" : \"" + BASE_URI + nodeId
                + "\", \"type\" : \"LOVES\"}" );
        assertEquals( 201, response.getStatus() );
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipWithBadJson() throws Exception
    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode, "{\"to\" : \"" + BASE_URI + endNode
                + "\", \"type\" ***and junk*** : \"LOVES\"}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith400WhenTryingToCreateRelationshipWithUnsupportedProperties() throws Exception

    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        Response response = service.createRelationship( startNode,
                "{\"to\" : \"" + BASE_URI + endNode
                        + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : {\"bar\" : \"baz\"}}}" );
        assertEquals( 400, response.getStatus() );
        assertEquals( Statement.ArgumentError.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204ForRemoveNodeProperties()
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        helper.setNodeProperties( nodeId, properties );
        Response response = service.deleteAllNodeProperties( nodeId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldBeAbleToRemoveNodeProperties()
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        helper.setNodeProperties( nodeId, properties );
        service.deleteAllNodeProperties( nodeId );
        assertTrue( helper.getNodeProperties( nodeId ).isEmpty() );
    }

    @Test
    public void shouldRespondWith404ForRemoveNodePropertiesForNonExistingNode() throws Exception
    {
        long nodeId = 999999;
        Response response = service.deleteAllNodeProperties( nodeId );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldBeAbleToRemoveNodeProperty()
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        helper.setNodeProperties( nodeId, properties );
        service.deleteNodeProperty( nodeId, "foo" );
        assertEquals( Collections.singletonMap( "number", (Object) 15 ),
                helper.getNodeProperties( nodeId ) );
    }

    @Test
    public void shouldGet404WhenRemovingNonExistingProperty() throws Exception
    {
        long nodeId = helper.createNode();
        Map<String, Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        properties.put( "number", 15 );
        helper.setNodeProperties( nodeId, properties );
        Response response = service.deleteNodeProperty( nodeId, "baz" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.PropertyNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldGet404WhenRemovingPropertyFromNonExistingNode() throws Exception
    {
        long nodeId = 999999;
        Response response = service.deleteNodeProperty( nodeId, "foo" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldGet200WhenRetrievingARelationshipFromANode()
    {
        long relationshipId = helper.createRelationship( "BEATS" );
        Response response = service.getRelationship( relationshipId );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldGet404WhenRetrievingRelationshipThatDoesNotExist() throws Exception
    {
        Response response = service.getRelationship( 999999 );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200AndDataForGetRelationshipProperties() throws Exception
    {
        long relationshipId = helper.createRelationship( "knows" );
        Map<String, Object> properties = new HashMap<>();
        properties.put( "foo", "bar" );
        helper.setRelationshipProperties( relationshipId, properties );
        Response response = service.getAllRelationshipProperties( relationshipId );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8( response );

        Map<String, Object> readProperties = JsonHelper.jsonToMap( entityAsString( response ) );
        assertEquals( properties, readProperties );
    }

    @Test
    public void shouldGet200WhenSuccessfullyRetrievedPropertyOnRelationship()
            throws Exception
    {

        long relationshipId = helper.createRelationship( "knows" );
        Map<String, Object> properties = new HashMap<>();
        properties.put( "some-key", "some-value" );
        helper.setRelationshipProperties( relationshipId, properties );

        Response response = service.getRelationshipProperty( relationshipId, "some-key" );

        assertEquals( 200, response.getStatus() );
        assertEquals( "some-value", JsonHelper.readJson( entityAsString( response ) ) );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldGet404WhenCannotResolveAPropertyOnRelationship() throws Exception
    {
        long relationshipId = helper.createRelationship( "knows" );
        Response response = service.getRelationshipProperty( relationshipId, "some-key" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.PropertyNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldGet204WhenRemovingARelationship()
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteRelationship( relationshipId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldGet404WhenRemovingNonExistentRelationship() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteRelationship( relationshipId + 1000 );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingRelationshipsForANode()
            throws Exception
    {
        long nodeId = helper.createNode();
        helper.createRelationship( "LIKES", nodeId, helper.createNode() );
        helper.createRelationship( "LIKES", helper.createNode(), nodeId );
        helper.createRelationship( "HATES", nodeId, helper.createNode() );

        Response response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals( 200, response.getStatus() );

        checkContentTypeCharsetUtf8(response);

        verifyRelReps( 3, entityAsString( response ) );

        response = service.getNodeRelationships( nodeId, RelationshipDirection.in,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 1, entityAsString( response ) );

        response = service.getNodeRelationships( nodeId, RelationshipDirection.out, new AmpersandSeparatedCollection(
                "" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 2, entityAsString( response ) );

        response = service.getNodeRelationships( nodeId, RelationshipDirection.out, new AmpersandSeparatedCollection(
                "LIKES&HATES" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 2, entityAsString( response ) );

        response = service.getNodeRelationships( nodeId, RelationshipDirection.all, new AmpersandSeparatedCollection(
                "LIKES" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 2, entityAsString( response ) );
    }

    @Test
    public void shouldNotReturnDuplicatesIfSameTypeSpecifiedMoreThanOnce() throws Exception
    {
        long nodeId = helper.createNode();
        helper.createRelationship( "LIKES", nodeId, helper.createNode() );
        Response response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "LIKES&LIKES" ) );
        Collection<?> array = (Collection<?>) JsonHelper.readJson( entityAsString( response ) );
        assertEquals( 1, array.size() );
    }

    private void verifyRelReps( int expectedSize, String entity ) throws JsonParseException
    {
        List<Map<String, Object>> relreps = JsonHelper.jsonToList( entity );
        assertEquals( expectedSize, relreps.size() );
        for ( Map<String, Object> relrep : relreps )
        {
            RelationshipRepresentationTest.verifySerialisation( relrep );
        }
    }

    @Test
    public void
    shouldRespondWith200AndEmptyListOfRelationshipRepresentationsWhenGettingRelationshipsForANodeWithoutRelationships()
            throws Exception
    {
        long nodeId = helper.createNode();

        Response response = service.getNodeRelationships( nodeId, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals( 200, response.getStatus() );
        verifyRelReps( 0, entityAsString( response ) );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldRespondWith404WhenGettingIncomingRelationshipsForNonExistingNode() throws Exception

    {
        Response response = service.getNodeRelationships( 999999, RelationshipDirection.all,
                new AmpersandSeparatedCollection( "" ) );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204AndSetCorrectDataWhenSettingRelationshipProperties()

    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        String json = "{\"name\": \"Mattias\", \"age\": 30}";
        Response response = service.setAllRelationshipProperties( relationshipId, json );
        assertEquals( 204, response.getStatus() );
        Map<String, Object> setProperties = new HashMap<>();
        setProperties.put( "name", "Mattias" );
        setProperties.put( "age", 30 );
        assertEquals( setProperties, helper.getRelationshipProperties( relationshipId ) );
    }

    @Test
    public void shouldRespondWith400WhenSettingRelationshipPropertiesWithBadJson() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        String json = "{\"name: \"Mattias\", \"age\": 30}";
        Response response = service.setAllRelationshipProperties( relationshipId, json );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404WhenSettingRelationshipPropertiesOnNonExistingRelationship() throws Exception

    {
        long relationshipId = 99999999;
        String json = "{\"name\": \"Mattias\", \"age\": 30}";
        Response response = service.setAllRelationshipProperties( relationshipId, json );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204AndSetCorrectDataWhenSettingRelationshipProperty()
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        String key = "name";
        Object value = "Mattias";
        String json = "\"" + value + "\"";
        Response response = service.setRelationshipProperty( relationshipId, key, json );
        assertEquals( 204, response.getStatus() );
        assertEquals( value, helper.getRelationshipProperties( relationshipId )
                .get( "name" ) );
    }

    @Test
    public void shouldRespondWith400WhenSettingRelationshipPropertyWithBadJson() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        String json = "}Mattias";
        Response response = service.setRelationshipProperty( relationshipId, "name", json );
        assertEquals( 400, response.getStatus() );
        assertEquals( Request.InvalidFormat.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404WhenSettingRelationshipPropertyOnNonExistingRelationship() throws Exception

    {
        long relationshipId = 99999999;
        String json = "\"Mattias\"";
        Response response = service.setRelationshipProperty( relationshipId, "name", json );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204WhenSuccessfullyRemovedRelationshipProperties()
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        helper.setRelationshipProperties( relationshipId, Collections.singletonMap( "foo", "bar" ) );

        Response response = service.deleteAllRelationshipProperties( relationshipId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith204WhenSuccessfullyRemovedRelationshipPropertiesWhichAreEmpty()

    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteAllRelationshipProperties( relationshipId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith404WhenNoRelationshipFromWhichToRemoveProperties() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteAllRelationshipProperties( relationshipId + 1000 );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith204WhenRemovingRelationshipProperty()
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        helper.setRelationshipProperties( relationshipId, Collections.singletonMap( "foo", "bar" ) );

        Response response = service.deleteRelationshipProperty( relationshipId, "foo" );

        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith404WhenRemovingRelationshipPropertyWhichDoesNotExist() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        Response response = service.deleteRelationshipProperty( relationshipId, "foo" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.PropertyNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldRespondWith404WhenNoRelationshipFromWhichToRemoveProperty() throws Exception
    {
        long relationshipId = helper.createRelationship( "KNOWS" );

        Response response = service.deleteRelationshipProperty( relationshipId * 1000, "some-key" );
        assertEquals( 404, response.getStatus() );
        assertEquals( Statement.EntityNotFound.code().serialize(), singleErrorCode( response ) );
    }

    @Test
    public void shouldBeAbleToGetRoot() throws JsonParseException
    {
        Response response = service.getRoot();
        assertEquals( 200, response.getStatus() );
        String entity = entityAsString( response );
        Map<String, Object> map = JsonHelper.jsonToMap( entity );
        assertNotNull( map.get( "node" ) );
        //this can be null
//        assertNotNull( map.get( "reference_node" ) );
        assertNotNull( map.get( "neo4j_version" ) );
        assertNotNull( map.get( "node_index" ) );
        assertNotNull( map.get( "extensions_info" ) );
        assertNotNull( map.get( "relationship_index" ) );
        assertNotNull( map.get( "batch" ) );

        checkContentTypeCharsetUtf8( response );
    }

    @Test
    public void shouldBeAbleToGetRootWhenNoReferenceNodePresent() throws Exception
    {
        Response response = service.getRoot();
        assertEquals( 200, response.getStatus() );
        String entity = entityAsString( response );
        Map<String, Object> map = JsonHelper.jsonToMap( entity );
        assertNotNull( map.get( "node" ) );

        assertNotNull( map.get( "node_index" ) );
        assertNotNull( map.get( "extensions_info" ) );
        assertNotNull( map.get( "relationship_index" ) );

        assertNull( map.get( "reference_node" ) );

        checkContentTypeCharsetUtf8(response);
    }

    private void checkContentTypeCharsetUtf8( Response response )
    {
        assertTrue( response.getMetadata()
                .getFirst( HttpHeaders.CONTENT_TYPE ).toString().contains( "UTF-8" ));
    }

    private static String markWithUnicodeMarker( String string )
    {
        return String.valueOf( (char) 0xfeff ) + string;
    }

    @Test
    public void shouldBeAbleToFindSinglePathBetweenTwoNodes()
    {
        long n1 = helper.createNode();
        long n2 = helper.createNode();
        helper.createRelationship( "knows", n1, n2 );
        Map<String, Object> config = MapUtil.map( "max depth", 3, "algorithm", "shortestPath", "to",
                Long.toString( n2 ), "relationships", MapUtil.map( "type", "knows", "direction", "out" ) );
        String payload = JsonHelper.createJsonFrom( config );

        Response response = service.singlePath( n1, payload );

        assertThat( response.getStatus(), is( 200 ) );
        try ( Transaction ignored = graph.beginTx() )
        {
            Map<String, Object> resultAsMap = output.getResultAsMap();
            assertThat( resultAsMap.get( "length" ), is( 1 ) );
        }
    }

    @Test
    public void shouldBeAbleToFindSinglePathBetweenTwoNodesEvenWhenAskingForAllPaths()
    {
        long n1 = helper.createNode();
        long n2 = helper.createNode();
        helper.createRelationship( "knows", n1, n2 );
        Map<String, Object> config = MapUtil.map( "max depth", 3, "algorithm", "shortestPath", "to",
                Long.toString( n2 ), "relationships", MapUtil.map( "type", "knows", "direction", "out" ) );
        String payload = JsonHelper.createJsonFrom( config );

        Response response = service.allPaths( n1, payload );

        assertThat( response.getStatus(), is( 200 ) );
        try ( Transaction ignored = graph.beginTx() )
        {
            List<Object> resultAsList = output.getResultAsList();
            assertThat( resultAsList.size(), is( 1 ) );
        }
    }

    @Test
    public void shouldBeAbleToParseJsonEvenWithUnicodeMarkerAtTheStart()
    {
        Response response = service.createNode( markWithUnicodeMarker( "{\"name\":\"Mattias\"}" ) );
        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        String nodeLocation = response.getMetadata()
                .getFirst( HttpHeaders.LOCATION )
                .toString();

        long node = helper.createNode();
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setNodeProperty( node, "foo", markWithUnicodeMarker( "\"bar\"" ) )
                        .getStatus() );
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setNodeProperty( node, "foo", markWithUnicodeMarker( "" + 10 ) )
                        .getStatus() );
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setAllNodeProperties( node, markWithUnicodeMarker( "{\"name\":\"Something\"," +
                        "\"number\":10}" ) )
                        .getStatus() );

        assertEquals(
                Status.CREATED.getStatusCode(),
                service.createRelationship( node,
                        markWithUnicodeMarker( "{\"to\":\"" + nodeLocation + "\",\"type\":\"knows\"}" ) )
                        .getStatus() );

        long relationship = helper.createRelationship( "knows" );
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setRelationshipProperty( relationship, "foo", markWithUnicodeMarker( "\"bar\"" ) )
                        .getStatus() );
        assertEquals( Status.NO_CONTENT.getStatusCode(),
                service.setRelationshipProperty( relationship, "foo", markWithUnicodeMarker( "" + 10 ) )
                        .getStatus() );
        assertEquals(
                Status.NO_CONTENT.getStatusCode(),
                service.setAllRelationshipProperties( relationship,
                        markWithUnicodeMarker( "{\"name\":\"Something\",\"number\":10}" ) )
                        .getStatus() );
    }

    @Test
    public void shouldAdvertiseUriForQueringAllRelationsInTheDatabase()
    {
        Response response = service.getRoot();
        assertThat( new String( (byte[]) response.getEntity() ),
                containsString( "\"relationship_types\" : \"http://neo4j.org/relationship/types\"" ) );
    }

    @Test
    public void shouldReturnAllLabelsPresentInTheDatabase() throws JsonParseException
    {
        // given
        helper.createNode( Label.label( "ALIVE" ) );
        long nodeId = helper.createNode( Label.label( "DEAD" ) );
        helper.deleteNode( nodeId );

        // when
        Response response = service.getAllLabels( false );

        // then
        assertEquals( 200, response.getStatus() );

        List<String> labels = entityAsList( response );
        assertThat( labels, hasItem( "DEAD" ) );
    }

    @Test
    public void shouldReturnAllLabelsInUseInTheDatabase() throws JsonParseException
    {
        // given
        helper.createNode( Label.label( "ALIVE" ) );
        long nodeId = helper.createNode( Label.label( "DEAD" ) );
        helper.deleteNode( nodeId );

        // when
        Response response = service.getAllLabels( true );

        // then
        assertEquals( 200, response.getStatus() );

        List<String> labels = entityAsList( response );
        assertThat( labels, not( hasItem( "DEAD" ) ) );
    }

    @SuppressWarnings( "unchecked" )
    private String singleErrorCode( Response response ) throws JsonParseException
    {
        String json = entityAsString( response );
        Map<String, Object> map = JsonHelper.jsonToMap( json );
        List<Object> errors = (List<Object>) map.get( "errors" );
        assertEquals( 1, errors.size() );
        Map<String, String> error = (Map<String, String>) errors.get( 0 );
        return error.get( "code" );
    }
}
