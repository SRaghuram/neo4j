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
package org.neo4j.server.rest.repr;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.http.cypher.TransactionStateChecker;

import static org.neo4j.internal.helpers.collection.MapUtil.map;

public final class RelationshipRepresentation extends ObjectRepresentation implements ExtensibleRepresentation,
        EntityRepresentation
{
    private final Relationship rel;
    private final GraphDatabaseService databaseService;
    private TransactionStateChecker checker;

    public RelationshipRepresentation( Relationship rel, GraphDatabaseService databaseService )
    {
        super( RepresentationType.RELATIONSHIP );
        this.rel = rel;
        this.databaseService = databaseService;
    }

    public void setTransactionStateChecker( TransactionStateChecker checker )
    {
        this.checker = checker;
    }

    @Override
    public String getIdentity()
    {
        return Long.toString( rel.getId() );
    }

    public long getId()
    {
        return rel.getId();
    }

    @Override
    @Mapping( "self" )
    public ValueRepresentation selfUri()
    {
        return ValueRepresentation.uri( path( "" ) );
    }

    private String path( String path )
    {
        return "relationship/" + rel.getId() + path;
    }

    static String path( Relationship rel )
    {
        return "relationship/" + rel.getId();
    }

    @Mapping( "type" )
    public ValueRepresentation getType()
    {
        return ValueRepresentation.relationshipType( rel.getType() );
    }

    @Mapping( "start" )
    public ValueRepresentation startNodeUri()
    {
        return ValueRepresentation.uri( NodeRepresentation.path( rel.getStartNode() ) );
    }

    @Mapping( "end" )
    public ValueRepresentation endNodeUri()
    {
        return ValueRepresentation.uri( NodeRepresentation.path( rel.getEndNode() ) );
    }

    @Mapping( "properties" )
    public ValueRepresentation propertiesUri()
    {
        return ValueRepresentation.uri( path( "/properties" ) );
    }

    @Mapping( "property" )
    public ValueRepresentation propertyUriTemplate()
    {
        return ValueRepresentation.template( path( "/properties/{key}" ) );
    }

    @Mapping( "metadata" )
    public MapRepresentation metadata()
    {
        if ( isDeleted() )
        {
            return new MapRepresentation( map( "id", rel.getId(), "deleted", Boolean.TRUE ), databaseService );
        }
        else
        {
            return new MapRepresentation( map( "id", rel.getId(), "type", rel.getType().name() ), databaseService );
        }
    }

    private boolean isDeleted()
    {
        return checker != null && checker.isRelationshipDeletedInCurrentTx( rel.getId() );
    }

    @Override
    public void extraData( MappingSerializer serializer )
    {
        if ( !isDeleted() )
        {
            MappingWriter properties = serializer.writer.newMapping( RepresentationType.PROPERTIES, "data" );
            new PropertiesRepresentation( rel ).serialize( properties );
            properties.done();
        }
    }
}
