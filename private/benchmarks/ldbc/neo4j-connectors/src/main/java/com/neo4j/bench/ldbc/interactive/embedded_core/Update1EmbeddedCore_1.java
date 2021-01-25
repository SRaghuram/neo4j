/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Organisation;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.StudiesAt;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.Domain.WorksAt;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate1;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class Update1EmbeddedCore_1 extends Neo4jUpdate1<Neo4jConnectionState>
{
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate1AddPerson operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = connection.getTx().createNode( Nodes.Person );
        person.setProperty( Person.ID, operation.personId() );
        person.setProperty( Person.FIRST_NAME, operation.personFirstName() );
        person.setProperty( Person.LAST_NAME, operation.personLastName() );
        person.setProperty( Person.GENDER, operation.gender() );
        person.setProperty( Person.BIRTHDAY, dateUtil.utcToFormat( operation.birthday().getTime() ) );
        person.setProperty( Person.CREATION_DATE, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        person.setProperty( Person.LOCATION_IP, operation.locationIp() );
        person.setProperty( Person.BROWSER_USED, operation.browserUsed() );
        person.setProperty( Person.LANGUAGES,
                operation.languages().toArray( new String[0] ) );
        person.setProperty( Person.EMAIL_ADDRESSES,
                operation.emails().toArray( new String[0] ) );

        Node city = Operators.findNode( connection.getTx(), Place.Type.City, Place.ID, operation.cityId() );
        person.createRelationshipTo( city, Rels.PERSON_IS_LOCATED_IN );

        for ( Long tagId : operation.tagIds() )
        {
            Node tag = Operators.findNode( connection.getTx(), Nodes.Tag, Tag.ID, tagId );
            person.createRelationshipTo( tag, Rels.HAS_INTEREST );
        }

        for ( LdbcUpdate1AddPerson.Organization organization : operation.studyAt() )
        {
            Node university = Operators.findNode( connection.getTx(), Organisation.Type.University,
                    Organisation.ID,
                    organization.organizationId() );
            Relationship studyAt = person.createRelationshipTo( university, Rels.STUDY_AT );
            studyAt.setProperty( StudiesAt.CLASS_YEAR, organization.year() );
        }

        int minWorkFromYear = Integer.MAX_VALUE;
        int maxWorkFromYear = Integer.MIN_VALUE;

        for ( LdbcUpdate1AddPerson.Organization organization : operation.workAt() )
        {
            Node company = Operators.findNode( connection.getTx(), Organisation.Type.Company,
                    Organisation.ID,
                    organization.organizationId() );
            RelationshipType workAtForYear =
                    connection.timeStampedRelationshipTypesCache().worksAtForYear( organization.year() );
            Relationship workAt = person.createRelationshipTo( company, workAtForYear );
            workAt.setProperty( WorksAt.WORK_FROM, organization.year() );

            if ( organization.year() < minWorkFromYear )
            {
                minWorkFromYear = organization.year();
            }
            if ( organization.year() > maxWorkFromYear )
            {
                maxWorkFromYear = organization.year();
            }
        }

        if ( Integer.MAX_VALUE != minWorkFromYear )
        {
            connection.timeStampedRelationshipTypesCache().resizeWorksAtForNewYear( minWorkFromYear );
        }
        if ( Integer.MIN_VALUE != maxWorkFromYear )
        {
            connection.timeStampedRelationshipTypesCache().resizeWorksAtForNewYear( maxWorkFromYear );
        }

        return LdbcNoResult.INSTANCE;
    }
}
