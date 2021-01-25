/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate1;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class Neo4jUpdate1EmbeddedCypher extends Neo4jUpdate1<Neo4jConnectionState>
{
    protected static final String PERSON_PARAMS_STRING = PERSON_PARAMS.toString();
    protected static final String PERSON_CITY_ID_STRING = PERSON_CITY_ID.toString();
    protected static final String PERSON_TAG_IDS_STRING = PERSON_TAG_IDS.toString();
    protected static final String PERSON_STUDY_ATS_STRING = PERSON_STUDY_ATS.toString();
    protected static final String PERSON_WORK_ATS_STRING = PERSON_WORK_ATS.toString();

    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate1AddPerson operation )
            throws DbException
    {
        connection.execute(
                connection.queries().queryFor( operation ).queryString(),
                buildParams( operation, connection.dateUtil(), connection.calendar() ) );
        return LdbcNoResult.INSTANCE;
    }

    private Map<String,Object> buildParams(
            LdbcUpdate1AddPerson operation,
            QueryDateUtil dateUtil,
            Calendar calendar )
    {
        Map<String,Object> personParams = new HashMap<>();
        personParams.put( Person.ID, operation.personId() );
        personParams.put( Person.FIRST_NAME, operation.personFirstName() );
        personParams.put( Person.LAST_NAME, operation.personLastName() );
        personParams.put( Person.GENDER, operation.gender() );
        personParams.put( Person.BIRTHDAY, dateUtil.utcToFormat( operation.birthday().getTime() ) );
        personParams.put( Person.BROWSER_USED, operation.browserUsed() );
        calendar.setTime( operation.birthday() );
        // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
        personParams.put( Person.BIRTHDAY_MONTH, calendar.get( Calendar.MONTH ) + 1 );
        personParams.put( Person.BIRTHDAY_DAY_OF_MONTH, calendar.get( Calendar.DAY_OF_MONTH ) );
        personParams.put( Person.CREATION_DATE, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        personParams.put( Person.LOCATION_IP, operation.locationIp() );
        personParams.put( Person.LANGUAGES, operation.languages() );
        personParams.put( Person.EMAIL_ADDRESSES, operation.emails() );

        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_PARAMS_STRING, personParams );
        queryParams.put( PERSON_CITY_ID_STRING, operation.cityId() );
        queryParams.put( PERSON_TAG_IDS_STRING, operation.tagIds() );

        int[][] studyAts = new int[operation.studyAt().size()][2];
        for ( int i = 0; i < operation.studyAt().size(); i++ )
        {
            LdbcUpdate1AddPerson.Organization studyAt = operation.studyAt().get( i );
            studyAts[i] = new int[]{(int) studyAt.organizationId(), studyAt.year()};
        }
        queryParams.put( PERSON_STUDY_ATS_STRING, studyAts );
        int[][] workAts = new int[operation.workAt().size()][2];
        for ( int i = 0; i < operation.workAt().size(); i++ )
        {
            LdbcUpdate1AddPerson.Organization workAt = operation.workAt().get( i );
            workAts[i] = new int[]{(int) workAt.organizationId(), workAt.year()};
        }
        queryParams.put( PERSON_WORK_ATS_STRING, workAts );
        return queryParams;
    }
}
