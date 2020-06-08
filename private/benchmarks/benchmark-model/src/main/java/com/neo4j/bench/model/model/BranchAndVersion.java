/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import static java.lang.String.format;

public class BranchAndVersion
{
    public static void validate( Repository repository, String owner, String branch )
    {
        boolean isDefaultOwner = repository.isDefaultOwner( owner );
        boolean isStandardBranch = repository.isStandardBranch( branch.toLowerCase() );

        if ( isDefaultOwner && !isStandardBranch )
        {
            throw new RuntimeException(
                    format(
                            "Branch of repository '%s' owned by '%s' must follow the pattern '%s' but was '%s'",
                            repository.projectName(),
                            owner,
                            repository.standardBranchPattern(),
                            branch
                    ) );
        }
        if ( !isDefaultOwner && isStandardBranch )
        {
            throw new RuntimeException(
                    format(
                            "Forks of repository '%s' owned by '%s' must not follow the pattern '%s' but was '%s",
                            repository.projectName(),
                            owner,
                            repository.standardBranchPattern(),
                            branch
                    ) );
        }
    }

    public static boolean isPersonalBranch( Repository repository, String owner )
    {
        return !repository.isDefaultOwner( owner );
    }

    public static boolean branchEqualsSeries( String version, String branch )
    {
        String seriesString = versionToBranch( version );
        return branch.equalsIgnoreCase( seriesString );
    }

    public static void assertBranchEqualsSeries( String version, String branch )
    {
        if ( !branchEqualsSeries( version, branch ) )
        {
            throw new RuntimeException( format( "Branch (%s) must be same as series (%s) for non-personal branches",
                                                branch, versionToBranch( version ) ) );
        }
    }

    private static String versionToBranch( String version )
    {
        int i = version.lastIndexOf( '.' );
        return (-1 == i) ? version : version.substring( 0, i );
    }

    public static String toSanitizeVersion( Repository repository, String version )
    {
        int i = version.indexOf( '-' );
        String sanitizedVersion = (-1 == i) ? version : version.substring( 0, i );
        repository.assertValidVersion( sanitizedVersion );
        return sanitizedVersion;
    }
}
