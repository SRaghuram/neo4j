package com.neo4j.bench.client.model;

import static com.neo4j.bench.client.model.Repository.RONJA_BENCH;

import static java.lang.String.format;

public class BranchAndVersion
{
    public static void validate( Repository repository, String owner, String branch )
    {
        boolean isDefaultOwner = repository.isDefaultOwner( owner );
        boolean isStandardBranch = repository.isStandardBranch( branch.toLowerCase() );

        if ( repository.equals( RONJA_BENCH ) && !isStandardBranch )
        {
            throw new RuntimeException( format( "'%s' must always have owner '%s' and branch names must always be named 'x.y', but: owner='%s' & branch='%s'",
                                                RONJA_BENCH.projectName(), RONJA_BENCH.defaultOwner(), owner, branch ) );
        }

        if ( isDefaultOwner && !isStandardBranch )
        {
            throw new RuntimeException(
                    format( "Branch of repository '%s' owned by '%s' must be named 'x.y' but was '%s'", repository.projectName(), owner, branch ) );
        }
        if ( !isDefaultOwner && isStandardBranch )
        {
            throw new RuntimeException(
                    format( "Forks of repository '%s' owned by '%s' must not repository branch 'x.y' but was '%s'", repository.projectName(), owner, branch ) );
        }

        if ( repository.equals( RONJA_BENCH ) && !isStandardBranch )
        {
            throw new RuntimeException(
                    format( "branches of '%s' owned by '%s' must always be named 'x.y' but was: %s", repository.projectName(), owner, branch ) );
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
        int i = version.lastIndexOf( "." );
        return (-1 == i) ? version : version.substring( 0, i );
    }

    public static String toSanitizeVersion( Repository repository, String version )
    {
        int i = version.indexOf( "-" );
        String sanitizedVersion = (-1 == i) ? version : version.substring( 0, i );
        repository.assertValidVersion( sanitizedVersion );
        return sanitizedVersion;
    }
}
