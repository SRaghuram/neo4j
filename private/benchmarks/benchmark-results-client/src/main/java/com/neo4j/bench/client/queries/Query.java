package com.neo4j.bench.client.queries;

import java.util.Optional;

import org.neo4j.driver.v1.Driver;

public interface Query<RESULT>
{
    RESULT execute( Driver driver );

    Optional<String> nonFatalError();
}
