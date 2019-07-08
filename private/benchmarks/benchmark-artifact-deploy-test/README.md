# how to test artifacts deployment

You have to test it in isolation, to make sure you, you are not using cached artifacts. 
So the best way to do it is to run it under docker.

First you have to set `neo4j.version` property, to a version you are going to test:

    docker run --rm -v $(pwd):/work -w /work maven:3.6.1-jdk-8-slim mvn versions:set-property -Dproperty="neo4j.version" -DnewVersion="3.4.15-SNAPSHOT"
    
Once it is done, you can then try to resolve dependencies:

    docker run --rm -v $(pwd):/work -w /work maven:3.6.1-jdk-8-slim mvn dependency:resolve