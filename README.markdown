Mesos RxJava [![Build Status](https://teamcity.mesosphere.io/guestAuth/app/rest/builds/buildType:(id:Oss_Mesos_MesosRxJava_Ci)/statusIcon)](https://teamcity.mesosphere.io/viewType.html?buildTypeId=Oss_Mesos_MesosRxJava_Ci&guest=1) [![In Progress](https://badge.waffle.io/mesosphere/mesos-rxjava.png?label=in+progress&title=In+Progress)](https://waffle.io/mesosphere/mesos-rxjava)
============

Mesos RxJava is a library that provides a Reactive Client (via [RxJava](https://github.com/ReactiveX/RxJava)) atop
Apache Mesos' new HTTP APIs.

## Background

Apache Mesos is a Cluster Resource manager providing access to cluster resources such as CPU, RAM, Disk and Ports.
In order to leverage these computing resources a program called a framework is created and registers with Mesos. Once
registered a framework can launch tasks using the resources offered by Mesos.

In an effort to allow Mesos to be more accessible to more languages, HTTP APIs are being developed to allow polyglot
access to authoring frameworks. Prior to the HTTP APIs, libmesos (C++ library) had to be used.


## Project Goal

This project's primary goal is to provide a Java client for interacting with these new HTTP APIs. Mesos' HTTP APIs
are modeled as an event stream; as such a scheduler can be modeled as an
[`rx.Observable<Event>`](http://reactivex.io/RxJava/javadoc/index.html?rx/Observable.html). Once created, the scheduler
is able to use the powerful stream manipulation functions provided by RxJava to react to events.

## Javadocs

Javadocs for the last successful build of `master` can be found [here](https://teamcity.mesosphere.io/guestAuth/repository/download/Oss_Mesos_MesosRxJava_Javadoc/lastSuccessful/javadoc.zip%21/index.html)


## Maven Coordinates

### Stable Release

Releases are available in Maven Central.

### Protobuf Client

```
<dependency>
    <groupId>com.mesosphere.mesos.rx.java</groupId>
    <artifactId>mesos-rxjava-protobuf-client</artifactId>
    <version>0.1.1</version>
</dependency>
```

### Testing Tools

```
<dependency>
    <groupId>com.mesosphere.mesos.rx.java</groupId>
    <artifactId>mesos-rxjava-test</artifactId>
    <version>0.1.1</version>
    <scope>test</scope>
</dependency>
```


### Snapshot Release

#### Snapshot Repo

Version `0.1.0-SNAPSHOT` has been published to the Sonatype OSS Snapshot Repo.
```
<repositories>
    <repository>
        <id>ossrh</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

#### Protobuf Client

```
<dependency>
    <groupId>com.mesosphere.mesos.rx.java</groupId>
    <artifactId>mesos-rxjava-protobuf-client</artifactId>
    <version>0.1.1-SNAPSHOT</version>
</dependency>
```

#### Testing Tools

```
<dependency>
    <groupId>com.mesosphere.mesos.rx.java</groupId>
    <artifactId>mesos-rxjava-test</artifactId>
    <version>0.1.1-SNAPSHOT</version>
    <scope>test</scope>
</dependency>
```

## Build

Mesos RxJava is defined by a Maven project and targeted at Java 1.8.

### Install Maven

Install Maven 3.2.x or newer

### Running Tests

```
mvn clean test
```

### Packaging Artifacts

```
mvn clean package
```


## Resources

1. [Apache Mesos](http://mesos.apache.org/)
1. [Mesos HTTP Scheduler API v1](https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md)
2. [RxJava](https://github.com/ReactiveX/RxJava)
3. [ReactiveX](http://reactivex.io/)
