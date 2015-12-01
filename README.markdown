Mesos RxJava [![Build Status](https://teamcity.mesosphere.io/guestAuth/app/rest/builds/buildType:(id:Oss_Mesos_MesosRxJava_Ci)/statusIcon)](https://teamcity.mesosphere.io/viewType.html?buildTypeId=Oss_Mesos_MesosRxJava_Ci&guest=1) [![Stories in Ready](https://badge.waffle.io/mesosphere/mesos-rxjava.png?label=in+progress&title=In+Progress)](https://waffle.io/mesosphere/mesos-rxjava)
============

Mesos RxJava is a library that provides a Reactive Client (via [RxJava](https://github.com/ReactiveX/RxJava)) atop
Apache Mesos' new HTTP APIs.  As the Apache Mesos HTTP APIs are experimental this library should also be considered
experimental. _The project reserves the right to change any interface at any time until the first release is made._

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


## Project Status

The project is still early in terms of development but it is possible to launch tasks. The issues section of this repo
will soon be updated with tasks that will require work and is the best place to check on the status toward the first
release.


## Maven Coordinates

Releases will be published to Maven Central. Snapshots will be published to Sonatypes OSS Repo.

More details once artifacts are published.

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
