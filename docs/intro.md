---
id: intro
title: Overview
sidebar_label: Overview
slug: /
---

 - @libName@ is a [EventStoreDB v22.10+](https://www.eventstore.com) client library for Scala.
 - @libName@ is purely functional, non-blocking, and provides a tagless-final API.
 - @libName@ embraces the [Scala Code of Conduct](https://www.scala-lang.org/conduct).

### Using @libName@

To use @libName@ in an existing [sbt](https://www.scala-sbt.org) project with Scala 3 or a later version, 
add the following to your `build.sbt` file.
```scala
libraryDependencies += "io.github.ahjohannessen" %% "sec-fs2-client" % "@libVersion@"
```

### How to learn

In order to use @libName@ effectively you need some prerequisites:

- It is assumed you are comfortable with [EventStoreDB](https://www.eventstore.com).
- It is assumed you are comfortable with [cats](https://typelevel.org/cats), [cats-effect](https://typelevel.org/cats-effect), 
  and [fs2](https://fs2.io).

If you feel not having the necessary prerequisites, the linked websites have many learning resources.

#### Learning about @libName@

In the following sections you will learn about:

- Basics about [types](types.md) and [API](client-api.md) used for interacting with EventStoreDB.
- Using the **Streams API** for [writing](writing.md) data, [reading](reading.md) data, [subscribing](subscribing.md) to streams,
  [deleting](deleting.md) data. Moreover, you will also learn about manipulating [metadata](metastreams.md).
- Connecting to a [single node](config.md#single-node) or a [cluster](config.md#cluster).  
- Various [configuration](config.md) for connections, retries and authentication.  


### License

@libName@ is licensed under [Apache 2.0](@libGithubRepo@/blob/master/LICENSE).
