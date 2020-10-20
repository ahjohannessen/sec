---
id: setup
title: Quick Start
sidebar_label: Quick Start
---
In order to learn about @libName@ in the following sections you will need a basic setup.
Below are instructions to get you up and running. 

### @esdb@ Setup

First we need to get an @esdb@ node loaded up. One easy way to do this is to use docker and start up a single 
node in in-secure mode as follows:
```console
docker run -it --rm --name es-node \
-p 2113:2113 \
-e EVENTSTORE_MEM_DB=True \
-e EVENTSTORE_INSECURE=True \
-e EVENTSTORE_GOSSIP_ON_SINGLE_NODE=True \
-e EVENTSTORE_DISCOVER_VIA_DNS=False \
-e EVENTSTORE_START_STANDARD_PROJECTIONS=True \
eventstore/eventstore:20.6.1-bionic
```

The above is enough to get something up and running for the purpose of exploration and learning. Consult the 
[docs](https://developers.eventstore.com/server/20.6/server/installation/) for more details on how 
you configure @esdb@ for production usage.

### Scala Setup

Create a new project with @libName@ as dependency:
```scala
libraryDependencies += "io.github.ahjohannessen" %% "sec-fs2" % "@libVersion@"
```

### Verify Your Setup

In order to verify that you can reach the database try out the
following [IOApp](https://typelevel.org/cats-effect/datatypes/ioapp.html):

```scala mdoc:compile-only
import cats.syntax.all._
import cats.effect._
import sec.api._

object HelloWorld extends IOApp {

  def run(args: List[String]): IO[ExitCode] = EsClient
    .singleNode[IO]("127.0.0.1", 2113)
    .resource.use(client => 
      client.gossip.read.flatMap(ci => IO(println(ci.show)))
    )
    .as(ExitCode.Success)

}
```

The methods used on `EsClient` are explained in the subsequent sections.
