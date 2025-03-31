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
-e KURRENTDB_MEM_DB=True \
-e KURRENTDB_INSECURE=True \
-e KURRENTDB_GOSSIP_ON_SINGLE_NODE=True \
-e KURRENTDB_DISCOVER_VIA_DNS=False \
-e KURRENTDB_START_STANDARD_PROJECTIONS=True \
docker.cloudsmith.io/eventstore/kurrent-latest/kurrentdb:25.0.0
```

The above is enough to get something up and running for the purpose of exploration and learning. Consult the 
@esdb@ [docs](https://docs.kurrent.io/server/v25.0/configuration) for more details on
configuration for production setup.

### Scala Setup

Create a new project with @libName@ as dependency:
```scala
libraryDependencies += "io.github.ahjohannessen" %% "sec-fs2" % "@libVersion@"
```

### Verify Your Setup

In order to verify that you can reach the database try out the
following [IOApp](https://typelevel.org/cats-effect/datatypes/ioapp.html):

```scala mdoc:compile-only
import cats.effect.*
import sec.api.*

object HelloWorld extends IOApp:

  def run(args: List[String]): IO[ExitCode] = EsClient
    .singleNode[IO](Endpoint("127.0.0.1", 2113))
    .resource.use(client => 
      client.gossip.read.map(_.render).flatMap(str => IO(println(str)))
    )
    .as(ExitCode.Success)

```

The methods used on `EsClient` are explained in the subsequent sections.
