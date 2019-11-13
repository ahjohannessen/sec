package sec
package grpc
package mapping

import sec.core.{EventNumber, Position, ReadDirection, StreamRevision}

object Streams {

  import com.eventstore.client.streams.ReadReq.Options.{AllOptions, StreamOptions}
  import com.eventstore.client.streams._

  val startOfAll    = AllOptions.AllOptions.Start(ReadReq.Empty())
  val endOfAll      = AllOptions.AllOptions.Position(ReadReq.Options.Position(-1L, -1L))
  val startOfStream = StreamOptions.RevisionOptions.Start(ReadReq.Empty())
  val endOfStream   = StreamOptions.RevisionOptions.Revision(-1L)

  val mapReadAllPosition: Position => ReadReq.Options.AllOptions.AllOptions = {
    case Position.Start       => startOfAll
    case Position.Exact(c, p) => ReadReq.Options.AllOptions.AllOptions.Position(ReadReq.Options.Position(c, p))
    case Position.End         => endOfAll
  }

  val mapReadStreamRevision: EventNumber => ReadReq.Options.StreamOptions.RevisionOptions = {
    case EventNumber.Start      => startOfStream
    case EventNumber.Exact(rev) => StreamOptions.RevisionOptions.Revision(rev)
    case EventNumber.End        => endOfStream
  }

  val mapAppendRevision: StreamRevision => AppendReq.Options.ExpectedStreamRevision = {
    case StreamRevision.Exact(v)     => AppendReq.Options.ExpectedStreamRevision.Revision(v)
    case StreamRevision.NoStream     => AppendReq.Options.ExpectedStreamRevision.NoStream(AppendReq.Empty())
    case StreamRevision.StreamExists => AppendReq.Options.ExpectedStreamRevision.StreamExists(AppendReq.Empty())
    case StreamRevision.Any          => AppendReq.Options.ExpectedStreamRevision.Any(AppendReq.Empty())
  }

  val mapDeleteRevision: StreamRevision => DeleteReq.Options.ExpectedStreamRevision = {
    case StreamRevision.Exact(v)     => DeleteReq.Options.ExpectedStreamRevision.Revision(v)
    case StreamRevision.NoStream     => DeleteReq.Options.ExpectedStreamRevision.NoStream(DeleteReq.Empty())
    case StreamRevision.StreamExists => DeleteReq.Options.ExpectedStreamRevision.StreamExists(DeleteReq.Empty())
    case StreamRevision.Any          => DeleteReq.Options.ExpectedStreamRevision.Any(DeleteReq.Empty())
  }

  val mapTombstoneRevision: StreamRevision => TombstoneReq.Options.ExpectedStreamRevision = {
    case StreamRevision.Exact(v) => TombstoneReq.Options.ExpectedStreamRevision.Revision(v)
    case StreamRevision.NoStream => TombstoneReq.Options.ExpectedStreamRevision.NoStream(TombstoneReq.Empty())
    case StreamRevision.StreamExists =>
      TombstoneReq.Options.ExpectedStreamRevision.StreamExists(TombstoneReq.Empty())
    case StreamRevision.Any => TombstoneReq.Options.ExpectedStreamRevision.Any(TombstoneReq.Empty())
  }

  val mapDirection: ReadDirection => ReadReq.Options.ReadDirection = {
    case ReadDirection.Forward  => ReadReq.Options.ReadDirection.Forwards
    case ReadDirection.Backward => ReadReq.Options.ReadDirection.Backwards
  }

  def mapPosition(exact: Position.Exact): ReadReq.Options.AllOptions.AllOptions.Position =
    ReadReq.Options.AllOptions.AllOptions.Position(ReadReq.Options.Position(exact.commit, exact.prepare))

  def mapRevision(exact: EventNumber.Exact): ReadReq.Options.StreamOptions.RevisionOptions.Revision =
    ReadReq.Options.StreamOptions.RevisionOptions.Revision(exact.revision)
}
