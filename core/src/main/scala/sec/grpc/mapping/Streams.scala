package sec
package grpc
package mapping

import java.util.{UUID => JUUID}
import cats._
import cats.implicits._
import com.eventstore.client.streams._
import com.eventstore.client.streams.AppendResp.CurrentRevisionOptions
import com.eventstore.client.streams.ReadReq.Options.{AllOptions, FilterOptions, StreamOptions}
import com.eventstore.client.streams.UUID.Value
import sec.core._

object Streams {

  /// Outgoing

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
    ReadReq.Options.StreamOptions.RevisionOptions.Revision(exact.value)

  def mapEventFilter(filter: EventFilter): Option[ReadReq.Options.FilterOptions] = {

    val expression: Option[FilterOptions.Expression] = (filter.prefixes, filter.regex) match {
      case (Nil, Some(r)) => ReadReq.Options.FilterOptions.Expression(regex = r.value).some
      case (_ :: _, None) => ReadReq.Options.FilterOptions.Expression(prefix = filter.prefixes.map(_.value)).some
      case _              => None
    }

    val window = filter.maxSearchWindow
      .map(ReadReq.Options.FilterOptions.Window.Max)
      .getOrElse(ReadReq.Options.FilterOptions.Window.Count(ReadReq.Empty()))

    val options = ReadReq.Options.FilterOptions().withWindow(window)

    expression.map { e =>
      filter.kind match {
        case EventFilter.Stream    => options.withStreamName(e)
        case EventFilter.EventType => options.withEventType(e)
      }
    }
  }

  /// Incoming

  def requireNonEmpty[F[_]: ApplicativeError[*[_], Throwable], A](t: Option[A], tpe: String): F[A] =
    t.toRight(ProtoResultError(s"Expected non empty $tpe")).liftTo[F]

  def expectUUID[F[_]: MonadError[*[_], Throwable]](uuid: Option[UUID]): F[JUUID] =
    requireNonEmpty(uuid, "UUID") >>= mkUUID[F]

  def mkUUID[F[_]: ApplicativeError[*[_], Throwable]](uuid: UUID): F[JUUID] = {

    val juuid = uuid.value match {
      case Value.Structured(v) => new JUUID(v.mostSignificantBits, v.leastSignificantBits).asRight
      case Value.String(v)     => JUUID.fromString(v).asRight
      case Value.Empty         => "UUID is missing".asLeft
    }

    juuid.leftMap(ProtoResultError).liftTo[F]
  }

  def mkWriteResult[F[_]: ApplicativeError[*[_], Throwable]](ar: AppendResp): F[WriteResult] = {

    val rev: Attempt[StreamRevision.Exact] = ar.currentRevisionOptions match {
      case CurrentRevisionOptions.CurrentRevision(v) => StreamRevision.Exact.exact(v).asRight
      case CurrentRevisionOptions.NoStream(_)        => "Did not expect NoStream when using NonEmptyList".asLeft
      case CurrentRevisionOptions.Empty              => "CurrentRevisionOptions is missing".asLeft
    }

    rev.map(WriteResult(_)).leftMap(ProtoResultError).liftTo[F]
  }

}
