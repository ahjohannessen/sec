package sec
package cluster
package grpc

import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._
import cats.implicits._
import io.grpc.ConnectivityState._
import io.grpc.ConnectivityStateInfo
import io.grpc.LoadBalancer
import io.grpc.LoadBalancer._
import io.grpc.Status

final class GossipLb(val helper: Helper) extends LoadBalancer {
  import GossipLb._

  private var subchannel: Option[Subchannel] = None

  override def handleResolvedAddresses(resolvedAddresses: ResolvedAddresses): Unit = {

    val servers = resolvedAddresses.getAddresses

    println(s"GossipLb->Got addresses:\n${servers.asScala.toList
      .flatMap { eag =>
        val tag: String = eag.getAttributes.get[String](Notifier.gossip.endpointTag)
        eag.getAddresses.asScala.toList.map(sa => s"  $sa => [$tag]")
      }
      .mkString("\n")}")

    def create(): Unit = {
      val args = CreateSubchannelArgs.newBuilder().setAddresses(servers).build()
      val sb   = helper.createSubchannel(args)
      sb.start(processSubchannelState(sb, _))
      subchannel = Option(sb)
      helper.updateBalancingState(CONNECTING, new Picker(PickResult.withSubchannel(sb)))
      sb.requestConnection()
    }

    subchannel.fold(create())(_.updateAddresses(servers))
  }

  def handleNameResolutionError(error: Status): Unit = {
    subchannel = subchannel >>= { sb =>
      sb.shutdown()
      None
    }

    helper.updateBalancingState(TRANSIENT_FAILURE, new Picker(PickResult.withError(error)))
  }

  def shutdown(): Unit                   = subchannel.foreach(_.shutdown())
  override def requestConnection(): Unit = subchannel.foreach(_.requestConnection())

  ///

  private def processSubchannelState(sb: Subchannel, stateInfo: ConnectivityStateInfo): Unit = {

    val picker = stateInfo.getState match {
      case IDLE              => new RequestConnectionPicker(helper, sb).some
      case CONNECTING        => new Picker(PickResult.withNoResult()).some
      case READY             => new Picker(PickResult.withSubchannel(sb)).some
      case TRANSIENT_FAILURE => new Picker(PickResult.withError(stateInfo.getStatus)).some
      case SHUTDOWN          => none[SubchannelPicker]
    }

    picker.foreach(helper.updateBalancingState(stateInfo.getState, _))
  }

}

object GossipLb {

  final class Picker(result: PickResult) extends SubchannelPicker {
    def pickSubchannel(args: PickSubchannelArgs): PickResult = result
  }

  final class RequestConnectionPicker(val helper: Helper, val sb: Subchannel) extends SubchannelPicker {
    val connectionRequested: AtomicBoolean = new AtomicBoolean(false)

    def pickSubchannel(args: PickSubchannelArgs): PickResult = {
      if (connectionRequested.compareAndSet(false, true))
        helper.getSynchronizationContext.execute(() => sb.requestConnection())
      PickResult.withNoResult()
    }
  }

}
