import sbt._
import scala.Console._

object Prompt {

  private lazy val isANSISupported = sys.env
    .get("sbt.log.noformat")
    .map(_ != "true")
    .orElse(sys.env.get("os.name").map(_.toLowerCase).filter(_.contains("windows")).map(_ => false))
    .getOrElse(true)

  private def cyan(str: String) =
    if (isANSISupported) CYAN + str + RESET else str

  val enrichedShellPrompt: State => String = { state: State =>
    s"[${cyan(Project.extract(state).currentProject.id)}] λ "
  }

}
