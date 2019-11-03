package sec

import cats.implicits._
import cats.effect._

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    IO(println("Here be dragons!")).as(ExitCode.Success)
  }
  
}