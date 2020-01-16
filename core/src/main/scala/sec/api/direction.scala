package sec
package api

//======================================================================================================================

sealed trait Direction
object Direction {
  case object Forwards  extends Direction
  case object Backwards extends Direction
}

//======================================================================================================================
