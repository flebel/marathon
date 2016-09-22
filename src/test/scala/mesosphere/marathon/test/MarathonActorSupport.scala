package mesosphere.marathon.test

import mesosphere.marathon.MarathonSpec
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, Materializer }
import akka.testkit.{ TestKit, TestKitBase }
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

/**
  * Start an actor system for all test methods and provide akka TestKit utility methods.
  */
trait MarathonActorSupport extends MarathonSpec with BeforeAndAfterAll with TestKitBase {

  val log = LoggerFactory.getLogger(getClass)

  /** Make sure that top-level actors in tests die if they throw an exception. */
  private[this] lazy val stoppingConfigStr =
    """ akka.actor.guardian-supervisor-strategy = "akka.actor.StoppingSupervisorStrategy" """
  private[this] lazy val stoppingConfig = ConfigFactory.parseString(stoppingConfigStr)

  implicit lazy val system: ActorSystem = ActorSystem(getClass.getSimpleName, stoppingConfig)
  implicit lazy val mat: Materializer = ActorMaterializer()
  log.info("actor system {}: starting", system.name)

  override protected def afterAll(): Unit = {
    super.afterAll()
    log.info("actor system {}: shutting down", system.name)
    TestKit.shutdownActorSystem(system)
  }
}
