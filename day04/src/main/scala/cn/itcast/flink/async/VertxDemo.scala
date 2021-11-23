package cn.itcast.flink.async



import io.vertx.core._
import io.vertx.redis.{RedisClient, RedisOptions}

object VertxDemo {
  def main(args: Array[String]): Unit = {
    val config = new RedisOptions()
    config.setHost("node2")
    config.setPort(6379)

    val vertxOptions = new VertxOptions()
    vertxOptions.setEventLoopPoolSize(10)
    vertxOptions.setWorkerPoolSize(20)

    val vertx = Vertx.vertx(vertxOptions)
   var redisClient = RedisClient.create(vertx,config)
    redisClient.hget("REDISSINK","hello",new Handler[AsyncResult[String]] {
      override def handle(event: AsyncResult[String]): Unit = {
        if (event.succeeded()){
          val str = event.result()
          println(str)

        }
      }
    })
  }
}

class VertxDemo extends AbstractVerticle{
  override def start(): Unit = super.start()

}