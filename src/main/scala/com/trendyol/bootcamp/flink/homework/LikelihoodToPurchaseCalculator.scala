package com.trendyol.bootcamp.flink.homework

import com.trendyol.bootcamp.flink.common.RandomEventSource
import com.trendyol.bootcamp.flink.common._
import com.trendyol.bootcamp.flink.example.UserStats
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration


case class PurchaseLikelihood(userId: Int, productId: Int, likelihood: Double)

object LikelihoodToPurchaseCalculator {

  val l2pCoefficients = Map(
    AddToBasket         -> 0.4,
    RemoveFromBasket    -> -0.2,
    AddToFavorites      -> 0.7,
    RemoveFromFavorites -> -0.2,
    DisplayBasket       -> 0.5
  )

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 15000))

    val keyedStream = env
      .addSource(new RandomEventSource)
      .filter(e => l2pCoefficients.keySet.contains(e.eventType))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(20))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[Event] {
              override def extractTimestamp(element: Event, recordTimestamp: Long): Long =
                element.timestamp
            }
          )
      )
      .keyBy(_.userId)

    keyedStream
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      .process(
        new ProcessWindowFunction[Event, PurchaseRatio, Int, TimeWindow] {
          override def process(key: Int, context: Context, elements: Iterable[Event], out: Collector[PurchaseRatio]): Unit = {

            val ratio = elements.map(event => l2pCoefficients.getOrElse(event.eventType,1.0)).product
            out.collect(PurchaseRatio(key,ratio))
          }
        }
      )
      .addSink(new PrintSinkFunction[PurchaseRatio])

    env.execute("Likelihood Streamer")
  }

  case class PurchaseRatio(userId: Int, ratio: Double)
}
