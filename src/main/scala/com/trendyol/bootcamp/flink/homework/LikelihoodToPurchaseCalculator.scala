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

  val l2pCoKeys = l2pCoefficients.keySet

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 15000))

    val keyedStream = env
      .addSource(new RandomEventSource)
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
      .keyBy(event => (event.userId, event.productId))

    keyedStream
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      .process(
        new ProcessWindowFunction[Event, PurchaseRatio, (Int,Int), TimeWindow] {
          override def process(key: (Int,Int), context: Context, elements: Iterable[Event], out: Collector[PurchaseRatio]): Unit = {

            val ratio = getPurchaseRatio(elements.toList)

            out.collect(PurchaseRatio(key._1, key._2, ratio))
          }
        }
      )
      .addSink(new PrintSinkFunction[PurchaseRatio])

    env.execute("Likelihood Streamer")
  }


  /**
   * Satın alma ihtimaline etki eden en az bir eleman varsa
   * hesaplama yapılır ve hesaplama sonucu (Double) döndürülür yoksa
   * yarıya yarıya anlamında 0.5 döndürülür.
   * */
  def getPurchaseRatio(eventList: List[Event]): Double =
    if(hasAtLeastOneValidEvent(eventList)) calculatePurchaseRatio(eventList)
    // Veri yoksa satın alma ihtimali %50 dir.
    else 0.5

  /**
   * 'eventList' teki her bir eleman için 'sum' a varsa etkisi
   * yoksa 0 eklenir ve sonuç döndürülür.
   * */
  def calculatePurchaseRatio(eventList: List[Event]): Double =
    eventList
      .foldLeft(0.0)((sum,event) => sum + l2pCoefficients.getOrElse(event.eventType,0.0))

  /**
   * 'eventList'te 'eventType'ı 'keys'te olan en az bir 'Event' varsa
   * true yoksa false döndürülür.
   * */
  def hasAtLeastOneValidEvent(eventList: List[Event]): Boolean =
    eventList
      .exists(event => l2pCoKeys.contains(event.eventType))

  case class PurchaseRatio( userId: Int, productId: Int, ratio: Double)
}
