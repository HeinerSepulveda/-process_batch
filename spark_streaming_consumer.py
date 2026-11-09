"""Ventanas de un minuto con tiempo de evento y checkpoint persistente."""
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--checkpoint", default="checkpoints/sensores")
    ap.add_argument("--output", default="resultados_streaming")
    ap.add_argument("--seconds", type=int, default=0)
    args = ap.parse_args()
    out = Path(args.output); out.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName("SensorDataAnalysis")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    schema = T.StructType([
        T.StructField("sensor_id", T.IntegerType()),
        T.StructField("temperature", T.DoubleType()),
        T.StructField("humidity", T.DoubleType()),
        T.StructField("timestamp", T.LongType())])
    raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", "sensor_data")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true").load())
    parsed = raw.select(F.from_json(F.col("value").cast("string"), schema)
                        .alias("data")).select("data.*")
    valid = (parsed.filter(F.col("sensor_id").between(1, 10) &
        F.col("temperature").between(20, 30) & F.col("humidity").between(30, 70) &
        (F.col("timestamp") > 0))
        .withColumn("event_time", F.timestamp_seconds("timestamp")))
    stats = (valid.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute"), "sensor_id")
        .agg(F.count("*").alias("events"),
             F.avg("temperature").alias("temperature_avg"),
             F.avg("humidity").alias("humidity_avg")))

    def show_batch(batch, batch_id):
        batch.orderBy("window", "sensor_id").show(50, truncate=False)
        # Salida acotada: por watermark solo quedan ventanas recientes.
        rows = [r.asDict(recursive=True) for r in batch.collect()]
        (out / f"batch_{batch_id:05d}.json").write_text(
            json.dumps(rows, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    query = (stats.writeStream.outputMode("update").foreachBatch(show_batch)
        .option("checkpointLocation", str(Path(args.checkpoint).resolve()))
        .trigger(processingTime="5 seconds").start())
    try:
        if args.seconds:
            query.awaitTermination(args.seconds)
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        progress = query.recentProgress
        query.stop()
        (out / "progress.json").write_text(json.dumps(progress, indent=2, default=str),
                                            encoding="utf-8")
        spark.stop()


if __name__ == "__main__":
    main()
