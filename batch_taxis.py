"""Análisis reproducible del archivo mensual completo de TLC con PySpark."""
import argparse
import csv
import hashlib
import json
import platform
import time
import urllib.request
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="datos/yellow_tripdata_2025-01.parquet")
    ap.add_argument("--output", default="resultados")
    args = ap.parse_args()
    path, out = Path(args.input), Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(URL, path)
    spark = (SparkSession.builder.appName("Tarea3TaxisTLC")
             .config("spark.sql.session.timeZone", "America/New_York")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    start = time.time()
    raw = spark.read.parquet(str(path.resolve()))
    raw.printSchema()
    raw.show(5, truncate=False)
    fields = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
              "total_amount", "PULocationID", "payment_type"]
    nulls = raw.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c)
                        for c in fields]).first().asDict()
    n_raw = raw.count()
    unique = raw.dropDuplicates().cache()
    n_unique = unique.count()
    enriched = unique.withColumn(
        "duration_min", F.expr("timestampdiff(SECOND, tpep_pickup_datetime, "
                                "tpep_dropoff_datetime) / 60.0"))
    required = F.lit(True)
    for c in fields:
        required = required & F.col(c).isNotNull()
    dated = enriched.filter(required &
        (F.col("tpep_pickup_datetime") >= F.lit("2025-01-01").cast("timestamp")) &
        (F.col("tpep_pickup_datetime") < F.lit("2025-02-01").cast("timestamp")))
    n_dated = dated.count()
    clean = dated.filter(
        (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 100) &
        (F.col("duration_min") > 0) & (F.col("duration_min") <= 180) &
        (F.col("total_amount") > 0) & (F.col("total_amount") <= 500) &
        F.col("PULocationID").between(1, 263) &
        ~F.isnan("trip_distance") & ~F.isnan("total_amount")
    ).cache()
    n_clean = clean.count()
    overview = clean.agg(
        F.avg("duration_min").alias("duration_mean_min"),
        F.avg("trip_distance").alias("distance_mean_miles"),
        F.avg("total_amount").alias("amount_mean_usd"),
        F.expr("percentile_approx(duration_min, 0.5, 10000)").alias("duration_median_min"),
        F.expr("percentile_approx(total_amount, 0.5, 10000)").alias("amount_median_usd")
    ).first().asDict()
    aggregations = {
        "por_hora": clean.withColumn("hora", F.hour("tpep_pickup_datetime"))
            .groupBy("hora").agg(F.count("*").alias("viajes"),
                F.avg("duration_min").alias("duracion_media_min"),
                F.avg("total_amount").alias("importe_medio_usd")).orderBy("hora"),
        "por_dia": clean.withColumn("fecha", F.to_date("tpep_pickup_datetime"))
            .groupBy("fecha").agg(F.count("*").alias("viajes")).orderBy("fecha"),
        "zonas_top": clean.groupBy("PULocationID").agg(F.count("*").alias("viajes"))
            .orderBy(F.desc("viajes"), "PULocationID").limit(10),
        "pagos": clean.groupBy("payment_type").count().orderBy("payment_type")
    }
    results = {}
    for name, df in aggregations.items():
        # Solo se trasladan al driver agregados pequeños (24/31/10 filas).
        rows = [r.asDict() for r in df.collect()]
        results[name] = rows
        with (out / (name + ".csv")).open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader(); writer.writerows(rows)
        df.show(35, truncate=False)
    # Resumen descriptivo compacto; nunca se colectan viajes individuales.
    results["descriptivos"] = [r.asDict() for r in clean.select(
        "trip_distance", "duration_min", "total_amount").summary().collect()]
    results.update({"source_url": URL, "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "spark_version": spark.version, "python_version": platform.python_version(),
        "master": spark.sparkContext.master, "input_columns": len(raw.columns),
        "input_schema": raw.schema.jsonValue(), "nulls": nulls,
        "rows_raw": n_raw, "rows_unique": n_unique, "rows_dated_complete": n_dated,
        "rows_clean": n_clean, "duplicates_removed": n_raw-n_unique,
        "missing_or_outside_month": n_unique-n_dated,
        "outside_analysis_bounds": n_dated-n_clean, "overview": overview,
        "elapsed_seconds": round(time.time()-start, 2)})
    (out / "resultados.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print("RESUMEN", json.dumps({k:v for k,v in results.items()
        if k.startswith("rows_") or k in ["overview","elapsed_seconds"]}, default=str))
    clean.unpersist(); unique.unpersist(); spark.stop()


if __name__ == "__main__":
    main()
