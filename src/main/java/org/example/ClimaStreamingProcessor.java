package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.VoidFunction2;


public class ClimaStreamingProcessor {
    public static void main(String[] args) {
        // 1. Configuración de emulación y rutas nativas
        System.setProperty("hadoop.home.dir", "C:\\spark");
        System.setProperty("java.library.path", "C:\\spark\\bin");
        System.setProperty("HADOOP_USER_NAME", "root");

        // 2. Inicializar la Sesión de Spark
        SparkSession spark = SparkSession.builder()
                .appName("ElNino-ClimaStreamingProcessor")
                .master("local[*]")
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        System.out.println("Spark inicializado correctamente con librerías dinámicas nativas.");
        System.out.println("Spark inicializado. Conectando al bus de eventos de Kafka...");

        // 3. Esquemas JSON
        StructType climaSchema = new StructType()
                .add("fuente", DataTypes.StringType)
                .add("provincia", DataTypes.StringType)
                .add("latitud", DataTypes.StringType)
                .add("longitud", DataTypes.StringType)
                .add("precipitacion_mm", DataTypes.DoubleType)
                .add("timestamp", DataTypes.LongType);

        StructType mareaSchema = new StructType()
                .add("fuente", DataTypes.StringType)
                .add("provincia", DataTypes.StringType)
                .add("nivel_marea_m", DataTypes.DoubleType)
                .add("estado_marea", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType);

        // 4. Flujo de Lluvia (Stream principal)
        Dataset<Row> climaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "precip-gpm")
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) as json_value")
                .select(functions.from_json(functions.col("json_value"), climaSchema).as("c"))
                .select("c.*")
                .withColumn("fecha_evento", functions.from_unixtime(functions.col("timestamp").divide(1000)).cast("string"));

        System.out.println("Estructura combinada lista. Desplegando en HDFS y Consola...");

        // 5. foreachBatch: En cada micro-lote, leer la marea como BATCH y hacer join
        try {
            climaStream.writeStream()
                    .queryName("riesgo_combinado")
                    .outputMode("append")
                    .option("checkpointLocation", "target/checkpoints/riesgo_v7")
                    .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDF, batchId) -> {
                        if (batchDF.isEmpty()) return;

                        System.out.println("\n========== BATCH #" + batchId + " ==========");

                        // Leer las mareas más recientes de Kafka como lote estático
                        Dataset<Row> mareaStatic;
                        try {
                            mareaStatic = spark.read()
                                    .format("kafka")
                                    .option("kafka.bootstrap.servers", "localhost:9092")
                                    .option("subscribe", "mareas-inocar")
                                    .option("startingOffsets", "earliest")
                                    .option("endingOffsets", "latest")
                                    .load()
                                    .selectExpr("CAST(value AS STRING) as json_value")
                                    .select(functions.from_json(functions.col("json_value"), mareaSchema).as("m"))
                                    .select("m.*");

                            // Tomar solo la última marea por provincia
                            mareaStatic = mareaStatic
                                    .withColumn("rn", functions.row_number().over(
                                            org.apache.spark.sql.expressions.Window
                                                    .partitionBy("provincia")
                                                    .orderBy(functions.col("timestamp").desc())))
                                    .filter("rn = 1")
                                    .drop("rn", "fuente", "timestamp")
                                    .withColumnRenamed("provincia", "marea_provincia");
                        } catch (Exception e) {
                            System.out.println("[WARN] No se pudo leer mareas: " + e.getMessage());
                            mareaStatic = null;
                        }

                        // Join clima + marea
                        Dataset<Row> joined;
                        if (mareaStatic != null && !mareaStatic.isEmpty()) {
                            joined = batchDF.join(mareaStatic,
                                    batchDF.col("provincia").equalTo(mareaStatic.col("marea_provincia")),
                                    "left_outer")
                                    .drop("marea_provincia");
                        } else {
                            joined = batchDF
                                    .withColumn("nivel_marea_m", functions.lit(0.0))
                                    .withColumn("estado_marea", functions.lit("No Aplica"));
                        }

                        // Calcular niveles de alerta y riesgo combinado
                        Dataset<Row> alertas = joined
                                .withColumn("nivel_alerta_lluvia",
                                        functions.when(functions.col("precipitacion_mm").equalTo(0.0), "Sin Novedad")
                                                .when(functions.col("precipitacion_mm").leq(5.0), "Lluvia Ligera")
                                                .when(functions.col("precipitacion_mm").leq(15.0), "Moderada - Precaución")
                                                .otherwise("CRÍTICO - Riesgo de Inundación"))
                                .withColumn("nivel_marea_m",
                                        functions.coalesce(functions.col("nivel_marea_m"), functions.lit(0.0)))
                                .withColumn("estado_marea",
                                        functions.coalesce(functions.col("estado_marea"), functions.lit("No Aplica")))
                                .withColumn("riesgo_combinado",
                                        functions.when(
                                                functions.col("nivel_alerta_lluvia").equalTo("CRÍTICO - Riesgo de Inundación")
                                                        .and(functions.col("estado_marea").equalTo("Pleamar (Alta)")),
                                                "ALERTA ROJA (Inundación Inminente por Represamiento)")
                                                .when(functions.col("nivel_alerta_lluvia").equalTo("CRÍTICO - Riesgo de Inundación"),
                                                        "CRÍTICO (Solo Lluvia)")
                                                .otherwise(functions.col("nivel_alerta_lluvia")))
                                .select("provincia", "latitud", "longitud", "precipitacion_mm",
                                        "nivel_alerta_lluvia", "nivel_marea_m", "estado_marea",
                                        "riesgo_combinado", "fecha_evento");

                        // Mostrar en consola
                        alertas.show(50, false);

                        // Guardar en HDFS
                        try {
                            alertas.write()
                                    .mode("append")
                                    .partitionBy("provincia")
                                    .parquet("hdfs://namenode:9000/user/root/processed/riesgo_combinado");
                            System.out.println("[OK] Batch #" + batchId + " guardado en HDFS (" + alertas.count() + " filas)");
                        } catch (Exception e) {
                            System.err.println("[ERROR HDFS] " + e.getMessage());
                        }
                    })
                    .start()
                    .awaitTermination();

        } catch (Exception e) {
            System.err.println("Error en la ejecución del stream de Spark: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
