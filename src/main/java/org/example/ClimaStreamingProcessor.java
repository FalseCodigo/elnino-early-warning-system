package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


public class ClimaStreamingProcessor {
    public static void main(String[] args) {
        // 1. Configuración de emulación y rutas nativas
        System.setProperty("hadoop.home.dir", "C:\\spark");
        System.setProperty("java.library.path", "C:\\spark\\bin");

        // 2. NUEVO: Falsificar el usuario de Windows para tener permisos de escritura en HDFS
        System.setProperty("HADOOP_USER_NAME", "root");

        // 3. Inicializar la Sesión de Spark en modo Standalone local
        SparkSession spark = SparkSession.builder()
                .appName("ElNino-ClimaStreamingProcessor")
                .master("local[*]")
                // NUEVA LÍNEA: Obliga a Spark a usar localhost para escribir los bloques en el DataNode de Docker
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                .getOrCreate();

        // Configurar logs para reducir ruido en la consola
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Spark inicializado correctamente con librerías dinámicas nativas.");

        // Configurar logs para reducir el ruido en consola
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Spark inicializado. Conectando al bus de eventos de Kafka...");

        // 2. Definir esquemas para JSONs
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

        // 3. Flujo Lluvia
        Dataset<Row> climaDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "precip-gpm")
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) as json_value")
                .select(functions.from_json(functions.col("json_value"), climaSchema).as("c_data"))
                .select("c_data.*")
                .withColumn("fecha_evento_clima", functions.from_unixtime(functions.col("timestamp").divide(1000)).cast(DataTypes.TimestampType))
                .withWatermark("fecha_evento_clima", "1 minute");

        // 4. Flujo Marea
        Dataset<Row> mareaDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "mareas-inocar")
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) as json_value")
                .select(functions.from_json(functions.col("json_value"), mareaSchema).as("m_data"))
                .select("m_data.*")
                .withColumn("fecha_evento_marea", functions.from_unixtime(functions.col("timestamp").divide(1000)).cast(DataTypes.TimestampType))
                .withWatermark("fecha_evento_marea", "1 minute");

        // 5. Join y Riesgo Combinado
        Dataset<Row> joinedDF = climaDF.as("c").join(
                mareaDF.as("m"),
                functions.expr("c.provincia = m.provincia AND " +
                        "m.fecha_evento_marea >= c.fecha_evento_clima - interval 1 minute AND " +
                        "m.fecha_evento_marea <= c.fecha_evento_clima + interval 1 minute"),
                "leftOuter"
        );

        Dataset<Row> alertasDF = joinedDF
                .withColumn("nivel_alerta_lluvia",
                        functions.when(functions.col("c.precipitacion_mm").equalTo(0.0), "Sin Novedad")
                                .when(functions.col("c.precipitacion_mm").leq(5.0), "Lluvia Ligera")
                                .when(functions.col("c.precipitacion_mm").leq(15.0), "Moderada - Precaución")
                                .otherwise("CRÍTICO - Riesgo de Inundación")
                )
                .withColumn("riesgo_combinado",
                        functions.when(
                                functions.col("nivel_alerta_lluvia").equalTo("CRÍTICO - Riesgo de Inundación")
                                .and(functions.col("m.estado_marea").equalTo("Pleamar (Alta)")), 
                                "ALERTA ROJA (Inundación Inminente por Represamiento)"
                        ).when(functions.col("nivel_alerta_lluvia").equalTo("CRÍTICO - Riesgo de Inundación"), "CRÍTICO (Solo Lluvia)")
                         .otherwise(functions.col("nivel_alerta_lluvia"))
                )
                .selectExpr(
                        "c.provincia as provincia",
                        "c.latitud as latitud",
                        "c.longitud as longitud",
                        "c.precipitacion_mm as precipitacion_mm",
                        "nivel_alerta_lluvia",
                        "COALESCE(m.nivel_marea_m, 0.0) as nivel_marea_m",
                        "COALESCE(m.estado_marea, 'No Aplica') as estado_marea",
                        "riesgo_combinado",
                        "CAST(c.fecha_evento_clima AS STRING) as fecha_evento"
                );

        System.out.println("Estructura combinada lista. Desplegando en HDFS y Consola...");

        try {
            alertasDF.writeStream()
                    .outputMode("append")
                    .format("console")
                    .option("truncate", "false")
                    .start();

            alertasDF.writeStream()
                    .outputMode("append")
                    .format("parquet")
                    .option("path", "hdfs://namenode:9000/user/root/processed/riesgo_combinado")
                    .option("checkpointLocation", "hdfs://namenode:9000/user/root/checkpoints/riesgo_combinado")
                    .partitionBy("provincia")
                    .start();

            spark.streams().awaitAnyTermination();

        } catch (Exception e) {
            System.err.println("Error en la ejecución del stream de Spark: " + e.getMessage());
        }
    }
}
