package org.example;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class MareaProducer {

    // Provincias costeras de Ecuador que sufren efectos de marea
    private static final String[] PROVINCIAS_COSTERAS = {
            "Guayas", "Esmeraldas", "Manabi", "Santa Elena", "El Oro"
    };

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "mareas-inocar";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Random random = new Random();

        System.out.println("Iniciando simulación de datos de marea (INOCAR)...");

        try {
            while (true) {
                for (String provincia : PROVINCIAS_COSTERAS) {
                    // Simular altura de la marea entre 0.0 y 5.0 metros
                    double nivelMarea = 0.5 + (4.5 * random.nextDouble());
                    
                    String estadoMarea;
                    if (nivelMarea >= 3.5) {
                        estadoMarea = "Pleamar (Alta)";
                    } else if (nivelMarea <= 1.5) {
                        estadoMarea = "Bajamar (Baja)";
                    } else {
                        estadoMarea = "Normal";
                    }

                    // Forzar marea alta en Guayas ocasionalmente para probar el escenario combinado
                    if (provincia.equals("Guayas") && random.nextDouble() > 0.5) {
                        nivelMarea = 4.0 + random.nextDouble();
                        estadoMarea = "Pleamar (Alta)";
                    }

                    JsonObject kafkaRecord = new JsonObject();
                    kafkaRecord.addProperty("fuente", "INOCAR_Simulado");
                    kafkaRecord.addProperty("provincia", provincia);
                    kafkaRecord.addProperty("nivel_marea_m", nivelMarea);
                    kafkaRecord.addProperty("estado_marea", estadoMarea);
                    kafkaRecord.addProperty("timestamp", System.currentTimeMillis());

                    String messageValue = kafkaRecord.toString();

                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, provincia, messageValue);
                    producer.send(record);

                    System.out.println("Enviado Marea -> " + messageValue);
                }

                // Pausa de 10 segundos
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            System.err.println("Error en MareaProducer: " + e.getMessage());
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
