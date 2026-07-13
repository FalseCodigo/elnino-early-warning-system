package org.example;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;

public class MeteorologiaProducer {

    private static final String[][] PROVINCIAS = {
            {"Azuay", "-3.1000", "-79.2000"},
            {"Bolivar", "-1.6000", "-79.0000"},
            {"Cañar", "-2.4000", "-78.9000"},
            {"Carchi", "0.7000", "-78.0000"},
            {"Chimborazo", "-1.9000", "-78.7000"},
            {"Cotopaxi", "-0.9000", "-78.8000"},
            {"El Oro", "-3.6000", "-79.8000"},
            {"Esmeraldas", "0.8000", "-79.4000"},
            {"Galapagos", "-0.5000", "-90.5000"},
            {"Guayas", "-2.2000", "-80.0000"},
            {"Imbabura", "0.3500", "-78.3000"},
            {"Loja", "-4.1000", "-79.6000"},
            {"Los Rios", "-1.5000", "-79.5000"},
            {"Manabi", "-0.6000", "-80.1000"},
            {"Morona Santiago", "-2.6000", "-77.9000"},
            {"Napo", "-0.8000", "-77.6000"},
            {"Orellana", "-0.9000", "-76.6000"},
            {"Pastaza", "-1.8000", "-76.5000"},
            {"Pichincha", "-0.1000", "-78.7000"},
            {"Santa Elena", "-2.2000", "-80.6000"},
            {"Santo Domingo", "-0.2500", "-79.2000"},
            {"Sucumbios", "-0.1000", "-76.5000"},
            {"Tungurahua", "-1.3000", "-78.6000"},
            {"Zamora Chinchipe", "-4.2000", "-78.8000"}
    };

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "precip-gpm";

        // 1. Configurar Productor Kafka
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Cliente HTTP para consultar la web
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        System.out.println("Iniciando extracción de datos meteorológicos REALES de Ecuador...");

        try {
            while (true) {
                // Recorremos cada provincia del Ecuador
                for (String[] prov : PROVINCIAS) {
                    String provincia = prov[0];
                    String lat = prov[1];
                    String lon = prov[2];

                    // 2. Consulta a la API de modelos climáticos para extraer la precipitación actual
                    String apiUrl = String.format("https://api.open-meteo.com/v1/forecast?latitude=%s&longitude=%s&current=precipitation", lat, lon);

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(apiUrl))
                            .GET()
                            .build();

                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() == 200) {
                        // 3. Extraer solo el dato que nos importa (La lluvia actual) usando Gson
                        JsonObject jsonResponse = JsonParser.parseString(response.body()).getAsJsonObject();
                        JsonObject current = jsonResponse.getAsJsonObject("current");
                        double precipitacionReal = current.get("precipitation").getAsDouble();

                        // 4. Empaquetar la información en el JSON para Kafka
                        JsonObject kafkaRecord = new JsonObject();
                        kafkaRecord.addProperty("fuente", "Modelo_Global_Satelital");
                        kafkaRecord.addProperty("provincia", provincia);
                        kafkaRecord.addProperty("latitud", lat);
                        kafkaRecord.addProperty("longitud", lon);
                        kafkaRecord.addProperty("precipitacion_mm", precipitacionReal);
                        kafkaRecord.addProperty("timestamp", System.currentTimeMillis());

                        String messageValue = kafkaRecord.toString();

                        // 5. Enviar al clúster de Kafka
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, provincia, messageValue);
                        producer.send(record);

                        System.out.println("Enviado -> " + messageValue);
                    } else {
                        System.err.println("Error al consultar API para " + provincia);
                    }

                    // Pausa de 5 segundos para no saturar el servidor de la API web
                    Thread.sleep(5000);
                }

                System.out.println("\n--- Mapeo nacional completado. Esperando 1 minuto para actualizar... ---\n");
                // Pausamos el programa 60 segundos antes de volver a consultar el clima
                Thread.sleep(60000);
            }
        } catch (Exception e) {
            System.err.println("Error crítico en el pipeline: " + e.getMessage());
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
