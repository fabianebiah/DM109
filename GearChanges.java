
package com.inatel.demos;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class GearChanges {

  public static void main(String[] args) throws Exception {

  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("flink-demo", 
            new JSONDeserializationSchema(), 
            properties)
    );
 

    stream  .flatMap(new TelemetryJsonParser())
            .keyBy(0)
            .timeWindow(Time.seconds(3))
            .reduce(new GearChangesReducer())
            .flatMap(new GearChangesMapper())
            .map(new GearChangesPrinter())
            .print();

    env.execute();
    }

    // FlatMap Function - Json Parser
    // Recebe do Kafka dados em JSON contendo  Kafka broker e faz o parse do número do carro, marcha e contador.
	
    // Dados do JSON:
    // {"Car": 9, "time": "52.196000", "telemetry": {"Vaz": "1.2Speed70000", "Distance": "4.605865", "LapTime": "0.128001", 
    // "RPM": "591.266113", "Ay": "24.344515", "Gear": "3.000000", "Throttle": "0.000000", 
    // "Steer": "0.207988", "Ax": "-17.551264", "Brake": "0.282736", "Fuel": "1.898847", "Speed": "34.137680"}}

    static class TelemetryJsonParser implements FlatMapFunction<ObjectNode, Tuple3<String, Float, Integer>> {
      @Override
      public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple3<String, Float, Integer>> out) throws Exception {
        String carNumber = "car" + jsonTelemetry.get("Car").asText();
        float gear = jsonTelemetry.get("telemetry").get("Gear").floatValue();
        out.collect(new Tuple3<>(carNumber, gear, 0));
      }
    }

    // Reduce Function
    // Retorna, para cada carro, um contador com a quantidade de troca de marchas
    static class GearChangesReducer implements ReduceFunction<Tuple3<String, Float, Integer>> {
      @Override
      public Tuple3<String, Float, Integer> reduce(Tuple3<String, Float, Integer> value1, Tuple3<String, Float, Integer> value2) {
        Integer gearChanges = value1.f2;

        if (Math.abs(value1.f1 - value2.f1) >= 0.000001) {
          gearChanges = gearChanges + 1;
        }

        return new Tuple3<>(value1.f0, value2.f1, gearChanges);
      }
    }

    // FlatMap Function
    // Recebe os contadores
    static class GearChangesMapper implements FlatMapFunction<Tuple3<String, Float, Integer>, Tuple2<String, Integer>> {
      @Override
      public void flatMap(Tuple3<String, Float, Integer> carInfo, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(new Tuple2<>(carInfo.f0, carInfo.f2));
      }
    }

    // Map Function - Imprime a contagem de mudança de marcha    
    static class GearChangesPrinter implements MapFunction<Tuple2<String, Integer>, String> {
      @Override
      public String map(Tuple2<String, Integer> avgEntry) throws Exception {
        return  String.format(" %s : %d ", avgEntry.f0 , avgEntry.f1) ;
      }
    }

  }
