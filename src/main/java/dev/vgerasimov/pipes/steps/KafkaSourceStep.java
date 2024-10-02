package dev.vgerasimov.pipes.steps;

import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.SourceStep;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

public class KafkaSourceStep extends BaseStep implements SourceStep {

  private final String kafkaBootstrapServers;
  private final String kafkaTopic;

  public KafkaSourceStep(String name, String kafkaBootstrapServers, String kafkaTopic) {
    super(name);
    this.kafkaBootstrapServers = kafkaBootstrapServers;
    this.kafkaTopic = kafkaTopic;
  }

  @Override
  public Flux<Map<String, Object>> apply() {
    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

    ReceiverOptions<String, Map<String, Object>> receiverOptions =
        ReceiverOptions.<String, Map<String, Object>>create(consumerProps)
            .subscription(Collections.singleton(kafkaTopic));

    return KafkaReceiver.create(receiverOptions)
        .receive()
        .map(ConsumerRecord::value)
        .doOnError(t -> log.error("Got error", t))
        .onErrorResume(t -> Mono.empty());
  }
}
