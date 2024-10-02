package dev.vgerasimov.pipes.registrar;

import static com.google.common.base.Preconditions.checkState;

import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteOptions;
import dev.vgerasimov.pipes.CustomStep;
import dev.vgerasimov.pipes.idsmapping.JdbcTemplateIdMappingRepository;
import dev.vgerasimov.pipes.registrar.models.BeanNameFormatter;
import dev.vgerasimov.pipes.registrar.models.Context;
import dev.vgerasimov.pipes.registrar.models.Named;
import dev.vgerasimov.pipes.registrar.models.WithDependencies;
import dev.vgerasimov.pipes.registrar.properties.Schedule;
import dev.vgerasimov.pipes.registrar.properties.StepProperties;
import dev.vgerasimov.pipes.steps.EchoStep;
import dev.vgerasimov.pipes.steps.IdsLookupStep;
import dev.vgerasimov.pipes.steps.IdsSaveStep;
import dev.vgerasimov.pipes.steps.InfluxSinkStep;
import dev.vgerasimov.pipes.steps.KafkaSinkStep;
import dev.vgerasimov.pipes.steps.KafkaSourceStep;
import dev.vgerasimov.pipes.steps.ReadJsonFilesStep;
import dev.vgerasimov.pipes.steps.ReadMultipartJsonFilesStep;
import dev.vgerasimov.pipes.steps.RestPollerStep;
import dev.vgerasimov.pipes.steps.RestSinkStep;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

@Slf4j
class StepsRegistrar implements BeanDefinitionRegistryPostProcessor {

  private final List<StepProperties> configurations;

  StepsRegistrar(Environment environment) {
    this.configurations =
        Binder.get(environment)
            .bind("steps", Bindable.listOf(StepProperties.class))
            .orElseThrow(IllegalStateException::new);
    log.debug("Found {} steps configurations", this.configurations.size());
  }

  @Override
  public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry)
      throws BeansException {
    Context ctx = new Context();
    HashSet<String> stepNames = new HashSet<>();
    for (StepProperties configuration : configurations) {
      if (stepNames.contains(configuration.name())) {
        throw new IllegalStateException(
            String.format("Step with name %s already exists", configuration.name()));
      }
      stepNames.add(configuration.name());
      var formatName = new BeanNameFormatter(configuration.name());
      for (var beanDefinition : createStepDefinitions(formatName, configuration, ctx)) {
        registry.registerBeanDefinition(beanDefinition.name(), beanDefinition.value());
        log.debug("Bean {} is registered", beanDefinition.name());
      }
    }
  }

  private WithDependencies createStepDefinitions(
      BeanNameFormatter beanNameFormatter, StepProperties properties, Context ctx) {
    var type = properties.type();
    return switch (type) {
      case "custom" -> custom(properties);
      case "source/kafka" -> sourceKafka(properties);
      case "source/rest" -> sourceRest(beanNameFormatter, properties, ctx);
      case "sink/kafka" -> sinkKafka(properties, ctx);
      case "sink/rest" -> sinkRest(beanNameFormatter, properties, ctx);
      case "echo" -> simple(EchoStep.class, properties.name());
      case "read/json-file" -> simple(ReadJsonFilesStep.class, properties.name());
      case "read/multipart-json-file" ->
          simple(ReadMultipartJsonFilesStep.class, properties.name());
      case "ids/lookup" -> idsLookup(beanNameFormatter, properties, ctx);
      case "ids/save" -> idsSave(beanNameFormatter, properties, ctx);
      case "influx/sink" -> influxSink(beanNameFormatter, properties);
      default -> throw new IllegalArgumentException(String.format("Invalid step type: %s", type));
    };
  }

  private static Named<BeanDefinition> createWebClientDefinitions(
      BeanNameFormatter beanNameFormatter, String url, Context ctx) {
    var clazz = WebClient.class;
    var beanDefinition = new GenericBeanDefinition();
    beanDefinition.setBeanClass(clazz);
    beanDefinition.setInstanceSupplier(
        () ->
            WebClient.builder()
                .baseUrl(url)
                .clientConnector(
                    new ReactorClientHttpConnector(
                        HttpClient.create().responseTimeout(Duration.ofMinutes(5))))
                .defaultHeaders(
                    headers -> {
                      headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
                      headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
                    })
                .build());
    return new Named<>(beanNameFormatter.apply("webClient"), clazz, beanDefinition);
  }

  private static WithDependencies influxSink(
      BeanNameFormatter beanNameFormatter, StepProperties properties) {
    var clazz = InfluxSinkStep.class;
    var name = properties.name();
    var influxDB = createInfluxDBDefinitions(beanNameFormatter, properties);
    var writeOptionsBuilder = WriteOptions.builder();
    if (properties.influxBatchSize() != null) {
      writeOptionsBuilder.batchSize(properties.influxBatchSize());
    }
    if (properties.influxBufferLimit() != null) {
      writeOptionsBuilder.bufferLimit(properties.influxBufferLimit());
    }
    return new WithDependencies(
        List.of(influxDB),
        new Named<>(
            name,
            clazz,
            BeanDefinitionBuilder.genericBeanDefinition(clazz)
                .addConstructorArgValue(name)
                .addConstructorArgReference(influxDB.name())
                .addConstructorArgValue(writeOptionsBuilder.build())
                .getBeanDefinition()));
  }

  private static Named<BeanDefinition> createInfluxDBDefinitions(
      BeanNameFormatter beanNameFormatter, StepProperties properties) {
    var influxUrl = properties.influxUrl();
    var influxToken = properties.influxToken();
    var influxOrg = properties.influxOrg();
    var influxBucket = properties.influxBucket();
    checkState(influxUrl != null, "InfluxDBClient: InfluxDB url must be set");
    checkState(influxToken != null, "InfluxDBClient: InfluxDB token must be set");
    checkState(influxOrg != null, "InfluxDBClient: InfluxDB org must be set");
    checkState(influxBucket != null, "InfluxDBClient: InfluxDB bucket must be set");
    var clazz = InfluxDBClient.class;
    var name = beanNameFormatter.apply("influxDB");
    var beanDefinition = new GenericBeanDefinition();
    beanDefinition.setBeanClass(clazz);
    beanDefinition.setInstanceSupplier(
        () -> {
          return InfluxDBClientFactory.create(
              InfluxDBClientOptions.builder()
                  .url(influxUrl)
                  .authenticateToken(influxToken.toCharArray())
                  .org(influxOrg)
                  .bucket(influxBucket)
                  .logLevel(LogLevel.BASIC)
                  .okHttpClient(
                      new OkHttpClient.Builder()
                          .connectTimeout(Duration.ofSeconds(30))
                          .writeTimeout(Duration.ofMinutes(60))
                          .readTimeout(Duration.ofMinutes(60))
                          .retryOnConnectionFailure(true))
                  .build());
        });
    return new Named<>(name, clazz, beanDefinition);
  }

  private static WithDependencies custom(StepProperties properties) {
    checkState(properties.className() != null, "CustomStep: class name must be set");
    var className = properties.className();
    var name = properties.name();
    Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(String.format("Class %s not found", className), e);
    }
    if (!CustomStep.class.isAssignableFrom(clazz)) {
      throw new IllegalStateException(String.format("Class %s is not a CustomStep", className));
    }
    Constructor<?> constructor;
    try {
      constructor = clazz.getDeclaredConstructor(String.class, String.class);
      constructor.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          String.format(
              "Class %s doesn't have constructor with String and Map arguments", className),
          e);
    }
    var beanDefinition = new GenericBeanDefinition();
    beanDefinition.setBeanClass(clazz);
    beanDefinition.setInstanceSupplier(
        () -> {
          try {
            return constructor.newInstance(name, properties.custom());
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(
                String.format("Cannot instantiate class %s", className), e);
          }
        });
    return new WithDependencies(List.of(), new Named<>(name, clazz, beanDefinition));
  }

  private static WithDependencies sinkRest(
      BeanNameFormatter beanNameFormatter, StepProperties properties, Context ctx) {
    checkState(properties.restUrl() != null, "RestSinkStep: REST url must be set");
    var clazz = RestSinkStep.class;
    var name = properties.name();
    var webClient = createWebClientDefinitions(beanNameFormatter, properties.restUrl(), ctx);
    return new WithDependencies(
        List.of(webClient),
        new Named<>(
            name,
            clazz,
            BeanDefinitionBuilder.genericBeanDefinition(clazz)
                .addConstructorArgValue(name)
                .addConstructorArgReference(webClient.name())
                .getBeanDefinition()));
  }

  private static WithDependencies sinkKafka(StepProperties properties, Context ctx) {
    checkState(
        properties.kafkaBootstrapServers() != null,
        "KafkaSinkStep: Kafka bootstrap servers must be set");
    checkState(properties.kafkaTopic() != null, "KafkaSinkStep: Kafka topic must be set");
    var clazz = KafkaSinkStep.class;
    var name = properties.name();
    var kafkaTemplate = createKafkaTemplateDefinition(properties.kafkaBootstrapServers(), ctx);
    return WithDependencies.of(
        Stream.of(kafkaTemplate).filter(Optional::isPresent).map(Optional::get).toList(),
        new Named<>(
            name,
            clazz,
            BeanDefinitionBuilder.genericBeanDefinition(clazz)
                .addConstructorArgValue(name)
                .addConstructorArgReference("kafkaTemplate")
                .addConstructorArgValue(properties.kafkaTopic())
                .getBeanDefinition()));
  }

  private static WithDependencies idsLookup(
      BeanNameFormatter beanNameFormatter, StepProperties properties, Context ctx) {
    checkState(
        properties.sourceIdType() != null, "IdMappingLookupStep: Source id type must be set");
    var clazz = IdsLookupStep.class;
    var name = properties.name();
    var repository = createIdMappingRepositoryDefinitions(beanNameFormatter, ctx);
    return WithDependencies.of(
        List.of(repository),
        new Named<>(
            name,
            clazz,
            BeanDefinitionBuilder.genericBeanDefinition(clazz)
                .addConstructorArgValue(name)
                .addConstructorArgReference(repository.beanDefinition().name())
                .addConstructorArgValue(properties.sourceIdType())
                .getBeanDefinition()));
  }

  private static WithDependencies idsSave(
      BeanNameFormatter beanNameFormatter, StepProperties properties, Context ctx) {
    var clazz = IdsSaveStep.class;
    var name = properties.name();
    var repository = createIdMappingRepositoryDefinitions(beanNameFormatter, ctx);
    return WithDependencies.of(
        List.of(repository),
        new Named<>(
            name,
            clazz,
            BeanDefinitionBuilder.genericBeanDefinition(clazz)
                .addConstructorArgValue(name)
                .addConstructorArgReference(repository.beanDefinition().name())
                .getBeanDefinition()));
  }

  private static WithDependencies createIdMappingRepositoryDefinitions(
      BeanNameFormatter beanNameFormatter, Context ctx) {
    var clazz = JdbcTemplateIdMappingRepository.class;
    return WithDependencies.of(
        List.of(),
        new Named<>(
            beanNameFormatter.apply("idMappingRepository"),
            clazz,
            BeanDefinitionBuilder.genericBeanDefinition(clazz)
                .addConstructorArgReference("namedParameterJdbcTemplate")
                .getBeanDefinition()));
  }

  private static WithDependencies simple(Class<?> clazz, String name) {
    return WithDependencies.of(
        List.of(),
        new Named<>(
            name,
            clazz,
            BeanDefinitionBuilder.genericBeanDefinition(clazz)
                .addConstructorArgValue(name)
                .getBeanDefinition()));
  }

  private static WithDependencies sourceRest(
      BeanNameFormatter beanNameFormatter, StepProperties properties, Context ctx) {
    checkState(properties.restUrl() != null, "RestPollerStep: REST url must be set");
    checkState(properties.restSchedule() != null, "RestPollerStep: REST schedule must be set");
    var clazz = RestPollerStep.class;
    var name = properties.name();
    var webClient = createWebClientDefinitions(beanNameFormatter, properties.restUrl(), ctx);
    var restSchedule = properties.restSchedule();
    var source =
        BeanDefinitionBuilder.genericBeanDefinition(clazz)
            .addConstructorArgValue(name)
            .addConstructorArgReference(webClient.name())
            .addConstructorArgValue(
                switch (restSchedule.type()) {
                  case "interval", "INTERVAL" ->
                      new Schedule.Interval(restSchedule.interval(), restSchedule.window());
                  default ->
                      throw new IllegalStateException(
                          String.format("Unsupported rest schedule type: %s", restSchedule.type()));
                })
            .addConstructorArgReference("clock")
            .addConstructorArgReference("dateTimesService");
    return new WithDependencies(
        List.of(webClient), new Named<>(name, clazz, source.getBeanDefinition()));
  }

  private static WithDependencies sourceKafka(StepProperties properties) {
    checkState(
        properties.kafkaBootstrapServers() != null,
        "KafkaListenerStep: Kafka bootstrap servers must be set");
    checkState(properties.kafkaTopic() != null, "KafkaListenerStep: Kafka topic must be set");
    var clazz = KafkaSourceStep.class;
    var name = properties.name();
    return new WithDependencies(
        List.of(),
        new Named<>(
            name,
            clazz,
            BeanDefinitionBuilder.genericBeanDefinition(clazz)
                .addConstructorArgValue(name)
                .addConstructorArgValue(properties.kafkaBootstrapServers())
                .addConstructorArgValue(properties.kafkaTopic())
                .getBeanDefinition()));
  }

  private static Optional<WithDependencies> createKafkaTemplateDefinition(
      String kafkaBootstrapServers, Context ctx) {
    if (ctx.isKafkaTemplateRegistered()) return Optional.empty();

    var producerFactory = new GenericBeanDefinition();
    producerFactory.setBeanClass(ProducerFactory.class);
    producerFactory.setInstanceSupplier(
        () ->
            new DefaultKafkaProducerFactory<String, Map<String, Object>>(
                createKafkaConfigs(kafkaBootstrapServers)));
    var producerFactoryName = "kafkaProducerFactory";

    var kafkaTemplate =
        BeanDefinitionBuilder.genericBeanDefinition(KafkaTemplate.class)
            .addConstructorArgReference(producerFactoryName);
    var result =
        new WithDependencies(
            List.of(new Named<>(producerFactoryName, ProducerFactory.class, producerFactory)),
            new Named<>("kafkaTemplate", KafkaTemplate.class, kafkaTemplate.getBeanDefinition()));
    ctx.setKafkaTemplateRegistered(true);

    return Optional.of(result);
  }

  private static Map<String, Object> createKafkaConfigs(String kafkaBootstrapServers) {
    return Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        kafkaBootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        JsonSerializer.class);
  }
}
