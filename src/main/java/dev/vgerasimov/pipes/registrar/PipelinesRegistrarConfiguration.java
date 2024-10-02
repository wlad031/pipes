package dev.vgerasimov.pipes.registrar;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.ConfigurableEnvironment;

@Configuration
public class PipelinesRegistrarConfiguration {
  
  @Bean
  @Order(-2000)
  static StepsRegistrar stepsRegistrar(ConfigurableEnvironment environment) {
    return new StepsRegistrar(environment);
  }

  @Bean
  @Order(-1000)
  static PipelinesRegistrar pipelinesRegistrar(ApplicationContext applicationContext) {
    return new PipelinesRegistrar(applicationContext);
  }
}
