package dev.vgerasimov.pipes;

import dev.vgerasimov.common.time.DateTimesService;
import java.time.Clock;
import java.time.ZoneId;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication(
    exclude = {
      DataSourceAutoConfiguration.class,
    }
)
@EnableWebMvc
public class Main {

  public static void main(String[] args) {
    SpringApplication.run(Main.class, args);
  }

  @Configuration
  static class WebMvcConfiguration implements WebMvcConfigurer {
    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
      long timeout = 5 * 60 * 1000;// for example 5 minutes
      WebMvcConfigurer.super.configureAsyncSupport(configurer);
      configurer.setDefaultTimeout(timeout);
    }
  }

  @Configuration
  static class ClockConfiguration {

    @Bean
    ZoneId zoneId() {
      return ZoneId.systemDefault();
    }

    @Bean
    Clock clock(ZoneId zoneId) {
      return Clock.system(zoneId);
    }

    @Bean
    DateTimesService dateTimesService(ZoneId zoneId) {
      return new DateTimesService(zoneId);
    }
  }
}
