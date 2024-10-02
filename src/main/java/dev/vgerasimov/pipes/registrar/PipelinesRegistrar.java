package dev.vgerasimov.pipes.registrar;

import dev.vgerasimov.pipes.PipelineImpl;
import dev.vgerasimov.pipes.registrar.models.Named;
import dev.vgerasimov.pipes.registrar.models.WithDependencies;
import dev.vgerasimov.pipes.registrar.properties.PipelineProperties;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContext;

@Slf4j
class PipelinesRegistrar implements BeanDefinitionRegistryPostProcessor {

  private final ApplicationContext applicationContext;
  private final Collection<PipelineProperties> configurations;

  PipelinesRegistrar(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
    this.configurations =
        Binder.get(applicationContext.getEnvironment())
            .bind("pipelines", Bindable.listOf(PipelineProperties.class))
            .orElseThrow(IllegalStateException::new);
    log.debug("Found {} pipes configurations", this.configurations.size());
  }

  @Override
  public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry)
      throws BeansException {
    for (PipelineProperties configuration : configurations) {
      for (var beanDefinition : createPipelineDefinitions(configuration)) {
        registry.registerBeanDefinition(beanDefinition.name(), beanDefinition.value());
        log.debug("Bean {} is registered", beanDefinition.name());
      }
    }
  }

  private WithDependencies createPipelineDefinitions(
      PipelineProperties properties) {
    var pipelineClass = PipelineImpl.class;
    var pipe =
        BeanDefinitionBuilder.genericBeanDefinition(pipelineClass)
            .addConstructorArgValue(applicationContext)
            .addConstructorArgValue(properties.name())
            .addConstructorArgValue(properties.steps());
    return new WithDependencies(
        List.of(),
        new Named<>(properties.name(),pipelineClass, pipe.getBeanDefinition()));
  }
}
