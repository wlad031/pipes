package dev.vgerasimov.pipes.registrar.models;

import java.util.function.UnaryOperator;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BeanNameFormatter implements UnaryOperator<String> {
  private final String stepName;

  public String apply() {
    return stepName;
  }

  @Override
  public String apply(String beanName) {
    return String.format("%s-%s", stepName, beanName);
  }
}
