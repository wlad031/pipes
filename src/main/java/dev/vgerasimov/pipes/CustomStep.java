package dev.vgerasimov.pipes;

import javax.annotation.Nullable;

public abstract class CustomStep extends BaseStep {

  protected final String customProperties;

  protected CustomStep(String name, @Nullable String customProperties) {
    super(name);
    this.customProperties = customProperties;
  }
}
