package dev.vgerasimov.pipes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseStep implements Step {
  private final String name;
  protected final Logger log;

  protected BaseStep(String name) {
    this.name = name;
    this.log = LoggerFactory.getLogger(name);
  }

  @Override
  public String getName() {
    return name;
  }
}
