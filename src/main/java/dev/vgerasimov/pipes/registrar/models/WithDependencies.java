package dev.vgerasimov.pipes.registrar.models;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.springframework.beans.factory.config.BeanDefinition;

public record WithDependencies(
    List<Named<BeanDefinition>> dependencies, Named<BeanDefinition> beanDefinition)
    implements Iterable<Named<BeanDefinition>> {

  public static WithDependencies of(
      List<WithDependencies> dependencies, Named<BeanDefinition> beanDefinition) {
    var deps = new ArrayList<Named<BeanDefinition>>();
    for (WithDependencies dependency : dependencies) {
      for (Named<BeanDefinition> beanDefinitionNamed : dependency) {
        deps.add(beanDefinitionNamed);
      }
    }
    return new WithDependencies(deps, beanDefinition);
  }

  @Override
  public Iterator<Named<BeanDefinition>> iterator() {
    return new Iterator<>() {
      private final Iterator<Named<BeanDefinition>> deps = dependencies.iterator();
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        return deps.hasNext() || !finished;
      }

      @Override
      public Named<BeanDefinition> next() {
        if (deps.hasNext()) {
          return deps.next();
        } else if (!finished) {
          finished = true;
          return beanDefinition;
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }
}
