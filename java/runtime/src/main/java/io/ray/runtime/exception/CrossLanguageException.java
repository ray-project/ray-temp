package io.ray.runtime.exception;

import io.ray.runtime.generated.Common.Language;

public class CrossLanguageException extends RayException {

  private Language language;

  public CrossLanguageException(io.ray.runtime.generated.Common.RayException exception) {
    super(exception.getFormattedExceptionString());
    this.language = exception.getLanguage();
  }

  public Language getLanguage() {
    return this.language;
  }
}
