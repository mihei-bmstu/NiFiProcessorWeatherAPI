package com.example.processors.weatherAPI

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.util.StandardValidators

trait WeatherApiProcessorProperties {
  lazy val properties: List[PropertyDescriptor] = List(LocationProperty)
  val LocationProperty: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name(WeatherApiProcessor.useDefaultLocationPropertyName)
    .displayName("Use default location")
    .description("Use this option to send default location (moscow,rus) for testing purposes")
    .required(true)
    .allowableValues("true", "false")
    .defaultValue("true")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .build
}

object WeatherApiProcessorProperties extends WeatherApiProcessorProperties {

}
