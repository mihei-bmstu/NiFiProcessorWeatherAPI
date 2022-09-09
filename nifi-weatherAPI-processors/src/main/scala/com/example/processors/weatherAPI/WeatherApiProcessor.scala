package com.example.processors.weatherAPI

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.{AbstractProcessor, ProcessContext, ProcessSession, Relationship}
import org.apache.nifi.stream.io.StreamUtils
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, HttpResponse}

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

@Tags(Array("weather", "api"))
@CapabilityDescription(
  "This processor takes as an input a string 'city,country' and returns current weather in the specified location"
)
class WeatherApiProcessor extends AbstractProcessor
  with WeatherApiProcessorProperties {

  val SUCCESS: Relationship = new Relationship.Builder().name(WeatherApiProcessor.successRelationName).build

  val FAILURE: Relationship = new Relationship.Builder().name(WeatherApiProcessor.failureRelationName).build

  var useDefaultLocation: PropertyDescriptor = _

  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = {
    properties.asJava
  }

  /**
   * Build the property descriptor object
   *
   * @param context
   */
  /*override def init(context: ProcessorInitializationContext): Unit = {
    useDefaultLocation = new PropertyDescriptor.Builder()
      .name(WeatherApiProcessor.useDefaultLocationPropertyName)
      .displayName("Use default location")
      .description("Use this option to send default location (moscow,rus) for testing purposes")
      .required(true)
      .allowableValues("true", "false")
      .defaultValue("true")
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
      .build()
  }*/

  override def getRelationships: util.Set[Relationship] = {
    Set(SUCCESS, FAILURE).asJava
  }

  /**
   * We will process incoming flow file through this method.
   *
   * @param processContext
   * @param processSession
   */

  override def onTrigger(processContext: ProcessContext, processSession: ProcessSession): Unit = {
    val flowFile: FlowFile = processSession.get()
    if (flowFile != null) {
      val useDefault: Boolean = processContext
        .getProperty(WeatherApiProcessor.useDefaultLocationPropertyName)
        .getValue
        .toBoolean
        process(flowFile, useDefault, processSession)
    }
  }

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  def sendRequest(request: HttpRequest): Future[String] = {
    val responseFuture: Future[HttpResponse] = Http().singleRequest(request)
    val entityFuture: Future[HttpEntity.Strict] = responseFuture.flatMap(response => response.entity.toStrict(2.seconds))
    entityFuture.map(entity => entity.data.utf8String)
  }

  def getTemp(cityCountry: String = "moscow,rus"): Future[String] = {
    val request: HttpRequest = HttpRequest(
      method = HttpMethods.GET,
      uri = "http://localhost:8080/" + cityCountry
    )
    sendRequest(request)
  }

  private def process(inputFlowFile: FlowFile,
                      useDefaultLocation: Boolean,
                      session: ProcessSession): Unit = {
    val response = if (useDefaultLocation) getTemp() else {
      val flowFileContent = this.readFlowFileContents(inputFlowFile, session)
      getTemp(flowFileContent)
    }
    val outputFlowFiles: FlowFile = session.clone(inputFlowFile)

    session.putAttribute(outputFlowFiles, "request_result", Await.result(response, 5.seconds))
    session.transfer(outputFlowFiles, SUCCESS)
    session.remove(inputFlowFile)
  }

  /**
   * This method reads all the contents of given flow file and converts contents to String
   *
   * @param flowFile flow file from which content will be read
   * @param session  We need process session object to read contents of flow file
   * @return contents of flow file in String format
   */
  private def readFlowFileContents(flowFile: FlowFile, session: ProcessSession): String = {
    val flowFileContentsBuffer = new Array[Byte](flowFile.getSize.toInt)
    session.read(flowFile, new InputStreamCallback {
      override def process(inputStream: InputStream): Unit = {
        StreamUtils.fillBuffer(inputStream, flowFileContentsBuffer, true)
      }
    })
    new String(flowFileContentsBuffer, StandardCharsets.UTF_8)
  }
}
  object WeatherApiProcessor {

    val useDefaultLocationPropertyName: String = "use-default-location"
    val successRelationName: String = "success"
    val failureRelationName: String = "failure"

  }
