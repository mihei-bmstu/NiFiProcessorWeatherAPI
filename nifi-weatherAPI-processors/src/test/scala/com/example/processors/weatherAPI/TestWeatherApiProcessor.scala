package com.example.processors.weatherAPI

import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.scalatest._
import matchers._
import org.scalatest.funsuite._

import java.util
import scala.jdk.CollectionConverters._

class TestWeatherApiProcessor extends AnyFunSuite with should.Matchers {

  val correctInputData: String = "moscow,ru"
  val wrongInputData: String = "msdfdf,fdr"
  val wrongTestRunner: TestRunner = TestRunners.newTestRunner(new WeatherApiProcessor)

  wrongTestRunner.setProperty(WeatherApiProcessor.useDefaultLocationPropertyName, "false")
  wrongTestRunner.enqueue(wrongInputData)
  wrongTestRunner.run()
  val wrongResponseExample = "Please send me City and Country. For example: Moscow,rus"

  test("check number of wrong output flow files") {
    wrongTestRunner.assertAllFlowFilesTransferred(WeatherApiProcessor.successRelationName, 1)
  }

  val listOfWrongOutputFlowFiles: util.List[MockFlowFile] = wrongTestRunner
    .getFlowFilesForRelationship(WeatherApiProcessor.successRelationName)
  val wrongOutputFile: MockFlowFile = listOfWrongOutputFlowFiles.get(0)

  test("check if wrong 'request_result' attribute exists") {
    wrongOutputFile.assertAttributeExists("request_result")
  }

  test("check output of wrong input data") {
    val attribute = wrongOutputFile.getAttribute("request_result")
    assert(attribute == wrongResponseExample)
  }

  val testRunner: TestRunner = TestRunners.newTestRunner(new WeatherApiProcessor)

  testRunner.setProperty(WeatherApiProcessor.useDefaultLocationPropertyName, "true")
  testRunner.enqueue(correctInputData)
  testRunner.run()

  test("check number of correct output flow files") {
    testRunner.assertAllFlowFilesTransferred(WeatherApiProcessor.successRelationName, 1)
  }

  val listOfCorrectOutputFlowFiles: util.List[MockFlowFile] = testRunner
    .getFlowFilesForRelationship(WeatherApiProcessor.successRelationName)
  val correctOutputFile: MockFlowFile = listOfCorrectOutputFlowFiles.get(0)

  test("check if correct 'request_result' attribute exists") {
    correctOutputFile.assertAttributeExists("request_result")
  }

  test("check output of correct input data") {
    val attribute = correctOutputFile.getAttribute("request_result")
    assert(attribute != wrongResponseExample)
  }

  /*test("check if defined property exists") {
    correctOutputFile.assert
  }*/

}