import SentimentAnalysis.SentimentAnalyzer

class SentimentAnalyzerSpec extends FunSpec with Matchers {

  describe("sentiment analyzer") {

    it("should return POSITIVE when input has positive emotion") {
      val input = "I had a bad day, but tomorrow will be better."
      val sentiment = SentimentAnalyzer.mainSentiment(input)
      println(sentiment)
    }
  }
}