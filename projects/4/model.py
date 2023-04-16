from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, VectorAssembler

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable

class RemoveNonLatin(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
  input_col = Param(Params._dummy(), "input_col", "input column name.", typeConverter=TypeConverters.toString)
  output_col = Param(Params._dummy(), "output_col", "output column name.", typeConverter=TypeConverters.toString)
  
  @keyword_only
  def __init__(self, input_col, output_col):
    super().__init__()
    self._setDefault(input_col=None, output_col=None)
    kwargs = self._input_kwargs
    self.set_params(**kwargs)
    
  @keyword_only
  def set_params(self, input_col, output_col):
    kwargs = self._input_kwargs
    self._set(**kwargs)
  
  def _transform(self, df):
    input_col = self.getOrDefault(self.input_col)
    output_col = self.getOrDefault(self.output_col)
    return df.withColumn(output_col, f.regexp_replace(input_col, "[^a-zA-Z ]", ' '))


# remove all characters that are not a latin letters
rem_non_latin_rev = RemoveNonLatin(input_col='reviewText', output_col='review_text')
rem_non_latin_sum = RemoveNonLatin(input_col='summary', output_col='summary_')

# split by whitespace
review_tokenizer = Tokenizer(inputCol='review_text', outputCol='reviewTokens')
summary_tokenizer = Tokenizer(inputCol='summary_', outputCol='summaryTokens')

# remove english stopwords
stop_words = StopWordsRemover.loadDefaultStopWords("english")
stop_words_rem = StopWordsRemover(
    inputCols=['reviewTokens', 'summaryTokens'],
    outputCols=['reviewTokensClean', 'summaryTokensClean'],
    stopWords=stop_words
)

# vectirize using hash trick
review_vectorizer = HashingTF(
    numFeatures=100, inputCol='reviewTokensClean', outputCol='reviewVec'
)
summary_vectorizer = HashingTF(
    numFeatures=30, inputCol='summaryTokensClean', outputCol='summaryVec'
)

# assemble into one column
asm = VectorAssembler(inputCols=['reviewVec', 'summaryVec', 'vote', 'verified'], outputCol='features')

# simple regression model
reg = LinearRegression(featuresCol='features', labelCol='overall', predictionCol='prediction')

# the goal of this file
pipeline = Pipeline(stages=[
    rem_non_latin_rev,
    rem_non_latin_sum,
    review_tokenizer,
    summary_tokenizer,
    stop_words_rem,
    review_vectorizer,
    summary_vectorizer,
    asm,
    reg
])
