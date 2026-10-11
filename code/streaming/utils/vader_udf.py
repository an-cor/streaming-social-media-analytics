from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

_analyzer = SentimentIntensityAnalyzer()


def _score_text(text: str):
    if text is None:
        return 0.0, "neu"
    scores = _analyzer.polarity_scores(text)
    compound = scores.get("compound", 0.0)
    if compound >= 0.05:
        label = "pos"
    elif compound <= -0.05:
        label = "neg"
    else:
        label = "neu"
    return compound, label


def getVaderUDF():
    schema = StructType([
        StructField("compound_score", DoubleType(), nullable=False),
        StructField("label", StringType(), nullable=False),
    ])

    @F.udf(returnType=schema)
    def vader_struct(text):
        c, l = _score_text(text)
        return {"compound_score": c, "label": l}
    return vader_struct

