# Databricks notebook source
sc <- sparkR.session()
books <- as.data.frame(read.df(source = 'csv', schema = NULL, path = '/FileStore/tables/5c89o5011491261805559/BX_Book_Ratings-05a61.csv', header = "true"))
books$Rating <- as.integer(books$Rating)
books$User <- as.integer(books$User)
books$User <- as.integer(books$User)
#sdf <- createDataFrame(sc, books)

# COMMAND ----------

model <- SparkR::spark.als(books, maxIter = 5, regParam = 0.01, userCol = "User",
                   itemCol = "ISBN", ratingCol = "Rating")

# Model summary
summary(model)

# Prediction
predictions <- predict(model, test)
showDF(predictions)

# COMMAND ----------

library(SparkR)

# COMMAND ----------

sdf

# COMMAND ----------



# COMMAND ----------


