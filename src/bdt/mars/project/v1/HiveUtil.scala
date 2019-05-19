var mars = sqlContext.sql("SELECT voteFor,COUNT(*) FROM election GROUP BY voteFor")
var mars = sqlContext.sql("SELECT COUNT(*) FROM election")
mars.show()