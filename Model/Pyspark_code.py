import conf
codes = {}

codes['load_crm']="""
CRMDF = spark.read.parquet("""+'"'+conf.canfra_hdfs_crm+'"'+""")
CRMDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqMer")
CRMDF.createOrReplaceTempView("CRMView")
CRMDFMer=spark.sql("SELECT MerchantCode as merchant_number, GuildCode as senf_code, GuildName as senf_name, CityName as city_name  FROM CRMView")
CRMDFMer_dataframe = CRMDFMer.toPandas()
CRMDFMer_dataframe.to_json()
"""

codes['load_crm1']="""
CRMDF = spark.read.parquet("""+'"'+conf.canfra_hdfs_crm+'"'+""")
CRMDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqMer")
CRMDF.createOrReplaceTempView("CRMView")
CRMDFMer=spark.sql("SELECT MerchantCode as merchant_number, GuildCode as senf_code, GuildName as senf_name, CityName as city_name  FROM CRMView")
pandas_df=CRMDFMer.toJSON().collect()
"""

codes ['3dclustring'] ="""
spark.conf.set("spark.sql.shuffle.partitions",5)
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.driver.memory","10g")
from pyspark.sql import Row
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as sf    # for sum agg
pqDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqTotal")
Sp_Df = pqDF.select("Merchantnumber","Amount",((((pqDF.FinancialDate.substr(4,2).cast('int'))+(12*((pqDF.FinancialDate.substr(1,2).cast('int'))-94)))- {0})*30+(pqDF.FinancialDate.substr(7,2).cast('int'))).alias('dayNum')).filter("ProccessCode='000000' AND MessageType='200'AND SuccessOrFailure='S'AND FinancialDate between {1} and {2}")
Sp_Df= Sp_Df.groupBy(Sp_Df.Merchantnumber,Sp_Df.dayNum).agg(sf.sum(Sp_Df.Amount).alias("TxnSum"),sf.count(Sp_Df.Amount).alias("TxnNo"))
Sp_Df = Sp_Df.groupBy(Sp_Df.Merchantnumber).agg(sf.sum(Sp_Df.TxnNo).alias("all_transactions"),sf.sum(Sp_Df.TxnSum).alias("sum_amounts"),sf.sum((Sp_Df.TxnNo)/Sp_Df.dayNum).alias("harmonic"))
Sp_rdd = Sp_Df.rdd
Sp_Piperdd = Sp_rdd.map(lambda x:(x[1],x[2],x[3]))
model = KMeans.train(Sp_Piperdd, {3})
labels =model.predict(Sp_Piperdd)
labels = labels.map(Row("label")).toDF()   #add schema
df11 =  Sp_Df.withColumn("id", monotonically_increasing_id())
df22 =  labels.withColumn("id", monotonically_increasing_id())
newDF = df11.join(df22, df11.id == df22.id, 'inner').drop(df22.id)
pandas_df=newDF.toPandas()
pandas_df.to_json()
"""

codes ['no_transaction_vs_amount'] ="""
from pyspark.sql.types import *
pqDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqData")
pqDF.createOrReplaceTempView("KIView")
KIDDMMDF=spark.sql("SELECT Merchantnumber,SUBSTRING(FinancialDate,1,2) as yy, SUBSTRING(FinancialDate,4,2) as mm, SUBSTRING(FinancialDate,7,2) as dd,Amount FROM KIView WHERE ProcessCode='000000' and MessageType='200' and SuccessofFailure='S' and FinancialDate between {0} and {1}")
KIDDMMDF.createOrReplaceTempView("KIDDMMView")
KIDayNumDF=spark.sql("SELECT Merchantnumber,Amount,((cast( mm as int)+(12*(cast(yy as int) - 94)))- {2} )*30 + (cast(dd as int)) as dayNum FROM KIDDMMView")
KIDayNumDF.createOrReplaceTempView("KIDayNumView")
KIDayNumGrDF=spark.sql("SELECT Merchantnumber,dayNum,Count(*) as TxnNo,Sum(Amount) as TxnSum FROM KIDayNumView group by Merchantnumber,dayNum")
KIDayNumGrDF.createOrReplaceTempView("KIDayNumGrView")
KIRFMDF=spark.sql("SELECT Merchantnumber as merchant_number, Sum(TxnNo) as all_transactions, Sum(TxnSum) as sum_amounts, Sum(1/cast(dayNum as float)*cast(TxnNo as float)) as harmonic FROM KIDayNumGrView group by Merchantnumber")
kirfmdf_dataframe = KIRFMDF.toPandas()
kirfmdf_dataframe.to_json()
"""

codes ['boxplot_amount'] ="""
from pyspark.sql.types import *
pqDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqData")
pqDF.createOrReplaceTempView("KIView")
KIDDMMDF=spark.sql("SELECT Merchantnumber,SUBSTRING(FinancialDate,1,2) as yy, SUBSTRING(FinancialDate,4,2) as mm, SUBSTRING(FinancialDate,7,2) as dd,Amount FROM KIView WHERE ProcessCode='000000' and MessageType='200' and SuccessofFailure='S' and FinancialDate between {0} and {1}")
KIDDMMDF.createOrReplaceTempView("KIDDMMView")
KIDayNumDF=spark.sql("SELECT Merchantnumber,Amount,((cast( mm as int)+(12*(cast(yy as int) - 94)))- {2} )*30 + (cast(dd as int)) as dayNum FROM KIDDMMView")
KIDayNumDF.createOrReplaceTempView("KIDayNumView")
KIDayNumGrDF=spark.sql("SELECT Merchantnumber,dayNum,Count(*) as TxnNo,Sum(Amount) as TxnSum FROM KIDayNumView group by Merchantnumber,dayNum")
KIDayNumGrDF.createOrReplaceTempView("KIDayNumGrView")
KIRFMDF=spark.sql("SELECT Merchantnumber as merchant_number, Sum(TxnNo) as all_transactions, Sum(TxnSum) as sum_amounts, Sum(1/cast(dayNum as float)*cast(TxnNo as float)) as harmonic FROM KIDayNumGrView group by Merchantnumber")
kirfmdf_dataframe = KIRFMDF.toPandas()
kirfmdf_dataframe.to_json()
"""

codes ['no_transaction_vs_harmonic'] ="""
from pyspark.sql.types import *
pqDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqData")
pqDF.createOrReplaceTempView("KIView")
KIDDMMDF=spark.sql("SELECT Merchantnumber,SUBSTRING(FinancialDate,1,2) as yy, SUBSTRING(FinancialDate,4,2) as mm, SUBSTRING(FinancialDate,7,2) as dd,Amount FROM KIView WHERE ProcessCode='000000' and MessageType='200' and SuccessofFailure='S' and FinancialDate between {0} and {1}")
KIDDMMDF.createOrReplaceTempView("KIDDMMView")
KIDayNumDF=spark.sql("SELECT Merchantnumber,Amount,((cast( mm as int)+(12*(cast(yy as int) - 94)))- {2} )*30 + (cast(dd as int)) as dayNum FROM KIDDMMView")
KIDayNumDF.createOrReplaceTempView("KIDayNumView")
KIDayNumGrDF=spark.sql("SELECT Merchantnumber,dayNum,Count(*) as TxnNo,Sum(Amount) as TxnSum FROM KIDayNumView group by Merchantnumber,dayNum")
KIDayNumGrDF.createOrReplaceTempView("KIDayNumGrView")
KIRFMDF=spark.sql("SELECT Merchantnumber as merchant_number, Sum(TxnNo) as all_transactions, Sum(TxnSum) as sum_amounts, Sum(1/cast(dayNum as float)*cast(TxnNo as float)) as harmonic FROM KIDayNumGrView group by Merchantnumber")
kirfmdf_dataframe = KIRFMDF.toPandas()
kirfmdf_dataframe.to_json()
"""

codes ['guilds_heatmap'] ="""
spark.conf.set("spark.sql.shuffle.partitions",5)
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.driver.memory","10g")
from pyspark.sql import Row
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as sf    # for sum agg
pqDF = spark.read.parquet("hdfs://10.100.136.40:9000/user/hduser/pqTotal")
Sp_Df = pqDF.select("Merchantnumber","Amount",((((pqDF.FinancialDate.substr(4,2).cast('int'))+(12*((pqDF.FinancialDate.substr(1,2).cast('int'))-94)))- {0})*30+(pqDF.FinancialDate.substr(7,2).cast('int'))).alias('dayNum')).filter("ProccessCode='000000' AND MessageType='200'AND SuccessOrFailure='S'AND FinancialDate between {1} and {2}")
Sp_Df= Sp_Df.groupBy(Sp_Df.Merchantnumber,Sp_Df.dayNum).agg(sf.sum(Sp_Df.Amount).alias("TxnSum"),sf.count(Sp_Df.Amount).alias("TxnNo"))
Sp_Df = Sp_Df.groupBy(Sp_Df.Merchantnumber).agg(sf.sum(Sp_Df.TxnNo).alias("all_transactions"),sf.sum(Sp_Df.TxnSum).alias("sum_amounts"),sf.sum((Sp_Df.TxnNo)/Sp_Df.dayNum).alias("harmonic"))
Sp_rdd = Sp_Df.rdd
Sp_Piperdd = Sp_rdd.map(lambda x:(x[1],x[2],x[3]))
model = KMeans.train(Sp_Piperdd, {3})
labels =model.predict(Sp_Piperdd)
labels = labels.map(Row("label")).toDF()   #add schema
df11 =  Sp_Df.withColumn("id", monotonically_increasing_id())
df22 =  labels.withColumn("id", monotonically_increasing_id())
newDF = df11.join(df22, df11.id == df22.id, 'inner').drop(df22.id)
pandas_df=newDF.toPandas()
json1 = pandas_df.to_json()
"""
