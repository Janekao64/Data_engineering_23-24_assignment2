#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import random
from random import randint
from pyspark.sql import functions as f
from pyspark.sql.types import StringType


# In[3]:


spark_conf = (
SparkConf()
.set("spark.jars.packages", 'org.apache.hadoop:hadoop-client:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4')
.set("spark.driver.memory", "6g")
.set("spark.hadoop.fs.s3a.endpoint", "minio:9000")
.set("spark.hadoop.fs.s3a.access.key", "0AkJFIkokNT5VVeXLP0T")
.set("spark.hadoop.fs.s3a.secret.key","hNSiPnSFteuiOhZJXAaatsNkx8Z65C1AOvXXQvYm" )
.set("spark.hadoop.fs.s3a.path.style.access", "true")
.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)
sc = SparkContext.getOrCreate(spark_conf)
spark = SparkSession(sc)


# In[4]:


print(f"Hadoop version = {sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")


# In[5]:


N=10000
M=12500
K=150

documentCurrency = ["EUR", "USD", "JPY", "GBR", "CHF"]
documentService = ["Tax", "Rent", "Loan", "Insurance", "Utility"]

from random import randint
from datetime import datetime,timedelta
def invoiceGenerator(k=5):
  postingDate = datetime(2022,1,1)+timedelta(randint(0,200))
  return {
          "customerId":"Customer_{customerId}".format(customerId=str(randint(0,k)+1).zfill(3)),
          "amount":randint(50,10000),
          "documentCurrency":random.choice(documentCurrency), 
          "postingDate":postingDate.strftime("%Y-%m-%d"),
          "dueDate":(postingDate+timedelta(randint(0,60))).strftime("%Y-%m-%d"),
          "fiscalYear":postingDate.strftime("%Y"),
          "documentService":random.choice(documentService)
         }

def invoiceList(k=5,n=1000):
  rawInvoiceList = [invoiceGenerator(k) for k in range(n)]
  rawInvoiceList.sort(key=lambda row: row.get("postingDate"))
  for pos,val in enumerate(rawInvoiceList):
    val["documentNumber"]="2022-{docNum}".format(docNum=str(pos).zfill(5))
  return rawInvoiceList
  
myInvoiceList = invoiceList(K,N)


# In[6]:


def paymentGenerator(paymentList):
  documentNumber = "2022-{docNum}".format(docNum=str(randint(0,len(paymentList)-1)).zfill(5))
  payment = [k for k in paymentList if k.get("documentNumber")==documentNumber][0]
  postingDate = datetime.strptime(payment.get("postingDate"),"%Y-%m-%d")
  return { 
          "documentNumber":documentNumber,
          "paymentDate":(postingDate+timedelta(randint(15,90))).strftime("%Y-%m-%d"),
          "valuePaid":randint(1,payment.get("amount")),
          "documentCurrency":payment.get("documentCurrency")
         }


def paymentList(paymentList,m=250):
  return [paymentGenerator(paymentList) for k in range(m)]
   
myPaymentList = paymentList(myInvoiceList,M)  


# In[7]:


invoice_rdd = sc.parallelize(myInvoiceList)
invoice_rdd


# In[8]:


payment_rdd = sc.parallelize(myPaymentList)
payment_rdd


# In[13]:


columns = ['value', 'customerId', 'documentCurrency', 'documentNumber', 'documentService', 'dueDate', 'fiscalYear', 'postingDate']
invoice_df = invoice_rdd.toDF(columns)
invoice_df.printSchema()


# In[10]:


invoice_df.first()


# In[11]:


payment_df = payment_rdd.toDF(["documentCurrency","documentNumber","paymentDate", "valuePaid"])
payment_df.printSchema()


# In[48]:


absolute_path = "/tmp/my_parquet_data"
invoice_df.write.format('parquet').mode('overwrite').save(absolute_path)


# In[12]:


payment_df.first()


# In[15]:


# REPORT WHICH SHOWS THE UNPAID INVOICES

from pyspark.sql import functions as F

payment_sum_df = payment_df.groupBy("documentNumber").agg(F.sum("valuePaid").alias("totalPaid"))

joined_df = invoice_df.join(payment_sum_df, on="documentNumber", how="left")

unpaid_invoices_df = joined_df.filter(joined_df["value"] > F.coalesce(joined_df["totalPaid"], F.lit(0)))

result_df = unpaid_invoices_df.select("value", "customerId", "documentCurrency", "documentNumber", "documentService", "dueDate", "fiscalYear", "postingDate", "totalPaid")

result_df.show()


# In[ ]:




