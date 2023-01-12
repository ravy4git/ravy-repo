from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

# Create a SparkSession
spark = SparkSession.builder.appName("Index Creation").getOrCreate()

# Load the JDBC driver for SQL Server
spark.sparkContext.addPyFile("path/to/sqljdbc4.jar")

# Connect to the SQL Server database
url = "jdbc:sqlserver://host:port;database=dbname"
properties = spark._jvm.java.util.Properties()
properties.setProperty("user", "username")
properties.setProperty("password", "password")
properties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

# Get connection from DriverManager
connection = spark._jvm.java.sql.DriverManager.getConnection(url, properties)

# Create the index
stmt = connection.createStatement()
stmt.execute("CREATE INDEX myindex ON mytable (mycolumn)")

# Close the connection
connection.close()

# Close the SparkSession
spark.stop()
