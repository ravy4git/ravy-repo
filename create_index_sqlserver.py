jvm = spark._jvm
DriverManager = jvm.java.sql.DriverManager
Properties = jvm.java.util.Properties

properties = Properties()
properties.setProperty("authentication", "ActiveDirectoryServicePrincipal")
properties.setProperty("user", <username>)
properties.setProperty("password", <password>)
properties.setProperty("encrypt", "true")
properties.setProperty("trustServerCertificate", "true")
properties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

sqlserver = "<server_name_or_ip_address>"
database = "<database>"

connection = DriverManager.getConnection(
    f"jdbc:sqlserver://{sqlserver}:1433;databaseName={database};",
    properties
)

statement = connection.createStatement()
index = "<index_name>"
object = "<table_name>"
columns = ["<col1>", "<col2>", ...]
result_set = statement.executeQuery(f"""
IF NOT EXISTS (
  SELECT 1 
  FROM sys.indexes 
  WHERE object_id = OBJECT_ID('{object}')
  AND name = '{index}'
)
CREATE INDEX [{index}] ON [{object}] ({str.join(",", columns)}
"""))

result_set.close()
statement.close()
connection.close()
