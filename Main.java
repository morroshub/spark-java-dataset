package com.lostsys.spark1;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

public class Main {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
            .builder()
            .appName("TestApp1")
            .getOrCreate();           

        StructType schema = new StructType()
            .add("InvoiceNo", "string")
            .add("StockCode", "string")
            .add("Description", "string")
            .add("Quantity", "integer") 
            .add("InvoiceDate", "date")  
            .add("UnitPrice", "long")
            .add("CustomerID", "string")
            .add("Country", "string");

        Dataset<Row> df = spark.read()
            .option("mode", "DROPMALFORMED")
            .schema(schema)
            .csv("file:///Users/lucasmorrone/Documents/Projectos_data/ECOMMERCE-DELIVERY/ecommerce-dataset.csv");

        System.out.println("Total registros: " + df.count());

        df.createOrReplaceTempView("pedidos");
        Dataset<Row> sqlResult = spark.sql("SELECT InvoiceNo, InvoiceDate, CustomerID, SUM(UnitPrice * Quantity) AS Total FROM pedidos GROUP BY InvoiceNo, InvoiceDate, CustomerID ORDER BY Total DESC LIMIT 10");

        sqlResult.show();

        sqlResult.write().option("header", "true").csv("file:///Users/lucasmorrone/Documents/Projectos_data/ECOMMERCE-DELIVERY/procesados");
        
        spark.stop(); // Detener la sesión de Spark al finalizar la aplicación 
    }
}
