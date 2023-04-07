package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.util.List;
import java.io.BufferedReader;
import java.util.ArrayList;

public class Exercise_4 {
	static String PATH_EDGES = "/home/miriam/Documentos/SDM/SDM-Distributed-Graphs/src/main/resources/wiki-edges.txt";
	static String PATH_VERTICES = "/home/miriam/Documentos/SDM/SDM-Distributed-Graphs/src/main/resources/wiki-vertices.txt";


	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		// vertices creation
		JavaRDD<String> vertices_raw = ctx.textFile(PATH_VERTICES);
		JavaRDD<Row> vertices_rdd = vertices_raw.map(line -> {
			String[] text = line.split("\t");
			return RowFactory.create(text[0],text[1]);
		});

		StructType vertices_schema = new StructType(new StructField[]{
			new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		
		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
		
		// edges creation
		JavaRDD<String> edges_raw = ctx.textFile(PATH_EDGES);
		JavaRDD<Row> edges_rdd = edges_raw.map(line -> {
			String[] text = line.split("\t");
			return RowFactory.create(text[0],text[1]);
		});
		StructType edges_schema = new StructType(new StructField[]{
			new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);
		
		GraphFrame gf = GraphFrame.apply(vertices,edges);
		System.out.println(gf);
	
		gf.edges().show();
		gf.vertices().show();

	}
	
	
}
