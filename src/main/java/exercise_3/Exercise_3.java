package exercise_3;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long,Tuple2<Integer,List<Long>>,Tuple2<Integer,List<Long>>,Tuple2<Integer,List<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer,List<Long>> apply(Long vertexID, Tuple2<Integer,List<Long>> vertexValue, Tuple2<Integer,List<Long>> message) {
            if (message._1() == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
                if(message._1() <= vertexValue._1())
                    return message;
                else
                    return vertexValue;
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer,List<Long>>,Integer>, Iterator<Tuple2<Object,Tuple2<Integer,List<Long>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer,List<Long>>>> apply(EdgeTriplet<Tuple2<Integer,List<Long>>, Integer> triplet) {
            Tuple2<Object,Tuple2<Integer,List<Long>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Tuple2<Integer,List<Long>>> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2._1() == Integer.MAX_VALUE) {   // source vertex value is smaller than dst vertex?
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Tuple2<Integer,List<Long>>>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                List<Long> path = sourceVertex._2()._2();
                path.add(Long.parseLong(String.valueOf(dstVertex._2()._1())));

                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Tuple2<Integer,List<Long>>>(triplet.dstId(),new Tuple2<>(sourceVertex._2._1() + triplet.toTuple()._3(),path))).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer,List<Long>>,Tuple2<Integer,List<Long>>,Tuple2<Integer,List<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer,List<Long>> apply(Tuple2<Integer,List<Long>> o, Tuple2<Integer,List<Long>> o2) {
            if (o._1 >= o2._1) {
                return o2;
            } else {
                return o;
            }
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
        .put(1l, "A")
        .put(2l, "B")
        .put(3l, "C")
        .put(4l, "D")
        .put(5l, "E")
        .put(6l, "F")
        .build();

    List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(
            new Tuple2<Object,Integer>(1l,0),
            new Tuple2<Object,Integer>(2l,Integer.MAX_VALUE),
            new Tuple2<Object,Integer>(3l,Integer.MAX_VALUE),
            new Tuple2<Object,Integer>(4l,Integer.MAX_VALUE),
            new Tuple2<Object,Integer>(5l,Integer.MAX_VALUE),
            new Tuple2<Object,Integer>(6l,Integer.MAX_VALUE)
    );
    List<Edge<Integer>> edges = Lists.newArrayList(
            new Edge<Integer>(1l,2l, 4), // A --> B (4)
            new Edge<Integer>(1l,3l, 2), // A --> C (2)
            new Edge<Integer>(2l,3l, 5), // B --> C (5)
            new Edge<Integer>(2l,4l, 10), // B --> D (10)
            new Edge<Integer>(3l,5l, 3), // C --> E (3)
            new Edge<Integer>(5l, 4l, 4), // E --> D (4)
            new Edge<Integer>(4l, 6l, 11) // D --> F (11)
    );

    JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
    JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

    Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
            scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

    GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

    ops.pregel(Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            EdgeDirection.Out(),
            new VProg(),
            new sendMsg(),
            new merge(),
            ClassTag$.MODULE$.apply(Integer.class))
        .vertices()
        .toJavaRDD()
        .foreach(v -> {
            Tuple2<Object,Integer> vertex = (Tuple2<Object,Integer>)v;
            System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+vertex._2);
        });
    }



}
        

