package exercise_3;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;

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

import java.util.stream.Collectors;

public class Exercise_3 {

    public static class Vertex implements Serializable{
        private Integer cost;
        private List<Long> path;

        public Vertex(Integer cost, List<Long> path) {
            this.cost = cost;
            this.path = path;
        }
        public Vertex() { // create an empty vertex
            this.cost = Integer.MAX_VALUE;
            this.path = new ArrayList<Long>();
        }
        public Integer cost() {return cost;}

        public List<Long> path() {return path;}

        public String toString() {
            return path + " with cost " + cost;
        }
    }

    private static class VProg extends AbstractFunction3<Long, Vertex,Vertex,Vertex> implements Serializable {
        @Override
        public Vertex apply(Long vertexID, Vertex vertexValue, Vertex message) {
            if (message.cost() == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
                return  vertexValue.cost() < message.cost() ? vertexValue : message;
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Vertex,Integer>, Iterator<Tuple2<Object,Vertex>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Vertex>> apply(EdgeTriplet<Vertex, Integer> triplet) {
            Tuple2<Object,Vertex> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Vertex> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2.cost() == Integer.MAX_VALUE) {   // source vertex value is smaller than dst vertex?
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Vertex>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                List<Long> path = sourceVertex._2().path();
                path.add(dstVertex._2().cost().longValue());
                Integer cost = sourceVertex._2.cost() + triplet.toTuple()._3();
                Vertex newvert = new Vertex(cost, path);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Vertex>(triplet.dstId(),newvert)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Vertex,Vertex,Vertex> implements Serializable {
        @Override
        public Vertex apply(Vertex o, Vertex o2) {
            return null;
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

    List<Tuple2<Object,Vertex>> vertices = Lists.newArrayList(
            new Tuple2<Object,Vertex>(1l,new Vertex(0, Lists.newArrayList())),
            new Tuple2<Object,Vertex>(2l,new Vertex(Integer.MAX_VALUE, Lists.newArrayList())),
            new Tuple2<Object,Vertex>(3l,new Vertex(Integer.MAX_VALUE, Lists.newArrayList())),
            new Tuple2<Object,Vertex>(4l,new Vertex(Integer.MAX_VALUE, Lists.newArrayList())),
            new Tuple2<Object,Vertex>(5l,new Vertex(Integer.MAX_VALUE, Lists.newArrayList())),
            new Tuple2<Object,Vertex>(6l,new Vertex(Integer.MAX_VALUE, Lists.newArrayList()))
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

    JavaRDD<Tuple2<Object,Vertex>> verticesRDD = ctx.parallelize(vertices);
    JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

    Graph<Vertex,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Vertex(Integer.MAX_VALUE, Lists.newArrayList()), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
            scala.reflect.ClassTag$.MODULE$.apply(Vertex.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

    GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Vertex.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

    ops.pregel(Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            EdgeDirection.Out(),
            new VProg(),
            new sendMsg(),
            new merge(),
            ClassTag$.MODULE$.apply(Vertex.class))
        .vertices()
        .toJavaRDD()
        .foreach(v -> {
            Tuple2<Object,Vertex> vertex = (Tuple2<Object,Vertex>)v;
            System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+ vertex._2.toString());
        });
    }



}
        

