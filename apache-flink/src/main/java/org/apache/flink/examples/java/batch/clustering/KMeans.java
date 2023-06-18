package org.apache.flink.examples.java.batch.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.batch.clustering.util.KMeansData;

import java.io.Serializable;
import java.util.Collection;

public class KMeans {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig()
                .setGlobalJobParameters(params); // make parameters available in the web interface

        // get input data:
        // read the points and centroids from the provided paths or fall back to default data
        DataSet<Point> points = getPointDataSet(params, env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);

        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids =
                points
                        // compute closest centroid for each point
                        .map(new SelectNearestCenter())
                        .withBroadcastSet(loop, "centroids")
                        // count and sum point coordinates for each centroid
                        .map(new CountAppender())
                        .groupBy(0)
                        .reduce(new CentroidAccumulator())
                        // compute new centroids from point counts and coordinate sums
                        .map(new CentroidAverager());

        // feed new centroids back into next iteration
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Point>> clusteredPoints =
                points
                        // assign points to final clusters
                        .map(new SelectNearestCenter())
                        .withBroadcastSet(finalCentroids, "centroids");

        // emit result
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            clusteredPoints.print();
        }
    }

    // *************************************************************************
    //     DATA SOURCE READING (POINTS AND CENTROIDS)
    // *************************************************************************

    private static DataSet<Centroid> getCentroidDataSet(
            ParameterTool params, ExecutionEnvironment env) {
        DataSet<Centroid> centroids;
        if (params.has("centroids")) {
            centroids =
                    env.readCsvFile(params.get("centroids"))
                            .fieldDelimiter(" ")
                            .pojoType(Centroid.class, "id", "x", "y");
        } else {
            System.out.println("Executing K-Means example with default centroid data set.");
            System.out.println("Use --centroids to specify file input.");
            centroids = KMeansData.getDefaultCentroidDataSet(env);
        }
        return centroids;
    }

    private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Point> points;
        if (params.has("points")) {
            // read points from CSV file
            points =
                    env.readCsvFile(params.get("points"))
                            .fieldDelimiter(" ")
                            .pojoType(Point.class, "x", "y");
        } else {
            System.out.println("Executing K-Means example with default point data set.");
            System.out.println("Use --points to specify file input.");
            points = KMeansData.getDefaultPointDataSet(env);
        }
        return points;
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    /** A simple two-dimensional point. */
    public static class Point implements Serializable {

        public double x, y;

        public Point() {}

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public Point add(Point other) {
            x += other.x;
            y += other.y;
            return this;
        }

        public Point div(long val) {
            x /= val;
            y /= val;
            return this;
        }

        public double euclideanDistance(Point other) {
            return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
        }

        public void clear() {
            x = y = 0.0;
        }

        @Override
        public String toString() {
            return x + " " + y;
        }
    }

    /** A simple two-dimensional centroid, basically a point with an ID. */
    public static class Centroid extends Point {

        public int id;

        public Centroid() {}

        public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        public Centroid(int id, Point p) {
            super(p.x, p.y);
            this.id = id;
        }

        @Override
        public String toString() {
            return id + " " + super.toString();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Determines the closest cluster center for a data point. */
    @FunctionAnnotation.ForwardedFields("*->1")
    public static final class SelectNearestCenter
            extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /** Appends a count variable to the tuple. */
    @FunctionAnnotation.ForwardedFields("f0;f1")
    public static final class CountAppender
            implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /** Sums and counts point coordinates. */
    @FunctionAnnotation.ForwardedFields("0")
    public static final class CentroidAccumulator
            implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(
                Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /** Computes new centroid from coordinate sum and count of points. */
    @FunctionAnnotation.ForwardedFields("0->id")
    public static final class CentroidAverager
            implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2));
        }
    }
}
