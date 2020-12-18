package de.fraunhofer.igd.geo.vgv.flink;

import de.fraunhofer.igd.geo.vgv.flink.FirstLayerMapper;
import de.fraunhofer.igd.geo.vgv.flink.NthLayerMapper;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import java.lang.Math;
import java.util.Iterator;
import org.apache.flink.core.fs.FileSystem;
public class PointCloudLOD {
    public static void main(String... args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String dataFile = ""; // path to the point cloud file
        if (params.has("data")) {
            dataFile = params.get("data");
        } else {
            throw new RuntimeException("No --data attribute passed. Set the location of the input data file.");
        }

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 1. Bounding box computation
	DataSet<String> lines = env.readTextFile(args[1]);
        double boundingBox[][] = lines
	.map(new MapFunction<String,double[][]>() {
		@Override
		public double[][] map(String dataline) throws Exception {
			String[] split = dataline.split(" ");
			double[][] xyz2 = {
					{Double.valueOf(split[0]),Double.valueOf(split[0])},
					{Double.valueOf(split[1]),Double.valueOf(split[1])},
					{Double.valueOf(split[2]),Double.valueOf(split[2])}
					};
			return xyz2;
		}
	})
	// Compare the maxima and minima in x,y,z coordinates of points 0 to n-1 with the x,y,z
	// coordinates of point n and update maxima and minima if applicable
	.reduce(new ReduceFunction<double[][]>() {
		@Override
		public double[][] reduce(double[][] xyz21, double[][] xyz22){
			if(xyz21[0][0] > xyz22[0][0]) {
				xyz21[0][0] = xyz22[0][0];
			}
			if(xyz21[0][1] < xyz22[0][0]){
				xyz21[0][1] = xyz22[0][0];
			}	
			
			if(xyz21[1][0] > xyz22[1][0]) {
				xyz21[1][0] = xyz22[1][0];
			}
			if(xyz21[1][1] < xyz22[1][0]){
				xyz21[1][1] = xyz22[1][0];
			}
			
			if(xyz21[2][0] > xyz22[2][0]) {
				xyz21[2][0] = xyz22[2][0];
			}
			if(xyz21[2][1] < xyz22[2][0]){
				xyz21[2][1] = xyz22[2][0];
			}
			return xyz21;	
		}
	})
	.collect().get(0);
	// Print bounding box to standard output
	System.out.println("\t x \t y \t z");
	for(int i = 0; i < 2; i++){
		for(int j = 0; j < 3; j++){
			System.out.print(boundingBox[j][i] + "\t");
		}
		System.out.println();
	}
	// Handle the first layer
	// 2. and 3. Construct an implicit grid by assigning integers to each point that designate the
	// placement of the point within the grid using Morton-Code 
	int layer = 0;
		DataSet<Tuple2<Integer,String>> groupedPoints = lines
		.map(new FirstLayerMapper(boundingBox))
		// 4. Group points that share the same cell
		.groupBy(0)
		// 5. Use a GroupReduceFunction to select up to 100 distinct points from within each cell and
		// mark them for assignment to the current layer
		.reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer,String>>() {
			@Override
			public void reduce(Iterable<Tuple2<Integer, String>> points, Collector<Tuple2<Integer, String>> points_file) throws Exception {
				String points_lines = "";
				int linecount = 0;
				Iterator<Tuple2<Integer,String>> pointsIt = points.iterator();
				while (pointsIt.hasNext()) {
					if(linecount < 100){
						points_file.collect(new Tuple2<>(-1,pointsIt.next().f1));
					}
					else{ 
						points_file.collect(pointsIt.next());
					}
					linecount++;
				}
			}
		});
		// 8. Select the marked points by filtering and write them to file
		groupedPoints
		.filter(new FilterFunction<Tuple2<Integer,String>>(){
			@Override
			public boolean filter(Tuple2<Integer,String> point){
				return point.f0.equals(-1);
			}
		})
		.writeAsFormattedText("layer"+layer+".xyz",FileSystem.WriteMode.OVERWRITE, element -> element.f1).setParallelism(1);
		// Remove points that have been written to the first layer
		groupedPoints = groupedPoints
		.filter(new FilterFunction<Tuple2<Integer,String>>(){
			@Override
			public boolean filter(Tuple2<Integer,String> point){
				return !point.f0.equals(-1);
			}
		});

	// 7. Handle the remaining layers
	while(true){
		layer++;
		// 6. Each layer is divided horizontally and vertically and points are assigned to their cells 
		// in NthLayerMapper
		DataSet<Tuple2<Integer,String>> assignedPoints = groupedPoints
		.map(new NthLayerMapper(boundingBox, layer))
		// 4. Group points of the current layer
		.groupBy(0)
		// 5. Select up to 100 distinct points from within each cell and mark them for assignment 
		.reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer,String>>() {
			@Override
			public void reduce(Iterable<Tuple2<Integer, String>> points, Collector<Tuple2<Integer, String>> points_file) throws Exception {
				String points_lines = "";
				Integer cellID = 0;
				int linecount = 0;
				Iterator<Tuple2<Integer,String>> pointsIt = points.iterator();
				while (pointsIt.hasNext()) {
					if(linecount < 100){
						points_file.collect(new Tuple2<>(-1,pointsIt.next().f1));
					}
					else{ 
						points_file.collect(pointsIt.next());
					}
					linecount++;
				}
			}
		});
		// 8. Filtering to write layer to file
		assignedPoints
		.filter(new FilterFunction<Tuple2<Integer,String>>(){
			@Override
			public boolean filter(Tuple2<Integer,String> point){
				return point.f0.equals(-1);
			}
		})
		.writeAsFormattedText("layer"+layer+".xyz",FileSystem.WriteMode.OVERWRITE, element -> element.f1).setParallelism(1);
		// Remove points that have been written to current layer
		groupedPoints = assignedPoints
		.filter(new FilterFunction<Tuple2<Integer,String>>(){
			@Override
			public boolean filter(Tuple2<Integer,String> point){
				return !point.f0.equals(-1);
			}
		});
		long remainingPoints = groupedPoints.count();
		System.out.println(remainingPoints + " points still remaining for usage in finer layers.");
		if(remainingPoints == 0) {
			break;
		}
	}
	System.out.println("Done");
    }
}
