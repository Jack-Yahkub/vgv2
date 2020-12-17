package de.fraunhofer.igd.geo.vgv.flink;


import de.fraunhofer.igd.geo.vgv.flink.FirstLayerMapper;
import de.fraunhofer.igd.geo.vgv.flink.NthLayerMapper;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import java.lang.Math;
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
        // TODO
	DataSet<String> lines = env.readTextFile(args[1]);
//	double boundingBox = new double[3][2];
//	DataSet<double[3][2]> 
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
	System.out.println("\t x \t y \t z");
	for(int i = 0; i < 2; i++){
		for(int j = 0; j < 3; j++){
			System.out.print(boundingBox[j][i] + "\t");
		}
		System.out.println();
	}

	int layer = 0;

		DataSet<Tuple2<Integer,String>> groupedPoints = lines
		.map(new FirstLayerMapper(boundingBox));
		groupedPoints
		.groupBy(0)
		.first(100)
//		.reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, String>() {
//			@Override
//			public void reduce(Iterable<Tuple2<Integer, String>> points, Collector<String> points_file) throws Exception {
//				String points_lines = "";
//				for (Tuple2<Integer, String> point : points) {
//					points_lines += point.f1 + "\n";
//				}
//				points_file.collect(points_lines);
//			}
//		})
		.map(new MapFunction<Tuple2<Integer,String>, Tuple2<Integer,String>>() {
			@Override
			public Tuple2<Integer,String> map(Tuple2<Integer,String> point) throws Exception {
				return new Tuple2<>(-1,point.f1);
			}
		})
		.writeAsFormattedText("layer"+layer+".xyz",FileSystem.WriteMode.OVERWRITE, element -> element.f1).setParallelism(1);
	while(true){
		layer++;
		groupedPoints = groupedPoints
		.filter(new FilterFunction<Tuple2<Integer,String>>(){
			@Override
			public boolean filter(Tuple2<Integer,String> point){
				return !point.f0.equals(-1);
			}
		})
		.map(new NthLayerMapper(boundingBox, layer));
		System.out.println(groupedPoints.count());
		groupedPoints	
		.groupBy(0)
		.first(100)
//		.reduceGroup(new GroupReduceFunction<Tuple2<Integer,String>, String>() {
//			@Override
//			public void reduce(Iterable<Tuple2<Integer, String>> points, Collector<String> points_file) throws Exception {
//				String points_lines = "";
//				for (Tuple2<Integer, String> point : points) {
//					points_lines += point.f1 + "\n";
//				}
//				points_file.collect(points_lines);
//			}
//		})
		.map(new MapFunction<Tuple2<Integer,String>, Tuple2<Integer,String>>() {
			@Override
			public Tuple2<Integer,String> map(Tuple2<Integer,String> point) throws Exception {
				return new Tuple2<>(-1,point.f1);
			}
		})
		.writeAsFormattedText("layer"+layer+".xyz",FileSystem.WriteMode.OVERWRITE, element -> element.f1).setParallelism(1);
//		nthLayerPoints.
//		map(new MapFunction<String, Tuple2<Integer,String>>() {
//			@Override
//			public Tuple2<Integer,String> map(String savedPoint) {
//				return new Tuple2<>(-1, savedPoint);
//			}
//		});
//		.reduceGroup(new GroupReduceFunction<String, String>() {
//			@Override
//			public void reduce(Iterable<String> savedPoints, Collector<String> cleared) throws Exception {
//				cleared.collect("yes");	
//			}
//		});
		long remainingPoints = groupedPoints.count();
		System.out.println(remainingPoints);
		if(remainingPoints == 0) {
			break;
		}
	}
	System.out.println("Done");
    }
}
