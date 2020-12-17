package de.fraunhofer.igd.geo.vgv.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class FirstLayerMapper implements MapFunction<String, Tuple2<Integer,String>> {
	private double[][] bBox;
	public FirstLayerMapper(double[][] bBox) {
	        this.bBox = bBox;
	}
	@Override
	public Tuple2<Integer, String> map(String dataline) throws Exception {
	        String[] split = dataline.split(" ");
	        double x = Double.valueOf(split[0]);
	        double y = Double.valueOf(split[1]);
	        // Morton Code for indices 0x0 to 0x9 
	        // Compute indices by dividing through bounding box dimensions and
	        // scaling the result to a 0-9 integer. Interleave xID and yID and
	        // OR xID with a once left bitshifted yID. The result is the cellID
	        int xID = (int) (10*(x - bBox[0][0])/(bBox[0][1] - bBox[0][0]));
	        xID = xID == 10 ? 9 : xID;
	        int yID = (int) (10*(y - bBox[1][0])/(bBox[1][1] - bBox[1][0]));
	        yID = yID == 10 ? 9 : yID;
	
	        int xID_interleave = (xID | (xID << 2)) & 0x33;
	        xID_interleave = (xID_interleave | (xID_interleave << 1)) & 0x55;
	        int yID_interleave = (yID | (yID << 2)) & 0x33;
	        yID_interleave = ((yID_interleave | (yID_interleave << 1)) & 0x55) << 1;
	        int cellID = xID_interleave | yID_interleave;
	
	        return new Tuple2<>(cellID,dataline);
	}
}
