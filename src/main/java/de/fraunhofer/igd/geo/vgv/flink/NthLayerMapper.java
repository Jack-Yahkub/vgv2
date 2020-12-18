package de.fraunhofer.igd.geo.vgv.flink;
 
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class NthLayerMapper implements MapFunction<Tuple2<Integer,String>, Tuple2<Integer,String>> {
	private double[][] bBox;
	private int layer;
	public NthLayerMapper(double[][] bBox, int layer) {
	        this.bBox = bBox;
	        this.layer = layer;
	}
	@Override
	public Tuple2<Integer, String> map(Tuple2<Integer, String> groupedPoint) throws Exception {
		String[] split = groupedPoint.f1.split(" ");
	        double x = Double.valueOf(split[0]);
	        double y = Double.valueOf(split[1]);
	        // Morton Code for indices 0b0 and 0b1 on top of previous indices
	        // Compute absolute x and y indices in bounding box grid and then 
	        // interleave with zeros and OR xID with once left bitshifted yID
	        // to obtain new cellID and attach it to the output.
	        int xID = (int) (Math.pow(2,layer)*10*(x - bBox[0][0])/(bBox[0][1] - bBox[0][0]));
	        xID = xID == (int) Math.pow(2,layer)*10 ? (int) Math.pow(2,layer)*10-1 : xID;
	        xID = zeroInterleaveBits(xID);
	        int yID = (int) (Math.pow(2,layer)*10*(y - bBox[1][0])/(bBox[1][1] - bBox[1][0]));
	        yID = yID == (int) Math.pow(2,layer)*10 ? (int) Math.pow(2,layer)*10-1 : yID;
	        yID = zeroInterleaveBits(yID);
	        groupedPoint.f0 = xID | (yID << 1);
	        return groupedPoint;
	}

	private static final int bitmasks[] = {0x55555555, 0x33333333, 0x0f0f0f0f, 0x00FF00FF};
	private static final int bitshifts[] = {0b1, 0b10, 0b100, 0b1000};
	public static int zeroInterleaveBits(int bits){
	    for(int i = 3; i >= 0; i--){
	            bits = (bits | (bits << bitshifts[i])) & bitmasks[i];
	    }
	    return bits;
	}

}
