//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
import java.io.*;
import java.lang.Math;
import java.lang.Runtime;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.HashPartitioner;
//import org.apache.hama.bsp.NullOutputFormat;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.sync.SyncException;

public class DelaunayTriangulator {
    public static class DelaunayTriangulatorBSP 
            extends BSP<LongWritable, Text, NullWritable, Text, Text> {
            //extends BSP<LongWritable, Text, NullWritable, NullWritable, BytesWritable> {

	    private String masterTask;

	    @Override
	    public void bsp(BSPPeer<LongWritable, Text, NullWritable, Text, Text> peer)
                throws IOException, SyncException, InterruptedException {
            LongWritable key = new LongWritable();
            Text value = new Text();
            
//            ArrayList<String> input = new ArrayList<String>();
//            input.add("72113143 " + "494845000");
//            input.add("72114559 " + "494852000");
//            input.add("72106321 " + "494842000");
//            input.add("72108243 " + "494838238");
//            
//            ArrayList<String> points = new ArrayList<String>();
////            points.add("72114559 " + "494851999");
////            points.add("72114559 " + "494852001");
////            points.add("72114560 " + "494852000");
////            points.add("72114558 " + "494852000");
//            points.add("72113143 " + "494845000");
//            
//            ArrayList<String> triangles = LocalTriangulator(input, peer);
//            ArrayList<String> edges = formatTriangulation(triangles);
//            for(String edge : edges) {
//                //System.out.println("Edge is " + edge);
//                for(String point : points) {
//                    if(conflictTest(edge, point) == true) {
//                        //System.out.println("Point " + point + "in conflict with edge");
//                    }
//                }
//            }
            
            
            // read input points from file
            ArrayList<String> inputPoints = new ArrayList<String>();
            while (peer.readNext(key, value)) {
                inputPoints.add(value.toString());
            }
            
            // list of edges against whose kernel we need to check the output triangles; initially empty
            ArrayList<String> conflictEdges = new ArrayList<String>();
            
            // call triangulation procedure
            Triangulator(new ArrayList<String>(Arrays.asList(peer.getAllPeerNames())), masterTask, inputPoints, conflictEdges, peer);
            
            //System.out.println("Peer " + peer.getPeerName() + " signing off!!");
            //peer.sync();
	    }

	    @Override
	    public void setup(BSPPeer<LongWritable, Text, NullWritable, Text, Text> peer)
		        throws IOException {
		    // choose a master task among all peers
		    this.masterTask = peer.getPeerName(peer.getNumPeers()/2);
	    }

	    @Override
	    public void cleanup(BSPPeer<LongWritable, Text, NullWritable, Text, Text> peer)
            	throws IOException {
            //System.out.println("Bye bye");
	    }
	    
	    // Check edge type. Returns 1 for non-boundary edge, 2 for boundary edge,
	    // and 3 for edge with one endpoint at infinity. Returns 0 for error.
	    private int checkEdgeType(String edge) {
	        String[] tokens = (edge.trim()).split("\\s+");
	        if((!tokens[0].equals("inf")) && (!tokens[1].equals("inf")) &&
	            (!tokens[2].equals("inf")) && (!tokens[3].equals("inf")) &&
	            (!tokens[4].equals("inf")) && (!tokens[5].equals("inf")) &&
	            (!tokens[6].equals("inf")) && (!tokens[7].equals("inf"))) { // non-boundary edge
	            return 1;   
	        }
	        if((tokens[4].equals("inf") && tokens[5].equals("inf")) ||
	            (tokens[6].equals("inf") && tokens[7].equals("inf"))) { // boundary edge
	            return 2;
	        }
	        if((tokens[0].equals("inf") && tokens[1].equals("inf")) ||
	             (tokens[2].equals("inf") && tokens[3].equals("inf")) ) { // one end of the edge is at infinity
	            return 3;
	        }
	        return 0;
	    }
	    
	    // Checks if point p is in the one-sided strip bounded by segment joining pa and pb and 
	    // the rays perpendicular to the segment and incident to pa and pb, with the strip contained in the halfplane
	    // not containing pc and bounded by the line passing through pa and pb.
	    private boolean unboundedKernelTest (double[] pa, double[] pb, double[] pc, double[] p) {
	        if(pa[1] == pb[1]) { // segment joining pa and pb is horizontal
            	if(pc[1] <= pa[1]) {
            		if(pa[0] <= pb[0]) {
            			if( (pa[0] <= p[0]) && (p[0] <= pb[0]) && (pa[1] <=  p[1]) ) {
            				return true;
            			}
            		}
            		else{
            			if( (pb[0] <= p[0]) && (p[0] <= pa[0]) && (pa[1] <=  p[1]) ) {
            				return true;
            			}
            		}
            	}
            	else {
            		if(pa[0] <= pb[0]) {
            			if( (pa[0] <= p[0]) && (p[0] <= pb[0]) && (pa[1] >=  p[1]) ) {
            				return true;
            			}
            		}
            		else{
            			if( (pb[0] <= p[0]) && (p[0] <= pa[0]) && (pa[1] >=  p[1]) ) {
            				return true;
            			}
            		}
            	}
            }
            else if(pa[0] == pb[0]) { // segment joining pa and pb is vertical
            	if(pc[0] <= pa[0]) {
            		if(pa[1] <= pb[1]) {
            			if( (pa[1] <= p[1]) && (p[1] <= pb[1]) && (pa[0] <=  p[0]) ) {
            				return true;
            			}
            		}
            		else{
            			if( (pb[1] <= p[1]) && (p[1] <= pa[1]) && (pa[0] <=  p[0]) ) {
            				return true;
            			}
            		}
            	}
            	else {
            		if(pa[1] <= pb[1]) {
            			if( (pa[1] <= p[1]) && (p[1] <= pb[1]) && (pa[0] >=  p[0]) ) {
            				return true;
            			}
            		}
            		else{
            			if( (pb[1] <= p[1]) && (p[1] <= pa[1]) && (pa[0] >=  p[0]) ) {
            				return true;
            			}
            		}
            	}
            }
            else { // segment joining pa and pb is neither horizontal nor vertical
            	double slope = -1*(pb[0] - pa[0])/(pb[1] - pa[1]);
            	double[] pa1 = new double[2]; double[] pb1 = new double[2];
            	pa1[0] = pa[0] + 1; pa1[1] = pa[1] + slope;
            	pb1[0] = pb[0] + 1; pb1[1] = pb[1] + slope;
            	if(Primitives.orient2d(pa,pb,pc) >=0) {
            		if(Primitives.orient2d(pa,pb,p) <= 0) {
            			if(Primitives.orient2d(pa1, pa, pb) >= 0) {
            				if( (Primitives.orient2d(pa1, pa, p) >= 0) && (Primitives.orient2d(pb,pb1,p) >= 0) ) {
            					return true;
            				}
            			}
            			else {
            				if( (Primitives.orient2d(pa1, pa, p) <= 0) && (Primitives.orient2d(pb,pb1,p) <= 0) ) {
            					return true;
            				}
            			}
            		}
            	}
            	else {
            		if(Primitives.orient2d(pa,pb,p) >= 0) {
            			if(Primitives.orient2d(pa1, pa, pb) >= 0) {
            				if( (Primitives.orient2d(pa1, pa, p) >= 0) && (Primitives.orient2d(pb,pb1,p) >= 0) ) {
            					return true;
            				}
            			}
            			else {
            				if( (Primitives.orient2d(pa1, pa, p) <= 0) && (Primitives.orient2d(pb,pb1,p) <= 0) ) {
            					return true;
            				}
            			}
            		}
            	}
            }
            return false;
	    }
	    
	    // Checks if point p lies on the segment joining pa and pb.
	    private boolean segmentTest(double[] pa, double[] pb, double[] p) {
	        if( (pa[0] == pb[0]) && (pa[1] == pb[1]) ) {
	            if( (pa[0] == p[0]) && (pa[1] == p[1]) ) {
	                return true;
	            }
	        }
	        else if(pa[0] == pb[0]) {
            	if( (((p[1] - pb[1])/(pa[1] - pb[1])) >= 0) && (((p[1] - pb[1])/(pa[1] - pb[1])) <= 1)
            	        && (p[0] == pa[0]) ) {
            		return true;
            	}
            }
            else if(pa[1] == pb[1]) {
            	if( (((p[0] - pb[0])/(pa[0] - pb[0])) >= 0) && (((p[0] - pb[0])/(pa[0] - pb[0])) <= 1)
            	        && (p[1] == pa[1]) ) {
            		return true;
            	}
            }
            else {
                if( (((p[1] - pb[1])/(pa[1] - pb[1])) >= 0) && (((p[1] - pb[1])/(pa[1] - pb[1])) <= 1) &&
                    (((p[0] - pb[0])/(pa[0] - pb[0])) >= 0) && (((p[0] - pb[0])/(pa[0] - pb[0])) <= 1) ) {
                    return true;
                }
            }
            return false;
	    }
	    
	    // Checks if point p is inside the triangle with vertices pa, pb, pc;
	    // if pa, pb, pc are collinear, checks if p lies on the segment passing through
	    // pa, pb and pc.
	    private boolean triangleTest(double[] pa, double[] pb, double[] pc, double[] p) {
	        if(Primitives.orient2d(pa, pb, pc) > 0) { // pa, pb, pc in counterclockwise order
                if((Primitives.orient2d(pa,pb,p) >= 0) && (Primitives.orient2d(pb,pc,p) >= 0) && (Primitives.orient2d(pc,pa,p) >= 0)) {
                        return true;
                }
            }
            if(Primitives.orient2d(pa, pb, pc) < 0) { // pa, pb, pc in clockwise order
                if((Primitives.orient2d(pa,pc,p) >= 0) && (Primitives.orient2d(pc,pb,p) >= 0) && (Primitives.orient2d(pb,pa,p) >= 0)) {
                        return true;
                }
            }
            if(Primitives.orient2d(pa, pb, pc) == 0) { // pa, pb, pc collinear
                // check if p lies on the line segment b/w pa and pb
                if(segmentTest(pa, pb, p) == true) {
                    return true;
                }
               	
               	// check if p lies on the line segment b/w pa and pc
                if(segmentTest(pa, pc, p) == true) {
                    return true;
                }
            }
            return false;
	    }
	    
	    // Checks if the circumcenter of the triangle lies inside the kernel of edge.
	    private boolean kernelTest(String edge, String triangle) {
	        String[] vertices = (triangle.trim()).split("\\s+");
	        double[] v1 = new double[2]; double[] v2 = new double[2];
	        double[] v3 = new double[2];
	        v1[0] = Double.parseDouble(vertices[0]); v1[1] = Double.parseDouble(vertices[1]);
	        v2[0] = Double.parseDouble(vertices[2]); v2[1] = Double.parseDouble(vertices[3]);
	        v3[0] = Double.parseDouble(vertices[4]); v3[1] = Double.parseDouble(vertices[5]);
	        double[] p = Primitives.computeCircumcenter(v1, v2, v3);
	        int edgeType = checkEdgeType(edge);
	        
	        double[] pa = new double[2]; double[] pb = new double[2];
	        String[] tokens = (edge.trim()).split("\\s+");
	        if(edgeType == 1) { // non-boundary edge
	            double[] pc; double[] pd; double[] pe = new double[2]; double[] pf = new double[2];
	            pa[0] = Double.parseDouble(tokens[0]); pa[1] = Double.parseDouble(tokens[1]);
	            pb[0] = Double.parseDouble(tokens[2]); pb[1] = Double.parseDouble(tokens[3]);
	            pe[0] = Double.parseDouble(tokens[4]); pe[1] = Double.parseDouble(tokens[5]);
	            pf[0] = Double.parseDouble(tokens[6]); pf[1] = Double.parseDouble(tokens[7]);
	            pc = Primitives.computeCircumcenter(pa, pb, pe);
                pd = Primitives.computeCircumcenter(pa, pb, pf);
                if( triangleTest(pa, pb, pc, p) == true ) {
                    return true;
                }
                if( triangleTest(pa, pb, pd, p) == true ) {
                    return true;
                }
	        }
	        if(edgeType == 2) { // boundary edge
	            double[] pc;
	            pa[0] = Double.parseDouble(tokens[0]); pa[1] = Double.parseDouble(tokens[1]);
	            pb[0] = Double.parseDouble(tokens[2]); pb[1] = Double.parseDouble(tokens[3]);
	            if(tokens[4].equals("inf") && tokens[5].equals("inf")) {
	                double[] pd = new double[2];
	                pd[0] = Double.parseDouble(tokens[6]); pd[1] = Double.parseDouble(tokens[7]);
	                pc = Primitives.computeCircumcenter(pa, pb, pd);
	            }
	            else {
	            //if(tokens[6].equals("inf") && tokens[7].equals("inf")) {
	                double[] pd = new double[2];
	                pd[0] = Double.parseDouble(tokens[4]); pd[1] = Double.parseDouble(tokens[5]);
	                pc = Primitives.computeCircumcenter(pa, pb, pd);
	            }
	            if( triangleTest(pa, pb, pc, p) == true ) {
	                return true;
	            }
                // Check other half of the kernel (the unbounded half)
                if (unboundedKernelTest(pa, pb, pc, p) == true) {
                    return true;
                }
            }    
	        if(edgeType == 3) { // edge with one endpoint at infinity
	            double[] pc = new double[2];
	            double[] pa1 = new double[2];
	            double[] pa2 = new double[2];
	            pa[0] = Double.parseDouble(tokens[0]); pa[1] = Double.parseDouble(tokens[1]);
	            pb[0] = Double.parseDouble(tokens[4]); pb[1] = Double.parseDouble(tokens[5]);
	            pc[0] = Double.parseDouble(tokens[6]); pc[1] = Double.parseDouble(tokens[7]);
	            if(pa[1] == pb[1]) {
	                pa1[0] = pa[0]; pa1[1] = pa[1] + 1;
	            }
	            else{
	                double slope = -(pa[0] - pb[0])/(pa[1] - pb[1]);
	                pa1[0] = pa[0] + 1; pa1[1] = pa[1] + slope;
	            }
	            if(pa[1] == pc[1]) {
	                pa2[0] = pa[0]; pa2[1] = pa[1] + 1;
	            }
	            else{
	                double slope = -(pa[0] - pc[0])/(pa[1] - pc[1]);
	                pa2[0] = pa[0] + 1; pa2[1] = pa[1] + slope;
	            }
	            if(Primitives.orient2d(pa1, pa, pb) >= 0) {
	                if(Primitives.orient2d(pa2, pa, pc) >= 0) {
	                    if( (Primitives.orient2d(pa1, pa, p) <= 0) && (Primitives.orient2d(pa2, pa, p) <= 0) ) {
	                        return true;
	                    }
	                }
	                else {
	                    if( (Primitives.orient2d(pa1, pa, p) <= 0) && (Primitives.orient2d(pa2, pa, p) >= 0) ) {
	                        return true;
	                    }
	                }
	            }
	            else {
	                if(Primitives.orient2d(pa2, pa, pc) >= 0) {
	                    if( (Primitives.orient2d(pa1, pa, p) >= 0) && (Primitives.orient2d(pa2, pa, p) <= 0) ) {
	                        return true;
	                    }
	                }
	                else {
	                    if( (Primitives.orient2d(pa1, pa, p) >= 0) && (Primitives.orient2d(pa2, pa, p) >= 0) ) {
	                        return true;
	                    }
	                }
	            }
	        }
	        return false;
	    }
	    
	    // Checks if point is in conflict with edge.
	    private boolean conflictTest(String edge, String point) {
            int edgeType = checkEdgeType(edge);
            double[] pa = new double[2]; double[] pb = new double[2];
            String[] tokens = (edge.trim()).split("\\s+");
            String[] tokens1 = (point.trim()).split("\\s+");
            double[] p = new double[2];
            p[0] = Double.parseDouble(tokens1[0]); p[1] = Double.parseDouble(tokens1[1]);
            if(edgeType == 1) { // non-boundary edge
	            double[] pc = new double[2]; double[] pd = new double[2];
	            pa[0] = Double.parseDouble(tokens[0]); pa[1] = Double.parseDouble(tokens[1]);
	            pb[0] = Double.parseDouble(tokens[2]); pb[1] = Double.parseDouble(tokens[3]);
	            pc[0] = Double.parseDouble(tokens[4]); pc[1] = Double.parseDouble(tokens[5]);
	            pd[0] = Double.parseDouble(tokens[6]); pd[1] = Double.parseDouble(tokens[7]);
	            ArrayList<double[]> pe = new ArrayList<double[]>();
//	            System.out.println("I am here ; edge " + pa[0] + " " + pa[1] + " " + pb[0] + " " + pb[1] 
//	                + " point " + p[0] + " " + p[1]);
                pe.add(pc); pe.add(pd);
                for(double[] q : pe) {
	                if(Primitives.orient2d(pa, pb, q) > 0) { // pa, pb, q in counterclockwise order
	                    if(Primitives.incircle(pa, pb, q, p) >= 0) {
	                        //System.out.println("I am here");
	                        return true;
	                    }
	                }
	                if(Primitives.orient2d(pa, pb, q) < 0) { // pa, pb, q in clockwise order
	                    if(Primitives.incircle(pa, pb, q, p) <= 0) {
	                        //System.out.println("I am here");
	                        return true;
	                    }
	                }
	                if(Primitives.orient2d(pa, pb, q) == 0) { // pa, pb, q collinear, their circumcircle
	                							   // is the line passing through them
	                    if(Primitives.orient2d(pa,pb,p) == 0) {
	                    	//System.out.println("I am here"); 
	                    	return true;
	                    }
	                }
	            }
            }
            if(edgeType == 2) { // boundary edge
	            double[] pc = new double[2];
	            pa[0] = Double.parseDouble(tokens[0]); pa[1] = Double.parseDouble(tokens[1]);
	            pb[0] = Double.parseDouble(tokens[2]); pb[1] = Double.parseDouble(tokens[3]);
	            if(tokens[4].equals("inf") && tokens[5].equals("inf")) {
	                pc[0] = Double.parseDouble(tokens[6]); pc[1] = Double.parseDouble(tokens[7]);
	            }
	            if(tokens[6].equals("inf") && tokens[7].equals("inf")) {
	                pc[0] = Double.parseDouble(tokens[4]); pc[1] = Double.parseDouble(tokens[5]);
	            }
	            if(Primitives.orient2d(pa, pb, pc) > 0) { // pa, pb, pc in counterclockwise order
                    if(Primitives.incircle(pa, pb, pc, p) >= 0) {
                        return true;
                    }
                }
                if(Primitives.orient2d(pa, pb, pc) < 0) { // pa, pb, pc in clockwise order
                    if(Primitives.incircle(pa, pb, pc, p) <= 0) {
                        return true;
                    }
                }
                if(Primitives.orient2d(pa, pb, pc) == 0) { // pa, pb, pc collinear, their circumcircle
                								// is the line passing through them
                    if(Primitives.orient2d(pa,pb,p) == 0) {
	                    	return true;
	                }
                }
                // Check other half of the conflict space (the unbounded half)
                if(Primitives.orient2d(pa, pb, pc) >= 0) {
                	if(Primitives.orient2d(pa, pb, p) <= 0) {
                		return true;
                	}
                }
                else {
                	if(Primitives.orient2d(pa, pb, p) >= 0) {
                		return true;
                	}
                }
            }
            if(edgeType == 3) { // edge with one endpoint at infinity
	            double[] pc = new double[2];
	            if(tokens[0].equals("inf") && tokens[1].equals("inf")) {
	            	pa[0] = Double.parseDouble(tokens[2]); pa[1] = Double.parseDouble(tokens[3]);
	            }
	            if(tokens[2].equals("inf") && tokens[3].equals("inf")) {
	            	pa[0] = Double.parseDouble(tokens[0]); pa[1] = Double.parseDouble(tokens[1]);
	            }
	            pb[0] = Double.parseDouble(tokens[4]); pb[1] = Double.parseDouble(tokens[5]);
	            pc[0] = Double.parseDouble(tokens[6]); pc[1] = Double.parseDouble(tokens[7]);
	            if(Primitives.orient2d(pa, pb, pc) >= 0) {
	            	if(Primitives.orient2d(pa, pb, p) <= 0) {
	            		return true;
	            	}
	            	if(Primitives.orient2d(pa, p, pc) <= 0) {
	            		return true;
	            	}
	            }
	            else {
	            	if(Primitives.orient2d(pa, pb, p) >= 0) {
	            		return true;
	            	}
	            	if(Primitives.orient2d(pa, p, pc) >= 0) {
	            		return true;
	            	}
	            }
            }
            return false;
	    }
	    
	    // Function that returns true iff (x1,y1) is lexicographically smaller than or equal to (x2,y2).
	    private boolean lessThan(double x1, double y1, double x2, double y2) {
	        if(x1 < x2) {
	            return true;
	        }
            if(x1 > x2) {
                return false;
            }
            if(y1 <= y2) {
                return true;
            }
            return false;
	    }
	    
	    // Rewrite a triangle representation of a triangulation to an edge representation,
	    // each edge also storing the vertices of its two adjacent triangles.
	    // There are three kinds of edges - non-boundary edges, boundary edges, and edges whose one endpoint
	    // is the point at infinity, represented as "inf inf".
	    private ArrayList<String> formatTriangulation(ArrayList<String> triangles) {
	        ArrayList<String> edges = new ArrayList<String>();
	        HashMap<String,ArrayList<String>> temp = new HashMap<String,ArrayList<String>>();
	        double x1,y1,x2,y2,x3,y3;
	        String[] vertices = new String[6];
	        for(String triangle : triangles) {
	            vertices = (triangle.trim()).split("\\s+");
	            x1 = Double.parseDouble(vertices[0]); y1 = Double.parseDouble(vertices[1]);
	            x2 = Double.parseDouble(vertices[2]); y2 = Double.parseDouble(vertices[3]);
	            x3 = Double.parseDouble(vertices[4]); y3 = Double.parseDouble(vertices[5]);
	            if(lessThan(x1, y1, x2, y2)) {
	                if(!temp.containsKey(x1 + " " + y1 + " " + x2 + " " + y2)) {
	                    temp.put(x1 + " " + y1 + " " + x2 + " " + y2, new ArrayList<String>()); 
	                }
	                (temp.get(x1 + " " + y1 + " " + x2 + " " + y2)).add(x3 + " " + y3);
	            }
	            else {
	                if(!temp.containsKey(x2 + " " + y2 + " " + x1 + " " + y1)) {
	                    temp.put(x2 + " " + y2 + " " + x1 + " " + y1, new ArrayList<String>()); 
	                }
	                (temp.get(x2 + " " + y2 + " " + x1 + " " + y1)).add(x3 + " " + y3);
	            }
	            if(lessThan(x1, y1, x3, y3)) {
	                if(!temp.containsKey(x1 + " " + y1 + " " + x3 + " " + y3)) {
	                    temp.put(x1 + " " + y1 + " " + x3 + " " + y3, new ArrayList<String>()); 
	                }
	                (temp.get(x1 + " " + y1 + " " + x3 + " " + y3)).add(x2 + " " + y2);
	            }
	            else {
	                if(!temp.containsKey(x3 + " " + y3 + " " + x1 + " " + y1)) {
	                    temp.put(x3 + " " + y3 + " " + x1 + " " + y1, new ArrayList<String>()); 
	                }
	                (temp.get(x3 + " " + y3 + " " + x1 + " " + y1)).add(x2 + " " + y2);
	            }
	            if(lessThan(x3, y3, x2, y2)) {
	                if(!temp.containsKey(x3 + " " + y3 + " " + x2 + " " + y2)) {
	                    temp.put(x3 + " " + y3 + " " + x2 + " " + y2, new ArrayList<String>()); 
	                }
	                (temp.get(x3 + " " + y3 + " " + x2 + " " + y2)).add(x1 + " " + y1);
	            }
	            else {
	                if(!temp.containsKey(x2 + " " + y2 + " " + x3 + " " + y3)) {
	                    temp.put(x2 + " " + y2 + " " + x3 + " " + y3, new ArrayList<String>()); 
	                }
	                (temp.get(x2 + " " + y2 + " " + x3 + " " + y3)).add(x1 + " " + y1);
	            }
	        }
            
            ArrayList<String> boundaryEdges = new ArrayList<String>();
	        for(Map.Entry<String, ArrayList<String>> entry : temp.entrySet()) {
	            String edge = entry.getKey();
	            ArrayList<String> value = entry.getValue();
	            if(value.size() == 2) { // non-boundary edges
	                edges.add(edge + " " + value.get(0) + " " + value.get(1));
	            }
	            if(value.size() == 1) { // boundary edges
	                edges.add(edge + " " + value.get(0) + " inf inf");
	                boundaryEdges.add(edge);
	            }
	        }
	        
	        HashMap<String, ArrayList<String>> boundaryVertices = new HashMap<String, ArrayList<String>>();
	        for(String edge : boundaryEdges) {
	            String[] tokens = (edge.trim()).split("\\s+");
                if(!boundaryVertices.containsKey(tokens[0] + " " + tokens[1])) {
                    boundaryVertices.put(tokens[0] + " " + tokens[1], new ArrayList<String>());
                }
                (boundaryVertices.get(tokens[0] + " " + tokens[1])).add(tokens[2] + " " + tokens[3]);
                if(!boundaryVertices.containsKey(tokens[2] + " " + tokens[3])) {
                    boundaryVertices.put(tokens[2] + " " + tokens[3], new ArrayList<String>());
                }
                (boundaryVertices.get(tokens[2] + " " + tokens[3])).add(tokens[0] + " " + tokens[1]);
	        }
	        for(Map.Entry<String, ArrayList<String>> entry : boundaryVertices.entrySet()) {
	            String key = entry.getKey();
	            ArrayList<String> value = entry.getValue();
	            edges.add(key + " inf inf " + value.get(0) + " " + value.get(1));
	        }
	        return edges;
	    }
	    
	    // Function to compute a Delaunay triangulation locally, i.e., when the input points all reside in one peer.
	    // Uses triangle package of Shewchuk, and returns a list of triangles.
	    private ArrayList<String> LocalTriangulator(ArrayList<String> points,
                                            BSPPeer<LongWritable, Text, NullWritable, Text, Text> peer) 
	                                            throws IOException {
	        // Write the points to a file to be fed into the triangle program.
	        String inputFilenamePrefix = System.currentTimeMillis() + "" + (peer.getPeerName()).trim();
	        //System.out.println("inputFilenamePrefix is " + inputFilenamePrefix);
	        FileWriter fw = new FileWriter(inputFilenamePrefix + ".node");
	        int numPoints = points.size();
	        fw.write(numPoints + " 2 0 0");
	        fw.write(System.lineSeparator());
	        numPoints = 1;
	        for(String temp : points) {
	            fw.write(numPoints + " " + temp);
	            fw.write(System.lineSeparator());
	            numPoints += 1;
	        }
	        fw.close();
	        // System.out.println("HELLOO RUNNING ./triangle " + inputFilenamePrefix + ".node **************");
	        Process p = Runtime.getRuntime().exec("/home/abhinandan/hama-0.7.1/DelaunayTriangulator/triangle " + inputFilenamePrefix + ".node" );
	        //System.out.println("RUNNING ./triangle " + inputFilenamePrefix + ".node **************");
	        BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
	        String line;
	        while ((line = is.readLine()) != null)
                System.out.println(line);
                  //continue; 
             
            is.close();
            
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getErrorStream()));
	        while ((line = in.readLine()) != null)
                System.out.println(line); //continue; //
                  
            try {
                p.waitFor();  // wait for process to complete
            } catch (InterruptedException e) {
                System.err.println(e);  // "Can'tHappen"
              return null;
            }
            in.close();
	        
	        //Runtime.getRuntime().exec("./triangle " + inputFilenamePrefix + ".node");
	        //Runtime.getRuntime().exec("echo running ***************** ************");
	        //System.out.println(Shell.execCommand("/home/abhinandan/hama-0.7.1/DelaunayTriangulator/triangle" + 
	            //" /home/abhinandan/hama-0.7.1/DelaunayTriangulator/" + inputFilenamePrefix + ".node"));
	        //System.out.println(Shell.execCommand("ls > foobar"));
	        //String[] args = {"/bin/bash", "-c", "\"/home/abhinandan/hama-0.7.1/DelaunayTriangulator/triangle " + 
	        //                    "/home/abhinandan/hama-0.7.1/DelaunayTriangulator/" + inputFilenamePrefix +".node\""};
//	        String[] args = {"pwd"};
//	        Runtime.getRuntime().exec(args);
	        
	        //ShellCommandExecutor shexec = new ShellCommandExecutor(args);
	        //shexec.execute();
	        
	        // Read output file of the triangle program.
	        String outputFilename = inputFilenamePrefix +".1.ele";
	        BufferedReader br = new BufferedReader(new FileReader(outputFilename));
	        ArrayList<String> triangles = new ArrayList<String>();
	        String temp = null; int tri1, tri2, tri3; String[] tokens = new String[4];
	        br.readLine(); // skip first line
	        temp = br.readLine();
	        while(temp != null) {
                if(temp.charAt(0) == '#') {
                    temp = br.readLine();
                    continue;
                }
                tokens = (temp.trim()).split("\\s+");
                tri1 = Integer.parseInt(tokens[1]); tri2 = Integer.parseInt(tokens[2]); tri3 = Integer.parseInt(tokens[3]);
                triangles.add(points.get(tri1-1) + " " + points.get(tri2-1) + " " + points.get(tri3-1));
                temp = br.readLine();
            }
            br.close();
            return triangles;
	    }
	    
	    // Strip off the id from an id-augmented edge.
	    private String strip(String edge) {
            //System.out.println("Inside strip edge is " + edge);
	        String[] tokens = (edge.trim()).split("\\s+");
	        return new String(tokens[1] + " " + tokens[2] + " " + tokens[3] + " " + tokens[4] + " " + 
	                            tokens[5] + " " + tokens[6] + " " + tokens[7] + " " + tokens[8]);
	    }
	    
	    // Get the id from an id-augmented edge.
	    private int getId(String edge) {
	        String[] tokens = (edge.trim()).split("\\s+");
	        return Integer.parseInt(tokens[0]);
	    }
	    
	    // Get the id-augmented edge with a given id from a list of id-augmented edges, return null for error.
	    private String getEdge(ArrayList<String> edges, int id) {
	        for(String edge : edges) {
	            if(getId(edge) == id) {
	                String retval = new String(edge);
	                return retval;
	            }
	        }
	        return null;
	    }
	    
	    // Function to get the number of active peers; the job ends when this value is zero.
	    // Needed so that each peer calls sync() the same no. of times and hence syncException is prevented.
	    private int getNumActivePeers(BSPPeer<LongWritable, Text, NullWritable, Text, Text> peer, int isActive) 
                            throws IOException, SyncException, InterruptedException {
	        for( String machine : peer.getAllPeerNames() ) {
	            peer.send(machine, new Text(String.valueOf(isActive)));
	        }
	        peer.sync();
	        
	        int result = 0;
	        Text received = null;
	        while( (received = (Text) peer.getCurrentMessage()) != null ) {
	            result += Integer.parseInt(received.toString());
	        }
	        return result;
	    }
	    
	    // Function to compute Delaunay Triangulation of points residing
	    // in machines, and report those triangles whose circumcenters lie in kernel of edges in conflictEdges.
	    private void Triangulator(ArrayList<String> machines, String master, ArrayList<String> inputPoints,
	            ArrayList<String> conflictEdges, BSPPeer<LongWritable, Text, NullWritable, Text, Text> peer) 
	                    throws IOException, SyncException, InterruptedException {
            //System.out.println("Peer " + peer.getPeerName() + " has " + inputPoints.size() + " points");
            int numActivePeers = peer.getNumPeers();
            int isActive = 1;
            int counter = 0;
            Random rand = new Random();
            while (numActivePeers > 0) {
                counter += 1;
                if(isActive == 1) {
                    System.out.println("The no. of input points for peer " + peer.getPeerName() + " in iteration " + counter + " is " + inputPoints.size());
	                if(machines.size() == 1) {
	                    // Compute Delaunay triangulation locally, and output a triangle if its circumcenter lies inside the
	                    // kernel of each edge in conflictEdges.
	                    if(inputPoints.size() <= 3) {
	                       String result = "";
	                       for(String temp : inputPoints) {
	                        result += " " + temp;
	                       }
	                       peer.write(null, new Text(result));
	                    }
	                    else {
	                        ArrayList<String> triangles = LocalTriangulator(inputPoints, peer);
	                        boolean result = true;
	                        for(String triangle : triangles) {
	                            for(String edge : conflictEdges) {
	                                result = result && kernelTest(edge, triangle);
	                            }
	                            if(result == true) {
	                                peer.write(null, new Text(triangle));
	                            }
	                            triangle = null;
	                            result = true;
	                        }
	                    }
	                    // Peer is now inactive after computing the triangulation.
	                    isActive = 0;
	                    inputPoints.clear();
	                    inputPoints = null;
	                    System.gc();
	                }
	            }
                
                if(isActive == 1) {
	                // Each machine sends no. of input points it has to the master machine.
	                peer.send(master, new Text(String.valueOf(inputPoints.size())));
	            }
	            
	            // Each peer has to sync the same no. of times, regardless of whether it is active or not.
                peer.sync();

	            if(isActive == 1) {
	                // Master machine collects no. of points from each machine and computes the total no. of points,
	                // then sends it to every machine in machines.
                    if(master.equals(peer.getPeerName())) {
                        int sum = 0;
                        Text received = null;
                        while((received = (Text) peer.getCurrentMessage()) != null) {
                            sum += Integer.parseInt(received.toString());
                        }
                        for(String machine : machines) {
                            peer.send(machine, new Text(String.valueOf(sum)));
                        }
                        //System.out.println("Total for master " + master + "in iteration " + counter + " is " + sum);
                    }
                }
                
                peer.sync();
                
                int total = 0;
                if(isActive == 1) {
                    // Each machine receives total no. of points from master task.
                    total = Integer.parseInt(((Text) peer.getCurrentMessage()).toString());
                    // System.out.println("Total for machine " + peer.getPeerName() + " is " + total);
                    // return;
                    
                    // Special cases : either total number of points is very less, or #machines has dropped below #edges in DTReSampledPoints.
                    // In either case, send everything to master for local computation.
                    if((total <= 6640) || ( (machines.size() != 1) && (machines.size() < 45) )) {
                        for(String point : inputPoints) {
                            peer.send(master, new Text(point));
                        }
                        // Become inactive if you are not the master machine.
                        if(!master.equals(peer.getPeerName())) {
                            isActive = 0;
                            inputPoints.clear();
                            inputPoints = null;
                            System.gc();
                        }
                    }
                }
                
                peer.sync();
                        
                if(isActive == 1) {
                    if( (total <= 6640) || ( (machines.size() != 1) && (machines.size() < 45) ) ) {
                        // Master machine receives all the points.
                        if(master.equals(peer.getPeerName())) {
                            Text received = null;
                            ArrayList<String> points = new ArrayList<String>();
                            while((received = (Text) peer.getCurrentMessage()) != null) {
                                points.add(received.toString());
                            }
                            if(points.size() <= 3) {
	                            String result = "";
	                            for(String temp : points) {
	                                result += " " + temp;
	                            }
	                            peer.write(null, new Text(result));
	                        }
	                        else {
                                ArrayList<String> triangles = LocalTriangulator(points, peer);
                                boolean result = true;
                                for(String triangle : triangles) {
                                    for(String edge : conflictEdges) {
                                        result = result && kernelTest(edge, triangle);
                                    }
                                    if(result == true) {
                                        peer.write(null, new Text(triangle));
                                    }
                                    result = true;
                                }
                            }
                        }
                        // Become inactive after computing locally.
                        isActive = 0;
                        inputPoints.clear();
                        inputPoints = null;
                        System.gc();
                    }
                }
                
                if(isActive == 1) {
                    // Sample each input point with probability = (sqrt. of space)/total ~ 50/total : we have s ~ 2000
                    // and send it to the master.
        //            Random rand = new Random();
        //            for(String point : inputPoints) {
        //                if(rand.nextInt(total/50) == 0) {
        //                    peer.send(master, new Text(point));
        //                }
        //            }
        //            peer.sync();
                    // Randomly permute the inputPoints and take the prefix of the new list.
                    Collections.shuffle(inputPoints);
                    for(int i = 0; i < 25*(inputPoints.size()/total + ((inputPoints.size()%total == 0)?0:1)); i++) {
                        peer.send(master, new Text(inputPoints.get(i)));
                    }
                }
                
                peer.sync();
                
                if(isActive == 1) {
                    // Master machine collects all sampled points, and does two-level sampling.
                    if(master.equals(peer.getPeerName())) {
                        ArrayList<String> sampledPoints = new ArrayList<String>();
                        Text temp = null;
                        while((temp = (Text) peer.getCurrentMessage()) != null) {
                            sampledPoints.add(temp.toString());
                        }
                        // Master does two-level sampling on the received points.
                        // First level of sampling, each point is sampled with probability r/total.
                        total = sampledPoints.size();
                        ArrayList<String> reSampledPoints = new ArrayList<String>();
        //                for(String temp1 : sampledPoints) {
        //                    if(rand.nextInt(total/7) == 0) { //using r^2 = \sqrt{s} instead of \sqrt{s/ln n} = 1.52
        //                        reSampledPoints.add(temp1);
        //                    }
        //                }
                        // Randomly shuffle the sampledPoints.
                        Collections.shuffle(sampledPoints);
                        for(int i = 0; i < 16; i++) {
                            reSampledPoints.add(sampledPoints.get(i));
                        }
                        sampledPoints.clear();
                        sampledPoints = null;
                        System.gc();
                        // Compute Delaunay triangulation of reSampledPoints.
                        ArrayList<String> DTReSampledPoints = LocalTriangulator(reSampledPoints, peer);
                        ArrayList<String> edgesDTReSampledPoints = formatTriangulation(DTReSampledPoints);
                        // for the moment skipping the second level sampling -- MAYBE TO BE ADDED
                        // Master sends the triangulation to all the machines.
                        int index = 0;
                        for(String edge : edgesDTReSampledPoints) {
                            for(String machine : machines) {
                                peer.send(machine, new Text(index + " " + edge)); // Including an id in front of each
                                                                                  // edge, to be used later for partitioning.
                            }
                            //System.out.println("DT edge " + index + " " + edge);
                            index += 1;
                        }
                        //edgesDTReSampledPoints.clear();
                        
                    }
                }
                peer.sync();
                
                int numMachines = machines.size();
                int numEdges = 0;
                ArrayList<String> edgesDTReSampledPoints = null;
                if(isActive == 1) {
                    // Each machine receives Delaunay triangulation of reSampledPoints.
                    edgesDTReSampledPoints = new ArrayList<String>();
                    Text temp = null;

                    while((temp = (Text) peer.getCurrentMessage()) != null) {
                            edgesDTReSampledPoints.add(temp.toString());
                    }
                    
                    // Each machine computes the conflict list for each edge of the triangulation.
                    HashMap<String, ArrayList<String>> conflictList = new HashMap<String, ArrayList<String>>();
                    for(String edge : edgesDTReSampledPoints) {
                        String strippedEdge = strip(edge); // Stripping off id from edge for conflict testing purposes.
                        for(String point : inputPoints) {
                            if(conflictTest(strippedEdge, point) == true) { 
                                if(!conflictList.containsKey(edge)) {
                                    conflictList.put(edge, new ArrayList<String>());
                                }
                                (conflictList.get(edge)).add(point);
                            }
                        }
//                        if (conflictList.get(edge) == null) {
//                            System.out.println("Empty list for edge " + getId(edge) + " which is edgeType " + checkEdgeType(strippedEdge));
//                        }
                    }
                    
                    // Assume the edges are ordered as 0,1,2,... and the position in this ordering
                    // is given by the id in each edge string.
                    // The conflict lists are distributed to the machines for the next iteration,
                    // each machine receiving part of the conflict list for at most one edge.
                    numEdges = edgesDTReSampledPoints.size();
                    int numMachinesPerEdge = numMachines / numEdges;
                    // System.out.println("nummachinesperedg is " + numMachinesPerEdge);
                    // System.out.println("numMachines = " + numMachines + " numEdges = " + numEdges);
                    //Random rand = new Random();
                    for(Map.Entry<String, ArrayList<String>> entry : conflictList.entrySet()) {
                        String edge = entry.getKey();
                        int id = getId(edge);
                        ArrayList<String> points = entry.getValue();
                        for(String point : points) {
                            //First handle the case where numEdges is very large, almost comparable to numMachines.
                            if(numEdges >= numMachines/2) {
                                peer.send(machines.get(id), new Text(point));
                            }
                            else {
                                //Send the point to a machine randomly selected from the machines assigned to the particular edge.
                                int base = id * numMachinesPerEdge;
                                int offset = rand.nextInt(numMachinesPerEdge);
                                //System.out.println("Edge id " + id + " base " + base + " offset " + offset);
                                peer.send(machines.get(base + offset), new Text(point));
                            }
                        }
                        points.clear();
                        points = null;
                    }
                    conflictList = null;
                    System.gc();
                }
                peer.sync();
                
                if(isActive == 1) {
                    // Input for the next recursive call.
                    ArrayList<String> newInputPoints = new ArrayList<String>();
                    // Receive new input points.
                    Text temp = null;
                    while((temp = (Text) peer.getCurrentMessage()) != null) {
                        newInputPoints.add(temp.toString());
                    }
                    
                    // Call recursive procedure with appropriate parameters.
                    // First handle the case where numEdges is very large, almost comparable to numMachines.
                    if(numEdges >= numMachines/2) {
                        int index = machines.indexOf(peer.getPeerName());
                        if(index >= numEdges) { // Machine is not involved in future rounds.
                            isActive = 0;
                            inputPoints.clear();
                            inputPoints = null;
                            System.gc();
                        }
                        else {
                            String newEdge = getEdge(edgesDTReSampledPoints, index);
                            String newStrippedEdge = strip(newEdge);
                            conflictEdges.add(newStrippedEdge);
                            ArrayList<String> listOfOneMachine = new ArrayList<String>();
                            listOfOneMachine.add(peer.getPeerName());
                            //Triangulator(listOfOneMachine, peer.getPeerName(), newInputPoints, conflictEdges, peer);
                            // New values for the next round of iteration.
                            machines = listOfOneMachine;
                            master = peer.getPeerName();
                            inputPoints.clear();
                            inputPoints = null;
                            System.gc();
                            inputPoints = newInputPoints;
                        }
                    }
                    else {
                        int index = machines.indexOf(peer.getPeerName());
                        int numMachinesNew = numMachines - (numMachines % numEdges);
                        if( index >= numMachinesNew ) { // Current peer does not belong to any group.
                            isActive = 0;
                            inputPoints.clear();
                            inputPoints = null;
                            System.gc();
                        }
                        else {
                            int numMachinesPerEdgeNew = numMachinesNew / numEdges;
                            // Which group of machines the current peer belongs to
                            int group = index / numMachinesPerEdgeNew; // This is also equal to the id of the new edge added to conflictEdges
                            //System.out.println("index = " + group + " , numEdges = " + numEdges);
                            String newEdge = getEdge(edgesDTReSampledPoints, group);
                            //System.out.println("Sending edge " + newEdge + " to strip");
                            String newStrippedEdge = strip(newEdge);
                            conflictEdges.add(newStrippedEdge);            
                            //Triangulator(new ArrayList<String>(machines.subList(group * numMachinesPerEdge, (group+1) * numMachinesPerEdge - 1)),
                            //        machines.get(group * numMachinesPerEdge), newInputPoints, conflictEdges, peer);
                            // New values for the next round.
                            master = machines.get(group * numMachinesPerEdgeNew);
                            inputPoints.clear();
                            inputPoints = null;
                            System.gc();
                            inputPoints = newInputPoints;
                            machines = new ArrayList<String>(machines.subList(group * numMachinesPerEdgeNew, (group+1) * numMachinesPerEdgeNew));
                        }
                    }
                }
                numActivePeers = getNumActivePeers(peer, isActive);
            }
	    }
    }


    public static void main(String[] args)
	        throws InterruptedException, IOException, ClassNotFoundException {

        System.setProperty("java.library.path", "/home/abhinandan/hama-0.7.1/DelaunayTriangulator/");
	    HamaConfiguration conf = new HamaConfiguration();
	    conf.setBoolean("bsp.input.runtime.partitioning", false);
	    BSPJob job = new BSPJob(conf, DelaunayTriangulator.class);
	    job.setJobName("Delaunay Triangulation");
	    job.setBspClass(DelaunayTriangulatorBSP.class);
	    job.setInputPath(new Path("medium_input_nd_45"));
	    job.setInputFormat(TextInputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputPath(new Path("medium_output_nd_45"));
        //job.setPartitioner(HashPartitioner.class);
        //job.setOutputFormat(NullOutputFormat.class);
        //job.setBoolean("bsp.input.runtime.partitioning", true);

        BSPJobClient jobClient = new BSPJobClient(conf);
        ClusterStatus cluster = jobClient.getClusterStatus(true);
    
        // set number of tasks
        job.setNumBspTask(45);
        // job.setNumBspTask(cluster.getMaxTasks());
        // System.out.println("Max number of tasks is " + cluster.getMaxTasks());
        job.waitForCompletion(true);

    //    FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT);

 

    //    if (args.length > 0) {
    //      bsp.setNumBspTask(Integer.parseInt(args[0]));
    //    } else {
    //      // Set to maximum
    //      bsp.setNumBspTask(cluster.getMaxTasks());
    //    }

    //    long startTime = System.currentTimeMillis();
    //    if (bsp.waitForCompletion(true)) {
    //      printOutput(conf);
    //      System.out.println("Job Finished in "
    //          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    //    }
    }
}

