import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.Serializable;
import org.apache.spark.api.java.function.Function;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
class Window implements Serializable {
	private static final long serialVersionUID = 1L;
	int lineid;
	 double point1x;
	 double point1y;
	 public Window(int identifier,double cordx, double cordy) {
		 lineid = identifier;
		 point1x = cordx;
		 point1y = cordy;
	 }

	public int getId() {
		return lineid;
	}
}

class BaseRectangle implements Serializable {

	private static final long serialVersionUID = 1L;
	private int UniqueId;
	private double CoordinateX1;
	private double CoordinateY1;
	private double CoordinateX2;
	private double CoordinateY2;

	public double GetCoordinateX1() {
		return this.CoordinateX1;
	}

	public double GetCoordinateY1() {
		return CoordinateY1;
	}

	public double GetCoordinateX2() {
		return CoordinateX2;
	}

	public double GetCoordinateY2() {
		return CoordinateY2;
	}

	public BaseRectangle(int id, double x1, double y1, double x2, double y2) {
		this.UniqueId = id;
		this.CoordinateX1 = Math.min(x1,x2);
		this.CoordinateY1 = Math.min(y1,y2);
		this.CoordinateX2 = Math.max(x1,x2);
		this.CoordinateY2 = Math.max(y1,y2);
	}

	public int GetUniqueId() {
		return this.UniqueId;
	}

	public String getIdToString() {
		return this.UniqueId + "";
	}

	public Boolean isIn(BaseRectangle rectA) {
		if (rectA == null) {
			return false;
		}
		if (
			((this.CoordinateX1 > rectA.CoordinateX1
			&& this.CoordinateX1 < rectA.CoordinateX2)
			|| (rectA.CoordinateX1 > this.CoordinateX1
			&& rectA.CoordinateX1 < this.CoordinateX2)
			|| (this.CoordinateX2 > rectA.CoordinateX1
			&& this.CoordinateX2 < rectA.CoordinateX2)
			|| (rectA.CoordinateX2 > this.CoordinateX1
			&& rectA.CoordinateX2 < this.CoordinateX2))
			&&
			((this.CoordinateY1 > rectA.CoordinateY1
			&& this.CoordinateY1 < rectA.CoordinateY2)
			|| (rectA.CoordinateY1 > this.CoordinateY1
			&& rectA.CoordinateY1 < this.CoordinateY2)
			|| (this.CoordinateY2 > rectA.CoordinateY1
			&& this.CoordinateY2 < rectA.CoordinateY2)
			|| (rectA.CoordinateY2 > this.CoordinateY1
			&& rectA.CoordinateY2 < this.CoordinateY2))
		) {
			return true;
		}
		else
			return false;
	}

	public boolean contains(double point1x, double point1y) {
		if (
				(point1x >= this.CoordinateX1
				&& point1x <= this.CoordinateX2)
				&& (point1y >= this.CoordinateY1
				&& point1y <= this.CoordinateY2)
		) {
			return true;
		}
		else return false;
	}
}

public class spatialjoinop implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void spatialjoin(JavaSparkContext context, String inputfile1, String inputfile2, String inputfile3, String outputfile1, String outputfile2) {
		
		JavaRDD<String> file1 = context.textFile(inputfile1);
		JavaRDD<BaseRectangle> rectA = file1.map(new Function<String, BaseRectangle>() {
    		private static final long serialVersionUID = 2L;
    		public BaseRectangle call(String line) {
    			double point1x = 0.0, point1y = 0.0, point2x = 0.0, point2y = 0.0;
    			int id = 0;
    			String[] RectangleArray = line.split(",");
    			id = Integer.parseInt(RectangleArray[0]);
    			point1x = Double.parseDouble(RectangleArray[1]);
    			point1y = Double.parseDouble(RectangleArray[2]);
    			point2x = Double.parseDouble(RectangleArray[3]);
    			point2y = Double.parseDouble(RectangleArray[4]);
    			BaseRectangle rectangle = new BaseRectangle(id, point1x, point1y, point2x, point2y);
    			return rectangle;
    		}
    	});
		JavaRDD<String> file2 = context.textFile(inputfile2);
		JavaRDD<BaseRectangle> rectB = file2.map(new Function<String, BaseRectangle>() {
    		private static final long serialVersionUID = 2L;
    		public BaseRectangle call(String line) {
    			double point1x = 0.0, point1y = 0.0, point2x = 0.0, point2y = 0.0;
    			int id = 0;
    			String[] RectangleArray = line.split(",");
    			id = Integer.parseInt(RectangleArray[0]);
    			point1x = Double.parseDouble(RectangleArray[1]);
    			point1y = Double.parseDouble(RectangleArray[2]);
    			point2x = Double.parseDouble(RectangleArray[3]);
    			point2y = Double.parseDouble(RectangleArray[4]);
    			BaseRectangle rectangle = new BaseRectangle(id, point1x, point1y, point2x, point2y);
    			return rectangle;
    		}
    	});
		final Broadcast<List<BaseRectangle>> BroadcastRectangleB = context.broadcast(rectB.collect());
		JavaRDD<String> file3 = context.textFile(inputfile3);
		JavaRDD<Window> pointslist = file3.map(new Function<String, Window>() {
    		private static final long serialVersionUID = 2L;
    		public Window call(String line) {
    			double point1x = 0.0, point1y = 0.0;
    			int id = 0;
    			String[] RectangleArray = line.split(",");
    			id = Integer.parseInt(RectangleArray[0]);
    			point1x = Double.parseDouble(RectangleArray[1]);
    			point1y = Double.parseDouble(RectangleArray[2]);
    			Window point = new Window(id, point1x, point1y);
    			return point;
    		}
    	});
		final Broadcast<List<Window>> BroadcastPointlist = context.broadcast(pointslist.collect());
		JavaRDD<Tuple2<Integer, List<Integer>>> RectangleRectresult = rectA.map(new Function<BaseRectangle, Tuple2<Integer, List<Integer>>>() {
					private static final long serialVersionUID = 1L;
					public Tuple2<Integer, List<Integer>> call(BaseRectangle RectangleA) {
						Integer RectangleID = RectangleA.GetUniqueId();
						int i = 0;
						ArrayList<BaseRectangle> RectangleListB = (ArrayList<BaseRectangle>) BroadcastRectangleB.value();
						List<Integer> RectanglelistID = new ArrayList<Integer>();
						while (i < RectangleListB.size()) {
							if (RectangleListB.get(i).isIn(RectangleA)) {
								RectanglelistID.add(RectangleListB.get(i).GetUniqueId());
							}
							i++;
						}
						Tuple2<Integer, List<Integer>> RectRectJoin = new Tuple2<Integer, List<Integer>>(RectangleID, RectanglelistID);
						return RectRectJoin;
					}
		});
		JavaRDD<Tuple2<Integer, List<Integer>>> RectanglePointresult = rectA.map(new Function<BaseRectangle, Tuple2<Integer, List<Integer>>>() {
					private static final long serialVersionUID = 1L;
					public Tuple2<Integer, List<Integer>> call(BaseRectangle RectangleA) {
						ArrayList <Window> Pointlist = (ArrayList <Window>) BroadcastPointlist.value();
						int i = 0;
						Integer RectangleID = RectangleA.GetUniqueId();
						List<Integer> PointlistID = new ArrayList<Integer>();
						while (i < Pointlist.size()) {
							if (RectangleA.contains(Pointlist.get(i).point1x,Pointlist.get(i).point1y)) {
								PointlistID.add(Pointlist.get(i).getId());
							}
							i++;
						}
						Tuple2< Integer, List <Integer> > RectPointJoin = new Tuple2 <Integer, List <Integer> >(RectangleID, PointlistID);
						return RectPointJoin;
				}
		});

		RectangleRectresult.map( new Function<Tuple2<Integer, List<Integer>>, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<Integer, List<Integer>> RectRectJoin) {
					Integer RectID;
					RectID = RectRectJoin._1;
					StringBuffer resultstring;
					resultstring = new StringBuffer(""+RectID);
					List<Integer> PointslistID;
					PointslistID = RectRectJoin._2;
					for (Iterator<Integer> i = PointslistID.iterator(); i.hasNext(); ) {
						resultstring= new StringBuffer(resultstring+","+i.next());
					}	
					return resultstring.toString();
			}
			
		}).coalesce(1).saveAsTextFile(outputfile1);
		RectanglePointresult.map(new Function<Tuple2<Integer, List<Integer>>, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<Integer, List<Integer>> RectPointJoin) {
					Integer RectangleID;
					RectangleID = RectPointJoin._1;
					StringBuffer resultstring;
					resultstring = new StringBuffer(""+RectangleID);
					List<Integer> PointsID;
					PointsID = RectPointJoin._2;
					for (Iterator<Integer> i = PointsID.iterator(); i.hasNext(); ) {
						resultstring= new StringBuffer(resultstring+","+i.next());
					}
					return resultstring.toString();
			}
			
		}).coalesce(1).saveAsTextFile(outputfile2);
		context.close();
	}
	
	public static void main( String[] args ) {
    	SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("spatialjoinop");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	spatialjoin(sc, "/home/karthikacmilan/Desktop/JoinQueryInput2.csv","/home/karthikacmilan/Desktop/JoinQueryInput1.csv", "/home/karthikacmilan/Desktop/JoinQueryInput3.csv", "/home/karthikacmilan/Desktop/JoinQueryOutput2.csv","/home/karthikacmilan/Desktop/JoinQueryOutput3.csv");
    }
}
