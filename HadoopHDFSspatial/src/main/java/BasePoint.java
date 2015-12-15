import java.io.Serializable;

public class BasePoint implements Serializable {

	private static final long serialVersionUID = 1L;

	public BasePoint(double x, double y) {
		this.CoordinateX = x;
		this.CoordinateY = y;
		this.UniqueId = hashCode();
	}

	private final double CoordinateX, CoordinateY;
	private final int UniqueId;

	public double GetXValue() { // modified function name
		return CoordinateX;
	}

	public double GetYValue() { // modified function name
		return CoordinateY;
	}

	public double GetDistance(BasePoint point) {
		double differencex = this.CoordinateX - point.CoordinateX;
		double differencey = this.CoordinateY - point.CoordinateY;
		return differencex * differencex + differencey * differencey;
	}

	public int GetUniqueId() { // modified function name
		return this.UniqueId;
	}

	// @Override
	// public boolean equals(Object obj) {
	// if (obj == this) {
	// return true;
	// }
	//
	// if (!(obj instanceof Point)) {
	// return false;
	// }
	//
	// Point point = (Point) obj;
	// return this.id == point.getId();
	// }

	@Override
	public String toString() { // need to check if its needed
		return Double.toString(CoordinateX) + "," + Double.toString(CoordinateY);
	}

	public boolean IsEqual(BasePoint point) { // modified function name
		double precision = 0.000001;
		return Math.abs((this.CoordinateX - point.CoordinateX)) < precision
				&& Math.abs((this.CoordinateY - point.CoordinateY)) < precision;
	}

	public void print() {
		System.out.println("\npoint hash:" + this.UniqueId);
		System.out.println("x=" + this.CoordinateX);
		System.out.println("y=" + this.CoordinateY);
	}
}
