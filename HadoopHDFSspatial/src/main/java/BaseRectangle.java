import java.io.Serializable;

public class BaseRectangle implements Serializable {

	private static final long serialVersionUID = 1L;
	private int UniqueId;
	private double CoordinateX1;
	private double CoordinateY1;
	private double CoordinateX2;
	private double CoordinateY2;

	public double GetCoordinateX1() {
		return this.CoordinateX1;
	}

	public double CoordinateY1() {
		return CoordinateY1;
	}

	public double GetCoordinateX2() {
		return CoordinateX2;
	}

	public double GetCoordinateY2() {
		return CoordinateY2;
	}

	public String GetUniqueIdToString() {
		String UniqueIdString = UniqueId + "";
		return UniqueIdString;
	}

	public BaseRectangle(int id, double x1, double y1, double x2, double y2) {
		this.UniqueId = id;
		this.CoordinateX1 = Math.min(x1, x2);
		this.CoordinateY1 = Math.max(y1, y2);
		this.CoordinateX2 = Math.max(x1, x2);
		this.CoordinateY2 = Math.min(y1, y2);
	}

	public BaseRectangle(double x1, double y1, double x2, double y2) {
		this.UniqueId = hashCode();
		this.CoordinateX1 = Math.min(x1, x2);
		this.CoordinateY1 = Math.max(y1, y2);
		this.CoordinateX2 = Math.max(x1, x2);
		this.CoordinateY2 = Math.min(y1, y2);
	}

	public boolean ContainsRectangle(BaseRectangle rectangle) {

		if (rectangle == null) {
			return false;
		}
		return this.CoordinateX1 <= rectangle.CoordinateX1
			&& this.CoordinateY1 >= rectangle.CoordinateY1
			&& this.CoordinateX2 >= rectangle.CoordinateX2
			&& this.CoordinateY2 <= rectangle.CoordinateY2;
	}

	public boolean ContainsPoint(BasePoint point) {

		if (point == null) {
			return false;
		}
		return this.CoordinateX1 <= point.GetXValue()
			&& this.CoordinateY1 >= point.GetYValue()
			&& this.CoordinateX2 >= point.GetXValue()
			&& this.CoordinateY2 <= point.GetYValue();
	}

	@Override
	public String toString() {
		return Integer.toString(this.UniqueId);
	}

	public int GetUniqueId() {
		return this.UniqueId;
	}

	public boolean IsOverlap(BaseRectangle rectangle) {
		boolean f1 = between(rectangle.CoordinateX1, CoordinateX1, CoordinateX2)
				|| between(rectangle.CoordinateX2, CoordinateX1, CoordinateX2);
		boolean f2 = f1 || between(CoordinateX1, rectangle.CoordinateX1, rectangle.CoordinateX2)
				|| between(CoordinateX2, rectangle.CoordinateX1, rectangle.CoordinateX2);
		boolean f3 = between(rectangle.CoordinateY1, CoordinateY1, CoordinateY2)
				|| between(rectangle.CoordinateY2, CoordinateY1, CoordinateY2);
		boolean f4 = f3 || between(CoordinateY1, rectangle.CoordinateY1, rectangle.CoordinateY2)
				|| between(CoordinateY2, rectangle.CoordinateY1, rectangle.CoordinateY2);
		return f2 && f4;
	}

	public static boolean between(double x, double a, double b) {
		if (a <= b)
			return a <= x && x <= b;
		return b <= x && x <= a;
	}
}
