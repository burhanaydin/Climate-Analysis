package climateAnalysis;

import java.util.ArrayList;
import java.util.List;

public class DataPool {

	public int ncols = 0;
	public int nrows = 0;
	public double xllCorner = 0.0;
	public double yllCorner = 0.0;
	public double cellsize = 0.0;
	public int nodata_value = 0;
	public List<Double[]> record = new ArrayList<Double[]>();
	public void clear() {
		this.ncols = 0;
		this.nrows = 0;
		this.xllCorner = 0.0;
		this.yllCorner = 0.0;
		this.cellsize = 0.0;
		this.nodata_value = 0;
		this.record.clear();

	}
	public String toString() {

		System.out.println(String.valueOf(ncols));
		System.out.println(String.valueOf(nrows));
		System.out.println(String.valueOf(xllCorner));
		System.out.println(String.valueOf(yllCorner));
		System.out.println(String.valueOf(cellsize));
		System.out.println(String.valueOf(nodata_value));
		for(Double[] rec : record) {
			  for (int x=0; x < rec.length ; x++) {
                     System.out.print(rec[x].doubleValue() + " ");
			  }
			  System.out.println();
		}
		return "";
	}
    public void copy(DataPool dp) {
    	this.ncols = dp.ncols;
    	this.nrows = dp.nrows;
    	this.xllCorner = dp.xllCorner;
    	this.yllCorner = dp.yllCorner;
    	this.cellsize = dp.cellsize;
    	this.nodata_value = dp.nodata_value;
    	this.record = new ArrayList<Double[]>(dp.record);
    }
}
