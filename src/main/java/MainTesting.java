import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class MainTesting
{

	public static DataFrame<Double> testDataFrame()
	{
		List<String> colNames = Arrays.asList("year", "revenue", "costs");
		double[][] data = { { 2015, 70021.35, 25071.12 },
				            { 2016, 67008.12, 108632.80 },
				            { 2017, 10632.83, 37816.8 },
				            { 2018, 85216.33, 31863.73 } };
		return new DoubleDataFrame(colNames, data);
	}

	public static DataFrame<Double> sampledDataFrame()
	{
		List<String> colNames;
		colNames = Arrays.asList("uniform1", "uniform2", "normal1", "normal2", "exponential");
		double[][] data = { { 15.20, 11.75,  0.487, 1.646, 1.006 },
							{ 19.46, 12.82,  0.616, 0.865, 8.313 },
							{ 11.69, 10.61, -0.148, 0.402, 3.811 },
							{ 17.56, 10.62, -0.733, 0.925, 0.986 },
							{ 14.18, 11.59, -0.641, 0.860, 0.270 },
							{ 10.10, 15.87,  1.610, 1.480, 1.523 },
							{ 16.91, 18.55, -1.121, 1.477, 1.951 },
							{ 11.99, 13.24, 0.981, -0.243, 4.460 },
							{ 14.69, 16.02, 0.200, -0.648, 1.733 },
							{ 17.95, 13.25, 0.030, -1.493, 3.377 }};
		return new DoubleDataFrame(colNames, data);
	}

	public static void main(String[] args) throws IOException
	{
		dataFrameCore();
		dataFrameCoreSpeed();
		dataVectors();
		testRestructure();
		testAnalysis();
		// Extension 1
//		testRandom();
		// Extension 2
//		testStatisticsPart1();
//		testStatisticsPart2();
		// Extension 3
//		testPlotting();
		// Extension 4
//		testExcelIO();
	}

	public static void dataFrameCore()
	{
		List<String> colNames = Arrays.asList("year", "revenue", "costs");
		double[][] data = { { 2015, 70021.35, 25071.12 },
				{ 2016, 67008.12, 108632.80 },
				{ 2017, 10632.83, 37816.8 },
				{ 2018, 85216.33, 31863.73 } };
		DataFrame<Double> df = new DoubleDataFrame(colNames, data);
		System.out.println("Number of rows: " + df.getRowCount());
		System.out.println("Number of columns: " + df.getColumnCount());
		System.out.println(df.getValue(1, "year"));
		df.print();
		df.setValue(2, "revenue", 0d);
		df.print();
	}

	public static void dataFrameCoreSpeed()
	{
		int size = 10000;
		double[][] data = new double[1][size];
		List<String> header = new ArrayList<>(size);
		for (int j = 0; j < size; j++)
		{
			data[0][j] = j;
			header.add("x_" + j);
		}
		DoubleDataFrame df = new DoubleDataFrame(header, data);
		long time = System.currentTimeMillis();
		for (int j = 0; j < size; j++)
		{
			df.getValue(0, header.get(j));
			df.setValue(0, header.get(j), 0d);
		}
		time = System.currentTimeMillis() - time;
		System.out.println("Running time: " + time + "ms");
	}

	public static void dataVectors()
	{
		DataFrame<Double> df = testDataFrame();
		DataVector<Double> row = df.getRow(1);
		System.out.println(row.getName());
		System.out.println(row.getEntryNames());
		System.out.println(row.getValue("costs").equals(df.getValue(1, "costs")));
		System.out.println(row.getValues());
		System.out.println(row.asMap());
		System.out.println();

		DataVector<Double> col = df.getColumn("costs");
		System.out.println(col.getName());
		System.out.println(col.getEntryNames());
		System.out.println(col.getValue("row_1").equals(df.getValue(1, "costs")));
		System.out.println(col.getValues());
		System.out.println(col.asMap());
		System.out.println();

		System.out.println(df.getColumns().size() == df.getColumnCount());
		System.out.println(df.getRows().size() == df.getRowCount());
		for (DataVector<Double> vec : df)
		{
			System.out.println(vec.getName());
		}
	}

	public static void testRestructure()
	{
		DataFrame<Double> df = testDataFrame();
		DataFrame<Double> bigger = df.expand(1, "profit", "loss");
		bigger.print();
		bigger.setValue(1, "costs", 0d);
		System.out.println(!df.getValue(1, "costs").equals(0d));

		DataFrame<Double> smaller1 = df.project("year", "costs");
		smaller1.print();
		smaller1.setValue(1, "costs", 0d);
		System.out.println(!df.getValue(1, "costs").equals(0d));

		DataFrame<Double> smaller2 = df.select(row -> !row.getValue("year").equals(2017d));
		smaller2.print();
		smaller2.setValue(1, "costs", 0d);
		System.out.println(!df.getValue(1, "costs").equals(0d));
	}

	public static void testAnalysis()
	{
		DataFrame<Double> df = testDataFrame();
		Function<DataVector<Double>, Double> profitFunction;
		profitFunction = row -> row.getValue("revenue") - row.getValue("costs");
		DataFrame<Double> df2 = df.computeColumn("profit", profitFunction);
		df2.print();

		BinaryOperator<Double> sumOp = Double::sum;
		DataVector<Double> dv = df2.summarize("sum", sumOp);
		dv.print();
		df2.summarize("max", Math::max).print();
		df2.summarize("min", Math::min).print();
	}

//	public static void testRandom()
//	{
//		int rows = 10;
//		RandomTools rt = RandomTools.uniform(10, 20);
//		DataFrame<Double> df;
//		df = rt.generate(12345, rows, Arrays.asList("uniform1", "uniform2"));
//		rt = RandomTools.gaussian(0, 1);
//		df = df.concat(rt.generate(54321, rows, Arrays.asList("normal1", "normal2")));
//		rt = RandomTools.exponential(5);
//		df = df.concat(rt.generate(1337, rows, Arrays.asList("exponential")));
//		df.print();
//	}

	public static void testStatisticsPart1()
	{
		DataFrame<Double> df = sampledDataFrame();
		df = df.computeColumn("neg", row -> -row.getValue("normal1"));
		DataFrameStatistics stats = df.statistics();
		System.out.println(stats.tTest("normal1", 0));
		System.out.println(stats.tTest("normal1", "normal2"));
		System.out.println(stats.pearsonsCorrelation("normal1", "neg"));
		System.out.println(stats.pearsonsCorrelation("uniform1", "normal1"));
		System.out.println(stats.describe("exponential"));
	}

	public static void testStatisticsPart2()
	{
		DataFrame<Double> df = sampledDataFrame();
		Function<DataVector<Double>, Double> f;
		f = row -> 17 * row.getValue("uniform1") + 3 * row.getValue("uniform2")
				+ row.getValue("normal1");
		df = df.computeColumn("dep", f);
		Map<String, Double> model;
		model = df.statistics().estimateLinearModel("dep", "uniform1", "uniform2");
		System.out.println(model);
	}

	public static void testPlotting()
	{
		DataFrame<Double> df = sampledDataFrame();
		df.plotting().showScatter("My Fancy Plot", "uniform1", "normal1");
		df.plotting().showHistogram("A Histogram", "exponential", 5);
		try
		{
			File scatter = new File("scatter.png");
			File histogram = new File("histogram.png");
			df.plotting().saveScatter(scatter, "My Fancy Plot", "uniform1", "normal1");
			df.plotting().saveHistogram(histogram, "A Histogram", "exponential", 5);
		} catch (IOException ex)
		{
			ex.printStackTrace();
		}
	}

//	public static void testExcelIO()
//	{
//		DataFrame<Double> df = testDataFrame();
//		try
//		{
//			File f = new File("mydata.xlsx");
//			FileTools.writeToXLSX(df, new File("mydata.xlsx"));
//			DataFrame<Double> read = FileTools.readFromXLSX(f);
//			read.print();
//		} catch (IOException ex)
//		{
//			ex.printStackTrace();
//		}
//	}

}
