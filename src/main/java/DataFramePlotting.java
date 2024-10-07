import java.io.File;
import java.io.IOException;

import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;

/**
 * Interface that allows the user of a data frame to make scatter plots and
 * histograms.
 * 
 * @author Paul Bouman
 *
 */
public interface DataFramePlotting
{

	/**
	 * Produces a scatter plot of variable x against variable y
	 * 
	 * @param title the title of the chart
	 * @param xVar  the name of the column that is used as x values
	 * @param yVar  the name of the column that is used as y values
	 * @return
	 */
	public XYChart scatter(String title, String xVar, String yVar);

	/**
	 * Writes a scatter plot image to a PNG file
	 * 
	 * @param outputFile the file to which the plot will be written
	 * @param title      the title of the chart
	 * @param xVar       the name of the column that is used as x values
	 * @param yVar       the name of the column that is used as y values
	 * @throws IOException if an error occurs while writing the image
	 */
	public void saveScatter(File outputFile, String title, String xVar, String yVar) throws IOException;

	/**
	 * Produces a histogram bar chart for a single variable
	 * 
	 * @param title   the title of the chart
	 * @param varName the variable or column that should be used to compute the
	 *                histogram
	 * @param bins    the number of bins to use in the histogram
	 * @return a CategoryChart containing the histogram
	 */
	public CategoryChart histogram(String title, String varName, int bins);

	/**
	 * Writes a histogram plot image to a PNG file
	 * 
	 * @param outputFile the file to which the plot will be written
	 * @param title      the title of the chart
	 * @param varName    the variable or column that should be used to compute the
	 *                   histogram
	 * @param bins       the number of bins to use in the histogram
	 * @throws IOException if an error occurs while writing the image
	 */
	public void saveHistogram(File outputFile, String title, String varName, int bins) throws IOException;

	/**
	 * Computes a scatter plot and shows a window with the plot to the user
	 * 
	 * @param title the title of the plot
	 * @param xVar  the name of the column that is used as x values
	 * @param yVar  the name of the column that is used as y values
	 */
	public default void showScatter(String title, String xVar, String yVar)
	{
		new SwingWrapper<>(scatter(title, xVar, yVar)).displayChart();
	}

	/**
	 * Computer a histogram and show a window with the plot to the user
	 * 
	 * @param title   the title of the chart
	 * @param varName the variable or column that should be used to compute the
	 *                histogram
	 * @param bins    the number of bins to use in the histogram
	 */
	public default void showHistogram(String title, String varName, int bins)
	{
		new SwingWrapper<>(histogram(title, varName, bins)).displayChart();
	}

}