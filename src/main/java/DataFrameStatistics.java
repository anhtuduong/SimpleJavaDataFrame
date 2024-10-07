import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Interface that allows the user of a data frame to compute a number of
 * statistics based on the data stored in the data frame
 * 
 * @author Paul Bouman
 *
 */
public interface DataFrameStatistics
{

	/**
	 * Computes an unpaired Student's t-Test that compares a column against a given
	 * mean
	 * 
	 * @param var the variable for which the test statistic will be computed
	 * @param mu  the mean to compare against
	 * @return the p-value for the test
	 */
	public double tTest(String var, double mu);

	/**
	 * Computes an unpaired Student's t-Test that compares the mean of two columns.
	 * 
	 * @param var1 the name of the first column to compare
	 * @param var2 the name of the second column to compare
	 * @return the p-value for the test
	 */
	public double tTest(String var1, String var2);

	/**
	 * Computes the Pearson rrelation between two columns in the data frame
	 * 
	 * @param var1 the name of the first column
	 * @param var2 the name of the second column
	 * @return the Pearson correlation between the two columns
	 */
	public double pearsonsCorrelation(String var1, String var2);

	/**
	 * Provides a DescriptiveStatistics objects based on the data for a column in
	 * the data frame
	 * 
	 * @param var the name of the column for which the descriptive statistics must
	 *            be computed
	 * @return the descriptive statistics
	 */
	public DescriptiveStatistics describe(String var);

	/**
	 * Compute a linear model based on Ordinary Least Squares. This model includes
	 * an intercept. In the resulting map, the intercept is added with key
	 * "intercept".
	 * 
	 * @param dep   the dependent variable
	 * @param indep a list of independent variables
	 * @return a map with variable coefficients for the independent variables
	 */
	public Map<String, Double> estimateLinearModel(String dep, List<String> indep);

	/**
	 * Compute a linear model based on Ordinary Least Squares. This model includes
	 * an intercept. In the resulting map, the intercept is added with key
	 * "intercept".
	 * 
	 * @param dep   the dependent variable
	 * @param indep a list of independent variables
	 * @return a map with variable coefficients for the independent variables
	 */
	public default Map<String, Double> estimateLinearModel(String dep, String... indep)
	{
		return estimateLinearModel(dep, Arrays.asList(indep));
	}
}