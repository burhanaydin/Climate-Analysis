package climateAnalysis;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class NonLinearRegression {
	
	private double absoluteTolerance = 4.93e-32;
	private double falseConvergenceTolerance = 2.22e-14;
	private double gradientTolerance = 6.055e-6;
	private double relativeTolerance = 1.0e-20;
	private double stepTolerance = 3.667e-11;
	private double initialTrustRegionRadius;
	private double maxStepSize;
	private int numOfGoodResiduals;
	private boolean solved;
	private int numOfParam;
	/** the learning rate */
	private double rate;
	private double[][] derivativeAccordingToX;
	private double[][] derivativeAccordingToY;

	/** the weight to learn */
	private double[] coefficients;
	
	private double[] thetaGuess;
	
	private double[] scale;

	/** the number of iterations */
	private int ITERATIONS = 100;

	public NonLinearRegression(int n) {
		this.solved = false;
		this.numOfParam = n;
		this.rate = 0.0001;
		this.coefficients = new double[n+1];
	}

	private static double sigmoid(double z) {
		return 1.0 / (1.0 + Math.exp(-z));
	}
	@SuppressWarnings("null")
	public double getCoefficient(int i){
		if(this.coefficients != null && this.solved==true)
			return this.coefficients[i];
		else
			return (Double) null;
	}
	public double[] getCoefficients(){
		if(this.coefficients != null && this.solved==true)
			return this.coefficients;
		else
			return null;
	}
	public double getDFError(){
		return 0;
	}
	public double getErrorStatus(){
		boolean a=true;
		if(a){
			return 1;
		}else if(a){
			return 1;
		}else if(a){
			return 2;
		}else if(a){
			return 3;
		}else if(a){
			return 4;
		}
		return 4;
	}
	public double[][] getR(){
		return new double[5][5];
	}
	public int getRank(){
		return 0;
	}
	public double getSSE(){
		return 0;
	}
	public void setAbsoluteTolerance(double tolerance){
		this.absoluteTolerance = tolerance;
	}
	public void setDigits(int nGood){
		this.numOfGoodResiduals = nGood;
	}
	public void setFalseConvergenceTolerance(double falseConvergenceTolerance){
		this.falseConvergenceTolerance = falseConvergenceTolerance;
	}
	public void setGradientTolerance(double gradientTolerance){
		this.gradientTolerance = gradientTolerance;
	}
	public void setGuess(double[] thetaGuess){
		this.thetaGuess = thetaGuess;
	}
	public void setInitialTrustRegion(double initialTrustRegion){
		this.initialTrustRegionRadius = initialTrustRegion;
	}
	public void setMaxIterations(int maxIterations){
		this.ITERATIONS = maxIterations;
	}
	public void setMaxStepsize(double maxStepsize){
		this.maxStepSize = maxStepsize;
	}
	public void setRelativeTolerance(double relativeTolerance){
		this.relativeTolerance = relativeTolerance;
	}
	public void setScale(double[] scale){
		for(int i=0; i < thetaGuess.length; i++)
			this.scale[i] = 1 / thetaGuess[i]; 
	}
    public void setStepTolerance(double stepTolerance){
    	this.stepTolerance = stepTolerance;
    }
    public double[][] solve(){
    	return new double[5][5];
    }
    public void gradient(double[][] multiVariableFunction){
    	for(int i=0; i < multiVariableFunction.length ; i++){
    		if( multiVariableFunction[i][1] !=0 ){
    			this.derivativeAccordingToX[i][0] = multiVariableFunction[i][0] * multiVariableFunction[i][1];
    			this.derivativeAccordingToX[i][1] = multiVariableFunction[i][1] - 1;
    			this.derivativeAccordingToX[i][2] = multiVariableFunction[i][2];
    		}else{
    			this.derivativeAccordingToX[i][0] = 0;
    			this.derivativeAccordingToX[i][1] = 0;
    			this.derivativeAccordingToX[i][2] = 0;
    		}
    	}
    	for(int i=0; i < multiVariableFunction.length ; i++){
    		if( multiVariableFunction[i][2] != 0){
    			this.derivativeAccordingToY[i][0] = multiVariableFunction[i][0] * multiVariableFunction[i][2];
    			this.derivativeAccordingToY[i][2] = multiVariableFunction[i][2] - 1;
    			this.derivativeAccordingToY[i][1] = multiVariableFunction[i][1];
    	    }else{
    			this.derivativeAccordingToY[i][0] = 0;
    			this.derivativeAccordingToY[i][1] = 0;
    			this.derivativeAccordingToY[i][2] = 0;
    	    }
    	}
    }
	public void train(List<Instance> instances) {
		for (int n=0; n<ITERATIONS; n++) {
			double lik = 0.0;
			for (int i=0; i<instances.size(); i++) {
				int[] x = instances.get(i).x;
				double predicted = classify(x);
				int label = instances.get(i).label;
				for (int j=0; j<coefficients.length; j++) {
					coefficients[j] = coefficients[j] + rate * (label - predicted) * x[j];
				}
				// not necessary for learning
				lik += label * Math.log(classify(x)) + (1-label) * Math.log(1- classify(x));
			}
			System.out.println("iteration: " + n + " " + Arrays.toString(coefficients) + " mle: " + lik);
		}
	}

	private double classify(int[] x) {
		double logit = .0;
		for (int i=0; i<coefficients.length;i++)  {
			logit += coefficients[i] * x[i];
		}
		return sigmoid(logit);
	}

	public static class Instance {
		public int label;
		public int[] x;

		public Instance(int label, int[] x) {
			this.label = label;
			this.x = x;
		}
	}

	public static List<Instance> readDataSet(String file) throws FileNotFoundException {
		List<Instance> dataset = new ArrayList<Instance>();
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(file));
			while(scanner.hasNextLine()) {
				String line = scanner.nextLine();
				if (line.startsWith("#")) {
					continue;
				}
				String[] columns = line.split("\\s+");

				// skip first column and last column is the label
				int i = 1;
				int[] data = new int[columns.length-2];
				for (i=1; i<columns.length-1; i++) {
					data[i-1] = Integer.parseInt(columns[i]);
				}
				int label = Integer.parseInt(columns[i]);
				Instance instance = new Instance(label, data);
				dataset.add(instance);
			}
		} finally {
			if (scanner != null)
				scanner.close();
		}
		return dataset;
	}


	public static void start(String... args) throws FileNotFoundException {
		List<Instance> instances = readDataSet("dataset.txt");
		NonLinearRegression regression = new NonLinearRegression(5);
		regression.train(instances);
		int[] x = {2, 1, 1, 0, 1};
		System.out.println("prob(1|x) = " + regression.classify(x));

		int[] x2 = {1, 0, 1, 0, 0};
		System.out.println("prob(1|x2) = " + regression.classify(x2));

	}

}