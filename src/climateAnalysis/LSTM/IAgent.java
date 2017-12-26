package LSTM;

public interface IAgent
{
	void Reset();
	double[] Next(double[] input) throws Exception;
}
