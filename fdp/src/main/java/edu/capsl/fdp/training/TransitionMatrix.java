

package edu.capsl.fdp.training;

import java.util.Map;
import java.util.HashMap;

public class TransitionMatrix {
	
	private static final String DELIMETER = ",";
	
	private Map<String, Integer> label_to_pos;
	private double[][] transMatrix;
	private int numStates;
	
	public TransitionMatrix(String[] state_labels) {
		numStates = state_labels.length;
		
		transMatrix = new double[numStates][numStates];
		label_to_pos = new HashMap<String, Integer>(numStates);
		
		int pos = 0;
		for (String state : state_labels) {
			label_to_pos.put(state, pos++);
		}
	}

	public void addTo(String present, String future, int val) {
		
		int row = label_to_pos.get(present);
		int col = label_to_pos.get(future);
		
		transMatrix[row][col] += val;
	}
	
	public void normalizeRows() {
		// laplace correction (smoothing function)
		for (int r = 0; r < numStates; ++r) {
			
			boolean gotZeroCount = false;
			for (int c = 0; c < numStates && !gotZeroCount; ++c) {
				gotZeroCount = transMatrix[r][c] == 0;
			}
			
			if (gotZeroCount) {
				for (int c = 0; c < numStates; ++c) {
					transMatrix[r][c] += 1;
				}			
			}
		}		
		
		//normalize
		double rowSum = 0;
		for (int r = 0; r < numStates; ++r) {
			rowSum = getRowSum(r);
			for (int c = 0; c < numStates; ++c) {
				transMatrix[r][c] = transMatrix[r][c] / rowSum;
			}
		}
	}
	
	public int getRowSum(int row) {
		int sum = 0;
		for (int c = 0; c < numStates; ++c) {
			sum += transMatrix[row][c];
		}
		return sum;
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.util.TabularData#serializeRow(int)
	 */
	public String serializeRow(int row) {
		StringBuilder stBld = new StringBuilder();
		for (int c = 0; c < numStates; ++c) {
			stBld.append(transMatrix[row][c]).append(DELIMETER);
		}
		
		return stBld.substring(0, stBld.length() - DELIMETER.length());
	}
	
	/* (non-Javadoc)
	 * @see org.chombo.util.TabularData#deseralizeRow(java.lang.String, int)
	 */
	public void deseralizeRow(String data, int row) {
		String[] items = data.split(DELIMETER);
		int k = 0;
		for (int c = 0; c < numStates; ++c) {
			transMatrix[row][c]  = Double.parseDouble(items[k++]);
		}
	}
	
	
}
