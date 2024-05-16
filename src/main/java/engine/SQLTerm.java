package engine;

import java.util.Vector;

/** * @author Wael Abouelsaadat */

public class SQLTerm {

	public String _strTableName, _strColumnName, _strOperator;
	public Object _objValue;

	public Vector<Tuple> result;
	public String resultText;

	public SQLTerm() {

	}

	public SQLTerm(Vector<Tuple> result, SQLTerm term1, SQLTerm term2, String operator) {
		this.result = result;
		resultText = (term1.resultText == null ? (term1._strColumnName + term1._strOperator + term1._objValue)
				: term1.resultText) + " " + operator + " " + term2._strColumnName + term2._strOperator
				+ term2._objValue;
	}

}