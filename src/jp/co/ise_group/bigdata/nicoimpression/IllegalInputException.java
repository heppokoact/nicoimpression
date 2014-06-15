package jp.co.ise_group.bigdata.nicoimpression;

/**
 * 入力データの不備
 * 
 * @author M.Yoshida
 */
public class IllegalInputException extends RuntimeException {

	/** serialVersionUID */
	private static final long serialVersionUID = 9053037773004191203L;

	/**
	 * コンストラクタ
	 * 
	 * @param message
	 *            エラーの説明
	 */
	public IllegalInputException(String message) {
		super(message);
	}

}
