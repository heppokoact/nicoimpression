package jp.co.ise_group.bigdata.nicoimpression.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 感情の定義。 感情の識別子や、どの正規表現をその感情にマッピングするかを定義
 * 
 * @author M.Yoshida
 */
public class ImpressionDef {

	/** 感情の識別子 */
	private final String impressionId;
	/** 感情にマッピングするコメントの正規表現 */
	private final List<Pattern> patterns;

	@JsonCreator
	public ImpressionDef(//
			@JsonProperty("impressionId") String impressionId,//
			@JsonProperty("patterns") List<String> patterns) {
		this.impressionId = impressionId;
		List<Pattern> tempPatterns = new ArrayList<>();
		for (String pattern : patterns) {
			tempPatterns.add(Pattern.compile(pattern));
		}
		this.patterns = Collections.unmodifiableList(tempPatterns);
	}

	public String getImpressionId() {
		return impressionId;
	}

	public List<Pattern> getPatterns() {
		return patterns;
	}

}
