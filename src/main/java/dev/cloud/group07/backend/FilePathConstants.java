/**
 * 
 * このモジュールは完成済みです
 * 
 * 
 */



package dev.cloud.group07.backend;

public class FilePathConstants {

	// HDFS上の資材のパス
	public static final String FILE_BASE = "/user/root/rakuten_recipe";
	// ローカルの資材のパス
	public static final String LOCAL_FILE_BASE = "/root/rakuten_recipe";

	// 各ファイルの名前
	public static final String TSUKUREPO_FILE_NAME = "recipe_tsukurepo_20120705.txt";
	public static final String TSUKUREPO_COUNT_FILE_NAME = "tsukurepo_count";
	public static final String PROCESS_FILE_NAME = "recipe_process_20120705.txt";
	public static final String PROCESS_COUNT_FILE_NAME = "process_count";
    public static final String ALL_FILE_NAME = "recipe_all_20120705.txt";
    public static final String EVALUATION_FILE_NAME = "evaluation"; // 最終的な結果ファイル
}
