def save_results(results, output_path, analysis_name):
    if isinstance(results, list):
        results_df = spark.createDataFrame(results)
        results_df.write.csv(f"{output_path}/{analysis_name}.csv", header=True)
    else:
        with open(f"{output_path}/{analysis_name}.txt", 'w') as f:
            f.write(str(results))
