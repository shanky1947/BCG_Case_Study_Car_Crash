from yaml import safe_load
from src.data_loader import DataLoader
from src.analysis import AccidentAnalysis
from src.utils import save_results


def main(config_path):
    # Load configuration
    with open(config_path, 'r') as file:
        config = safe_load(file)

    # Load data
    data_loader = DataLoader(config)
    data = data_loader.load_data()
    
    # Perform analyses
    analysis = AccidentAnalysis(data)
    
    # Analysis 1
    result_1 = analysis.analysis_1()
    print(f"Analysis 1 result: {result_1}")
    save_results(result_1, config['output']['results'], 'analysis_1')
    
    # # Analysis 2
    result_2 = analysis.analysis_2()
    print(f"Analysis 2 result: {result_2}")
    save_results(result_2, config['output']['results'], 'analysis_2')
    
    # Analysis 3
    result_3 = analysis.analysis_3()
    print(f"Analysis 3 result: {result_3}")
    save_results(result_3, config['output']['results'], 'analysis_3')
    
    # Analysis 4
    result_4 = analysis.analysis_4()
    print(f"Analysis 4 result: {result_4}")
    save_results(result_4, config['output']['results'], 'analysis_4')

    # # Analysis 5
    result_5 = analysis.analysis_5()
    print(f"Analysis 5 result: {result_5}")
    save_results(result_5, config['output']['results'], 'analysis_5')
    
    # Analysis 6
    result_6 = analysis.analysis_6()
    print(f"Analysis 6 result: {result_6}")
    save_results(result_6, config['output']['results'], 'analysis_6')
    
    # Analysis 7
    result_7 = analysis.analysis_7()
    print(f"Analysis 7 result: {result_7}")
    save_results(result_7, config['output']['results'], 'analysis_7')
    
    # Analysis 8
    result_8 = analysis.analysis_8()
    print(f"Analysis 8 result: {result_8}")
    save_results(result_8, config['output']['results'], 'analysis_8')
    
    # Analysis 9
    result_9 = analysis.analysis_9()
    print(f"Analysis 9 result: {result_9}")
    save_results(result_9, config['output']['results'], 'analysis_9')

    # Analysis 10
    result_10 = analysis.analysis_10()
    print(f"Analysis 10 result: {result_10}")
    save_results(result_10, config['output']['results'], 'analysis_10')

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: spark-submit main.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    main(config_path)