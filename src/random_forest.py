import requests
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
import sys

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def get_files(files):
    file_names = []
    for f in files.iter_lines():
        file_names.append()
    return file_names

def feature_extraction(train_path, test_path):
    #feature extraction to extract the count of bytes from the files
    
    train_file = requests.get(train_path, stream=True)
    test_file = requests.get(test_path, stream=True)
    
    train_files = get_files(train_file)
    test_files = get_files(test_file)
    
    train_files_len = len(train_files)
    test_files_len = len(test_files)
    
    
def add_labels(y_path):
    y_train_file = requests.get(y_path, stream=True)
    y_train = []
    for line in y_train_file.iter_lines():
        y_train.append(int(line) - 1)
    return y_train

def main():

    dataset = sys.argv[1]
    num_of_trees = 40
    depth = 23
        
    if dataset == 'large':
        X_train_path = 'https://storage.googleapis.com/uga-dsp/project1/files/X_train.txt'
        X_test_path = 'https://storage.googleapis.com/uga-dsp/project1/files/X_test.txt'
        y_path = 'https://storage.googleapis.com/uga-dsp/project1/files/y_train.txt'
    else:
        X_train_path = 'https://storage.googleapis.com/uga-dsp/project1/files/X_small_train.txt'
        X_test_path = 'https://storage.googleapis.com/uga-dsp/project1/files/X_small_test.txt'
        y_path = 'https://storage.googleapis.com/uga-dsp/project1/files/y_small_train.txt'
        
    #Getting list of y labels
    y_train = add_labels(y_path)
    
    #feature-extraction
    X_train, X_test = feature_extraction(X_train_path, X_test_path)
    #print(y_train)
if __name__ == "__main__":
    main()
