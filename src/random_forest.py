import requests
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
import sys
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.ml.feature import CountVectorizer
import re
from pyspark.sql.types import ArrayType, StringType
import pickle
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.tree import RandomForest
import pandas as pd
import pandas

# creating the spark context
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def byte_feature_extraction(lists, file, feature, track, value):

        for hexa_file_num, f in enumerate(file):
            inputs = requests.get("https://storage.googleapis.com/uga-dsp/project1/data/bytes/" + f +".bytes", stream=True)
            for hexa_file in inputs.iter_lines():
                if len(hexa_file) == 0:
                    continue
                hexa_file = hexa_file.split()
            
                for byte in hexa_file[1:]:
                        if byte not in track:
                            if value == 1:
                                continue
                            else:
                                track[byte] = feature
                                lists[hexa_file_num][feature] += 1
                                feature += 1
                        else:
                                lists[hexa_file_num][track[byte]] += 1
        return lists, feature, track

def feature_extraction(train_path, test_path, max_val):
    #feature extraction to extract the count of bytes from the files
    
    current_feature = 0
    track_files = {}
    training = 0
    testing = 1
    train_file = requests.get(train_path, stream=True)
    test_file = requests.get(test_path, stream=True)  
    train_files = get_files(train_file)
    test_files = get_files(test_file)   
    length_of_train = len(train_files)
    length_of_test = len(test_files)
    train = [[0]*max_val for x in range(length_of_train)]
    test = [[0]*max_val for x in range(length_of_test)]
    train, current_feature, track_files = byte_feature_extraction(train[:], train_files, current_feature, track_files, training)
    test, current_feature, track_files = byte_feature_extraction(test[:], test_files, current_feature, track_files, testing)
    train = [t[:current_feature] for t in train[:]]
    test = [t[:current_feature] for t in test[:]]
    with open('train_set', 'wb') as fp:
        pickle.dump(train, fp)      
    with open('test_set', 'wb') as fp:
        pickle.dump(test, fp)   
    return train, test
    

def get_files(files):
    file_names = []
    for f in files.iter_lines():
        file_names.append(f)
    return file_names    
    
def add_labels(y_path):
    y_train_file = requests.get(y_path, stream=True)
    y_train = []

    for line in y_train_file.iter_lines():
        y_train.append(int(line) - 1)

    return y_train

def generate_training_matrix(X_train, y_train):
    
    training_data = []
    length_of_train = len(X_train)    
    for index in range(length_of_train):
        training_data.append(LabeledPoint(y_train[index], X_train[index]))
    return training_data

def malware_predict_and_store(sc, training_set, X_test, num_of_trees, depth):
    
    classifier_model = RandomForest.trainClassifier(sc.parallelize(training_set), 9, {}, num_of_trees,  maxDepth = depth)
    result = []
    for index in X_test:
         result.append(int(classifier_model.predict(index) + 1))
    name = 'result.txt'
    df = pd.DataFrame(result)
    df.to_csv(name, header = False, index = False)

def main():

    dataset = sys.argv[1]
    num_of_trees = 40
    depth = 23

    max_val = 1000  

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

    X_train, X_test = feature_extraction(X_train_path, X_test_path, max_val)

    #generating training data
    training_set = generate_training_matrix(X_train, y_train)
    
    #predicting the malware using RandomForest Classfier and saving the output in the GS bucket
    malware_predict_and_store(sc, training_set, X_test, num_of_trees, depth)

    #print(y_train)
if __name__ == "__main__":
    main()

