import sys

def flatten(input_list):
    output_list = []
    while True:
        if input_list == []:
            break
        for index, i in enumerate(input_list):
            if type(i)== list:
                input_list = i + input_list[index+1:]
                break
            else:
                output_list.append(i)
                input_list.pop(index)
                break
    return output_list

def add_def(func):
    def wrapper(*args, **kwargs):
        print('def tree{}(r):'.format(kwargs['index']))
        return func(*args, **kwargs)
    return wrapper
        
@add_def        
def tree_to_code(tree, columns, index=0):
    tree_ = tree.tree_
    featureNames = [columns[i] for i in tree_.feature]
    def recurse(node, depth):
        indent = "    " * depth
        if tree_.feature[node] != -2:
            name = featureNames[node]
            threshold = tree_.threshold[node]
            print("{}if r['{}'] <= {}:".format(indent, name, round(threshold, 4)))
            recurse(tree_.children_left[node], depth + 1)
            print("{}else:".format(indent))
            recurse(tree_.children_right[node], depth + 1)
        else:
            if 'DecisionTreeClassifier' in str(tree):
                print("{}return {}".format(indent, 
                    tree_.value[node][0][1] / np.sum(tree_.value[node][0])))
            elif 'DecisionTreeRegressor' in str(tree):
                print("{}return {}".format(indent, round(float(tree_.value[node]), 4)))
            else:
                return 'estimator is wrong!'               
    recurse(0, 1)
  
def text_to_file(func):
    def wrapper(*args, **kwargs):
        orig_stdout = sys.stdout
        f = open(args[2], 'w')
        sys.stdout = f
        func(*args, **kwargs)
        sys.stdout  = orig_stdout
        f.close()
    return wrapper

@text_to_file
def tree_to_file(tree, columns, func_file_name, index=0):
    tree_to_code(tree, columns, index=index)
    
@text_to_file    
def ensemble_to_file(ensemble, columns, func_file_name):
    try:
        estimators = flatten(ensemble.estimators_.tolist())
    except:
        estimators = ensemble.estimators_
    for index, tree in enumerate(estimators):
        tree_to_code(tree, columns, index=index)