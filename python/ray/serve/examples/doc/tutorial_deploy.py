# yapf: disable
# __doc_import_train_begin__
import pickle
import json
import numpy as np

from sklearn.datasets import load_iris
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import mean_squared_error

# Load data
iris_dataset = load_iris()
data, target, target_names = iris_dataset["data"], iris_dataset[
    "target"], iris_dataset["target_names"]

# Instantiate model
model = GradientBoostingClassifier()

# Training and validation split
np.random.shuffle(data), np.random.shuffle(target)
train_x, train_y = data[:100], target[:100]
val_x, val_y = data[100:], target[100:]

# Train and evaluate models
model.fit(train_x, train_y)
print("MSE:", mean_squared_error(model.predict(val_x), val_y))

# Save the model and label to file
with open("/tmp/iris_model_logistic_regression.pkl", "wb") as f:
    pickle.dump(model, f)
with open("/tmp/iris_labels.json", "w") as f:
    json.dump(target_names.tolist(), f)
# __doc_import_train_end__


# __doc_create_deploy_begin__
import pickle
import json

from ray import serve
import ray

class BoostingModel:
    def __init__(self):
        with open("/tmp/iris_model_logistic_regression.pkl", "rb") as f:
            self.model = pickle.load(f)
        with open("/tmp/iris_labels.json") as f:
            self.label_list = json.load(f)

    def __call__(self, flask_request):
        payload = flask_request.json
        print("Worker: received flask request with data", payload)

        input_vector = [
            payload["sepal length"],
            payload["sepal width"],
            payload["petal length"],
            payload["petal width"],
        ]
        prediction = self.model.predict([input_vector])[0]
        human_name = self.label_list[prediction]
        return {"result": human_name}

# connect to our existing Ray cluster
# note that the password will be different for your redis instance!
ray.init(address='auto', redis_password='5241590000000000')
# now we initialize /connect to the Ray service

serve.init()
serve.create_endpoint("iris_classifier", "/regressor")
serve.create_backend("lr:v1", BoostingModel)
serve.set_traffic("iris_classifier", {"lr:v1": 1, "version":"v1"})
# __doc_create_deploy_end__

# __doc_query_begin__
import requests

sample_request_input = {
    "sepal length": 1.2,
    "sepal width": 1.0,
    "petal length": 1.1,
    "petal width": 0.9,
}
response = requests.get(
    "http://localhost:8000/regressor", json=sample_request_input)
print(response.text)
# Result:
# {
#  "result": "setosa",
#  "version": "v1"
# }
# this result may vary, since the training parameters may change.
# as we update this model, this result will also change over time.
# __doc_query_end__



# __doc_create_deploy_2_begin__
import pickle
import json
import numpy as np

from sklearn.datasets import load_iris
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import mean_squared_error

# Load data
iris_dataset = load_iris()
data, target, target_names = iris_dataset["data"], iris_dataset[
    "target"], iris_dataset["target_names"]

# Instantiate model
model = GradientBoostingClassifier()

# Training and validation split
np.random.shuffle(data), np.random.shuffle(target)
train_x, train_y = data[:100], target[:100]
val_x, val_y = data[100:], target[100:]

# Train and evaluate models
model.fit(train_x, train_y)
print("MSE:", mean_squared_error(model.predict(val_x), val_y))

# Save the model and label to file
with open("/tmp/iris_model_logistic_regression_2.pkl", "wb") as f:
    pickle.dump(model, f)
with open("/tmp/iris_labels_2.json", "w") as f:
    json.dump(target_names.tolist(), f)


import pickle
import json

from ray import serve
import ray

class BoostingModelv2:
    def __init__(self):
        with open("/tmp/iris_model_logistic_regression_2.pkl", "rb") as f:
            self.model = pickle.load(f)
        with open("/tmp/iris_labels_2.json") as f:
            self.label_list = json.load(f)

    def __call__(self, flask_request):
        payload = flask_request.json
        print("Worker: received flask request with data", payload)

        input_vector = [
            payload["sepal length"],
            payload["sepal width"],
            payload["petal length"],
            payload["petal width"],
        ]
        prediction = self.model.predict([input_vector])[0]
        human_name = self.label_list[prediction]
        return {"result": human_name, "version":"v2"}

# connect to our existing Ray cluster
# note that the password will be different for your redis instance!
# ray.init(address='auto', redis_password='5241590000000000')
# now we initialize /connect to the Ray service

serve.init()
serve.create_backend("lr:v2", BoostingModelv2)
serve.set_traffic("iris_classifier", {"lr:v2": 0.25, "lr:v1": 0.75})
# __doc_create_deploy_2_end__